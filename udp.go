package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/netip"
	"sync"
	"time"
)

// serveUDP 启动 UDP 隧道监听并转发数据
func serveUDP(
	ctx context.Context,
	b *Bridge,
	wg *sync.WaitGroup,
	local, remote string,
	stat *ServiceStats,
) {
	pc, err := net.ListenPacket("udp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) UDP端口监听失败 %s: %v", stat.Name, local, err)
		return
	}
	defer pc.Close()

	conn := pc.(*net.UDPConn)
	_ = conn.SetReadBuffer(b.socketBufSize)
	_ = conn.SetWriteBuffer(b.socketBufSize)

	sessions := make(map[netip.AddrPort]*net.UDPConn)
	var mu sync.RWMutex

	// 统一注册清理逻辑：当全局 Context 取消时，关闭监听器和所有会话
	stop := context.AfterFunc(ctx, func() {
		_ = pc.Close()
		mu.Lock()
		for _, c := range sessions {
			_ = c.Close()
		}
		mu.Unlock()
	})
	defer stop()

	log.Printf("[INFO] (%s) [UDP] 隧道已启动: %s -> %s", stat.Name, local, remote)

	for {
		buf := b.getBuf()
		// 读取客户端发来的数据包，没有数据会阻塞
		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			b.putBuf(buf)
			if isClosed(ctx, err) {
				return
			}
			log.Printf("[WARN] (%s) 本地UDP数据读取失败: %v", stat.Name, err)
			continue
		}

		rConn := getOrCreateSession(ctx, b, wg, conn, sessions, &mu, cliAddr, remote, stat)
		if rConn == nil {
			b.putBuf(buf)
			continue
		}

		// 刷新超时时间(客户端发包，说明连接还存在，避免服务端超时断开)
		_ = rConn.SetReadDeadline(time.Now().Add(b.sessionTimeout))

		// 直接转发到服务端，设置一个较短的超时时间，避免阻塞
		_ = rConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		nw, err := rConn.Write(buf[:n])
		_ = rConn.SetWriteDeadline(time.Time{})

		if nw > 0 {
			stat.Sent.Add(uint64(nw))
		}
		b.putBuf(buf)

		if err != nil && !isClosed(ctx, err) {
			log.Printf("[WARN] (%s) 转发UDP数据到服务端失败: %v", stat.Name, err)
		}
	}
}

// getOrCreateSession 寻找已有连接或创建新连接
func getOrCreateSession(
	ctx context.Context,
	b *Bridge,
	wg *sync.WaitGroup,
	lConn *net.UDPConn,
	sessions map[netip.AddrPort]*net.UDPConn,
	mu *sync.RWMutex,
	cliAddr *net.UDPAddr,
	remote string,
	stat *ServiceStats,
) *net.UDPConn {
	key := cliAddr.AddrPort()

	// 检查UDP连接是否存在
	mu.RLock()
	c, ok := sessions[key]
	mu.RUnlock()
	if ok {
		return c
	}

	// 连接服务端
	rConnRaw, err := net.Dial("udp", remote)
	if err != nil {
		log.Printf("[WARN] (%s) 无法连接远程UDP服务: %v", stat.Name, err)
		return nil
	}
	// 在锁外创建连接，避免锁竞争
	rConn := rConnRaw.(*net.UDPConn)
	_ = rConn.SetReadBuffer(b.socketBufSize)
	_ = rConn.SetWriteBuffer(b.socketBufSize)

	// 二次检查，防止并发创建多个连接
	mu.Lock()
	if sExist, ok := sessions[key]; ok {
		// 已经存在连接，关闭新创建的连接
		mu.Unlock()
		_ = rConn.Close()
		return sExist
	}
	sessions[key] = rConn
	mu.Unlock()

	// 启动回传协程
	wg.Go(func() {
		readLoop(ctx, b, lConn, rConn, sessions, mu, key, stat)
	})

	return rConn
}

// readLoop 接收服务端的回包并回传给客户端
func readLoop(
	ctx context.Context,
	b *Bridge,
	lConn *net.UDPConn,
	rConn *net.UDPConn,
	sessions map[netip.AddrPort]*net.UDPConn,
	mu *sync.RWMutex,
	key netip.AddrPort,
	stat *ServiceStats,
) {
	defer func() {
		_ = rConn.Close()
		mu.Lock()
		if sessions[key] == rConn {
			delete(sessions, key)
		}
		mu.Unlock()
	}()

	buf := b.getBuf()
	defer b.putBuf(buf)

	for {
		// 设置读取超时时间，超过该时间没有收到数据包，则关闭连接
		_ = rConn.SetReadDeadline(time.Now().Add(b.sessionTimeout))
		rn, err := rConn.Read(buf)
		if err != nil {
			if isClosed(ctx, err) {
				return
			}
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				return
			}
			log.Printf("[WARN] (%s) UDP侧(%d)异常中断: %v", stat.Name, key.Port(), err)
			return
		}

		n, err := lConn.WriteToUDP(buf[:rn], net.UDPAddrFromAddrPort(key))
		if n > 0 {
			stat.Recv.Add(uint64(n))
		}
		if err != nil && !isClosed(ctx, err) {
			log.Printf("[WARN] (%s) 转发UDP数据包到客户端失败: %v", stat.Name, err)
		}
	}
}

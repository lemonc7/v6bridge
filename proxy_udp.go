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

func StartUDPProxy(ctx context.Context, logger *log.Logger, network NetworkConfig, name, local, remote string) {
	localAddr, err := net.ResolveUDPAddr("udp", local)
	if err != nil {
		logger.Printf("[ERROR] (%s) UDP 本地地址无效 (%s): %v", name, local, err)
		return
	}
	localConn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		logger.Printf("[ERROR] (%s) UDP 绑定本地端口失败 (%s): %v", name, local, err)
		return
	}
	defer localConn.Close()
	setUDPOptions(localConn, network.BufferSize)
	logger.Printf("[INFO] (%s) UDP 隧道已启动: %s -> %s", name, local, remote)

	remoteAddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		logger.Printf("[ERROR] (%s) UDP 解析服务器地址失败 %s: %v", name, remote, err)
		return
	}

	sessions := newUDPSessions()
	defer sessions.closeAll()

	// context关闭时，关闭监听器，清理所有会话
	stop := context.AfterFunc(ctx, func() {
		_ = localConn.Close()
		sessions.closeAll()
		logger.Printf("[INFO] (%s) UDP 隧道已关闭: %s", name, local)
	})
	defer stop()

	log.Printf("[INFO] (%s) UDP 隧道已启动: %s -> %s", name, local, remote)

	for {
		buf := make([]byte, network.BufferSize)
		// 读取客户端发来的数据包，没有数据会阻塞
		n, clientAddr, err := localConn.ReadFromUDP(buf)
		if err != nil {
			if isClosed(ctx, err) {
				return
			}
			logger.Printf("[ERROR] (%s) 接收 UDP 连接错误: %v", name, err)
			continue
		}

		remoteConn := sessions.getOrCreate(ctx, logger, network, name, localConn, clientAddr, remoteAddr)
		if remoteConn == nil {
			continue
		}

		// 刷新超时时间(客户端发包，说明连接还存在，避免服务端超时断开)
		_ = remoteConn.SetReadDeadline(time.Now().Add(network.SessionTimeout))
		// 直接转发到服务端，设置一个较短的超时时间，避免阻塞
		_ = remoteConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		_, err = remoteConn.Write(buf[:n])
		// 取消超时设置
		_ = remoteConn.SetWriteDeadline(time.Time{})

		if err != nil && !isClosed(ctx, err) {
			logger.Printf("[WARN] (%s) UDP 发送远程数据异常: %v", name, err)
		}
	}
}

type udpSessions struct {
	mu       sync.RWMutex
	sessions map[netip.AddrPort]*net.UDPConn
}

func newUDPSessions() *udpSessions {
	return &udpSessions{sessions: make(map[netip.AddrPort]*net.UDPConn)}
}

func (s *udpSessions) getOrCreate(
	ctx context.Context,
	logger *log.Logger,
	network NetworkConfig,
	name string,
	localConn *net.UDPConn,
	clientAddr *net.UDPAddr,
	remoteAddr *net.UDPAddr,
) *net.UDPConn {
	key := clientAddr.AddrPort()

	// 检查UDP连接是否存在
	s.mu.RLock()
	existing := s.sessions[key]
	s.mu.RUnlock()
	if existing != nil {
		return existing
	}

	// 连接服务端
	remoteConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		logger.Printf("[ERROR] (%s) UDP 连接远程失败: %v", name, err)
		return nil
	}
	setUDPOptions(remoteConn, network.BufferSize)

	// 二次检查，防止并发创建多个连接
	s.mu.Lock()
	if existing := s.sessions[key]; existing != nil {
		// 已经存在连接，关闭新创建的连接
		s.mu.Unlock()
		_ = remoteConn.Close()
		return existing
	}
	s.sessions[key] = remoteConn
	s.mu.Unlock()

	logger.Printf("[INFO] (%s) 新 UDP 客户端连接: %s", name, clientAddr)
	// 启动回传协程
	go readUDPResponses(ctx, logger, network, name, localConn, remoteConn, key, s)

	return remoteConn
}

func (s *udpSessions) remove(key netip.AddrPort, conn *net.UDPConn) {
	s.mu.Lock()
	if s.sessions[key] == conn {
		delete(s.sessions, key)
	}
	s.mu.Unlock()
}

func (s *udpSessions) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, conn := range s.sessions {
		_ = conn.Close()
		delete(s.sessions, key)
	}
}

// 接收服务端的回包并回传给客户端
func readUDPResponses(
	ctx context.Context,
	logger *log.Logger,
	network NetworkConfig,
	name string,
	localConn *net.UDPConn,
	remoteConn *net.UDPConn,
	clientKey netip.AddrPort,
	sessions *udpSessions,
) {
	defer func() {
		_ = remoteConn.Close()
		sessions.remove(clientKey, remoteConn)
		logger.Printf("[INFO] (%s) UDP 会话已关闭: %s", name, clientKey)
	}()

	buf := make([]byte, network.BufferSize)
	clientAddr := net.UDPAddrFromAddrPort(clientKey)
	for {
		// 设置读取超时时间，超过该时间没有收到数据包，则关闭连接
		_ = remoteConn.SetReadDeadline(time.Now().Add(network.SessionTimeout))
		n, err := remoteConn.Read(buf)
		if err != nil {
			if isClosed(ctx, err) {
				return
			}
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				logger.Printf("[INFO] (%s) UDP 连接空闲/超时，已清理: %s", name, clientKey)
				return
			}
			logger.Printf("[WARN] (%s) UDP 接收远程数据异常: %v", name, err)
			return
		}

		if _, err := localConn.WriteToUDP(buf[:n], clientAddr); err != nil && !isClosed(ctx, err) {
			logger.Printf("[WARN] (%s) UDP 回写本地客户端异常: %v", name, err)
		}
	}
}

func setUDPOptions(conn *net.UDPConn, bufferSize int) {
	_ = conn.SetReadBuffer(bufferSize)
	_ = conn.SetWriteBuffer(bufferSize)
}

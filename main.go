package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultUDPTimeout = 120 * time.Second // 会话超时时间
	CleanupInterval   = 30 * time.Second  // 清理间隔
	BufferSize        = 65535             // 64KB
)

// TunnelItem 单个隧道配置
type TunnelItem struct {
	Name   string `yaml:"name"`
	Remote int    `yaml:"remote"`
	Local  int    `yaml:"local"`
	Proto  string `yaml:"proto"`
}

// Config 配置文件映射
type Config map[string][]TunnelItem

// --- 全局资源池 ---
var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, BufferSize)
	},
}

func putBack(b []byte) {
	if b != nil {
		bufferPool.Put(b[:cap(b)])
	}
}

// --- 核心管理器 ---
type Manager struct {
	ctx context.Context
	wg  sync.WaitGroup
}

func NewManager(ctx context.Context) *Manager {
	return &Manager{ctx: ctx}
}

func (m *Manager) Start(cfg Config) {
	for host, tunnels := range cfg {
		for _, t := range tunnels {
			if t.Local <= 0 || t.Local > 65535 || t.Remote <= 0 || t.Remote > 65535 {
				log.Printf("[WARN] (%s) 跳过无效端口配置: %s, Local: %d, Remote: %d", t.Name, host, t.Local, t.Remote)
				continue
			}

			// 统一处理名称和地址
			name := t.Name
			proto := strings.ToLower(t.Proto)
			if proto == "" {
				proto = "udp"
			}
			if name == "" {
				name = fmt.Sprintf("%s-%d", proto, t.Local)
			}

			local := fmt.Sprintf(":%d", t.Local)
			remote := net.JoinHostPort(host, strconv.Itoa(t.Remote))

			// 启动对应的桥接服务
			if proto == "tcp" {
				m.wg.Go(func() { m.runTCP(local, remote, name) })
			} else {
				m.wg.Go(func() { m.runUDP(local, remote, name) })
			}
		}
	}
}

func (m *Manager) Wait() {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[INFO] 所有隧道已完成清理并退出")
	case <-time.After(5 * time.Second):
		log.Println("[WARN] 等待部分连接关闭超时，正在强制退出")
	}
}

func (m *Manager) runTCP(local, remote, name string) {
	lAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) 本地地址解析失败: %v", name, err)
		return
	}

	listener, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 监听失败: %v", name, err)
		return
	}
	defer listener.Close()

	go func() {
		<-m.ctx.Done()
		_ = listener.Close()
	}()

	log.Printf("[INFO] (%s) [TCP] 隧道已启动: %s -> %s", name, local, remote)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if m.isClosed(err) {
				return
			}
			log.Printf("[WARN] (%s) 接受连接错误: %v", name, err)
			continue
		}

		// 追踪每个连接协程
		m.wg.Go(func() { m.handleTCPConn(conn, remote, name) })
	}
}

func (m *Manager) handleTCPConn(clientConn *net.TCPConn, remoteAddr, name string) {
	defer clientConn.Close()
	_ = clientConn.SetKeepAlive(true)
	_ = clientConn.SetKeepAlivePeriod(30 * time.Second)
	_ = clientConn.SetNoDelay(true)

	d := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	c, err := d.DialContext(m.ctx, "tcp", remoteAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 无法连接远程服务器: %v", name, err)
		return
	}
	remoteConn := c.(*net.TCPConn)
	defer remoteConn.Close()
	_ = remoteConn.SetNoDelay(true)

	// 监听关闭信号
	go func() {
		<-m.ctx.Done()
		_ = clientConn.Close()
		_ = remoteConn.Close()
	}()

	// 内部双向读写 WaitGroup
	var internalWg sync.WaitGroup
	transfer := func(dst, src *net.TCPConn, dir string) {
		buf := bufferPool.Get().([]byte)
		defer putBack(buf)
		if _, err := io.CopyBuffer(dst, src, buf); err != nil {
			if !m.isClosed(err) {
				log.Printf("[WARN] (%s) %s 数据转发异常: %v", name, dir, err)
			}
		}
		// 半关闭，告诉对方已经写完了
		_ = dst.CloseWrite()
	}

	internalWg.Go(func() { transfer(remoteConn, clientConn, "客户端->服务端") })
	internalWg.Go(func() { transfer(clientConn, remoteConn, "服务端->客户端") })
	internalWg.Wait()
}

type udpSession struct {
	conn       *net.UDPConn
	lastActive atomic.Int64
}

func (m *Manager) runUDP(local, remote, name string) {
	lAddr, err := net.ResolveUDPAddr("udp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) 本地地址解析失败: %v", name, err)
		return
	}
	rAddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		log.Printf("[ERROR] (%s) 远程地址解析失败: %v", name, err)
		return
	}

	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 监听失败: %v", name, err)
		return
	}
	defer conn.Close()

	go func() {
		<-m.ctx.Done()
		_ = conn.Close()
	}()

	_ = conn.SetReadBuffer(2 * 1024 * 1024)
	_ = conn.SetWriteBuffer(2 * 1024 * 1024)

	var (
		sessions = make(map[netip.AddrPort]*udpSession)
		mu       sync.RWMutex
	)

	// 定期清理协程
	m.wg.Go(func() {
		ticker := time.NewTicker(CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				mu.Lock()
				for _, s := range sessions {
					_ = s.conn.Close()
				}
				mu.Unlock()
				return
			case <-ticker.C:
				now := time.Now().Unix()
				mu.Lock()
				for k, s := range sessions {
					if now-s.lastActive.Load() > int64(DefaultUDPTimeout.Seconds()) {
						_ = s.conn.Close()
						delete(sessions, k)
					}
				}
				mu.Unlock()
			}
		}
	})

	log.Printf("[INFO] (%s) [UDP] 隧道已启动: %s -> %s", name, local, remote)

	for {
		buf := bufferPool.Get().([]byte)
		// 读取本地客户端的数据包，准备转发给服务端
		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			putBack(buf)
			if m.isClosed(err) {
				return
			}
			log.Printf("[WARN] (%s) UDP读取错误: %v", name, err) // 增加日志防止静默失败
			continue
		}

		key := cliAddr.AddrPort()
		// 第一次检查(读锁)，会话是否已存在
		mu.RLock()
		s, ok := sessions[key]
		mu.RUnlock()

		if !ok {
			rConn, err := net.DialUDP("udp", nil, rAddr)
			if err != nil {
				putBack(buf)
				log.Printf("[WARN] (%s) 无法创建远程UDP连接: %v", name, err)
				continue
			}
			_ = rConn.SetReadBuffer(2 * 1024 * 1024)
			_ = rConn.SetWriteBuffer(2 * 1024 * 1024)

			// 二次检查，防止并发创建(写锁)
			mu.Lock()
			if sExist, ok := sessions[key]; ok {
				// 小概率并发冲突，使用已存在的，关闭新建的
				_ = rConn.Close()
				s = sExist
			} else {
				s = &udpSession{conn: rConn}
				s.lastActive.Store(time.Now().Unix())
				sessions[key] = s

				// 启动反向回包协程(将服务端返回的数据转发回本地客户端)
				m.wg.Go(func() {
					defer func() {
						_ = s.conn.Close()
						mu.Lock()
						delete(sessions, key)
						mu.Unlock()
					}()
					// 复用buffer(同步串行是安全的)
					localBuf := bufferPool.Get().([]byte)
					defer putBack(localBuf)

					for {
						_ = s.conn.SetReadDeadline(time.Now().Add(DefaultUDPTimeout))
						// 从远程服务端读取数据
						rn, err := s.conn.Read(localBuf)
						if err != nil {
							if m.isClosed(err) {
								return
							}
							if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
								log.Printf("[INFO] (%s) UDP 会话长时间无数据，自动关闭", name)
								return
							}

							log.Printf("[WARN] (%s) 会话异常中断: %v", name, err)
							return
						}

						s.lastActive.Store(time.Now().Unix())
						// 将数据发给本地客户端
						_, _ = conn.WriteToUDP(localBuf[:rn], net.UDPAddrFromAddrPort(key))
					}
				})
			}
			mu.Unlock()
		}

		s.lastActive.Store(time.Now().Unix())
		// 将数据发给服务端
		_, _ = s.conn.Write(buf[:n])
		putBack(buf)
	}
}

// --- 辅助方法 ---
func (m *Manager) isClosed(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		m.ctx.Err() != nil
}

// --- 主要流程 ---
func main() {
	fmt.Println(">> v6bridge | 端口映射工具, 主要用于游戏ipv6联机 \n>> 详情参考 GitHub: https://github.com/lemonc7/v6bridge")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	data, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("[ERROR] 无法读取配置文件: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("[ERROR] 配置文件解析错误: %v", err)
	}

	mgr := NewManager(ctx)
	mgr.Start(cfg)

	<-ctx.Done()
	log.Println("[INFO] 接收到退出信号，正在释放资源...")
	mgr.Wait()
}

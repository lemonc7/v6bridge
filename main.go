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
	BufferSize        = 32 * 1024         // 32KB: 兼顾低延迟与高吞吐
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
				log.Printf("[WARN] (%s) 跳过无效端口配置: %s, Local: %d, Remote: %d\n", t.Name, host, t.Local, t.Remote)
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

// --- TCP 桥接实现 ---
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

	go func() {
		<-m.ctx.Done()
		_ = listener.Close()
	}()

	log.Printf("[INFO] (%s) [TCP] 隧道已启动: %s -> %s\n", name, local, remote)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if m.isClosed(err) {
				return
			}
			log.Printf("[WARN] (%s) 接受连接错误: %v", name, err)
			continue
		}

		_ = conn.SetKeepAlive(true)
		_ = conn.SetKeepAlivePeriod(30 * time.Second)
		_ = conn.SetNoDelay(true)

		// 追踪每个连接协程
		m.wg.Go(func() { m.handleTCPConn(conn, remote, name) })
	}
}

func (m *Manager) handleTCPConn(clientConn *net.TCPConn, remoteAddr, name string) {
	defer clientConn.Close()

	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(m.ctx, "tcp", remoteAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 无法连接远程服务器: %v", name, err)
		return
	}
	remoteConn := c.(*net.TCPConn)
	_ = remoteConn.SetNoDelay(true)
	defer remoteConn.Close()

	// 内部双向读写 WaitGroup
	var internalWg sync.WaitGroup
	transfer := func(dst, src *net.TCPConn, dir string) {
		buf := bufferPool.Get().([]byte)
		defer putBack(buf)
		if _, err := io.CopyBuffer(dst, src, buf); err != nil {
			if !m.isClosed(err) {
				log.Printf("[WARN] (%s) %s 数据转发异常: %v\n", name, dir, err)
			}
		}
		_ = dst.CloseWrite()
	}

	internalWg.Go(func() { transfer(remoteConn, clientConn, "客户端->服务端") })
	internalWg.Go(func() { transfer(clientConn, remoteConn, "服务端->客户端") })

	// 等待连接结束或 ctx 取消
	done := make(chan struct{})
	go func() {
		internalWg.Wait()
		close(done)
	}()

	select {
	case <-m.ctx.Done():
	case <-done:
	}
}

// --- UDP 桥接实现 ---
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

	log.Printf("[INFO] (%s) [UDP] 隧道已启动: %s -> %s\n", name, local, remote)

	for {
		buf := bufferPool.Get().([]byte)
		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			putBack(buf)
			if m.isClosed(err) {
				return
			}
			continue
		}

		// 使用 netip.AddrPort 提升性能
		key := cliAddr.AddrPort()
		mu.RLock()
		s, ok := sessions[key]
		mu.RUnlock()

		if !ok {
			mu.Lock()
			// 二次检查
			if s, ok = sessions[key]; !ok {
				rConn, err := net.DialUDP("udp", nil, rAddr)
				if err != nil {
					mu.Unlock()
					putBack(buf)
					log.Printf("[WARN] (%s) 无法创建远程UDP连接: %v", name, err)
					continue
				}
				_ = rConn.SetReadBuffer(2 * 1024 * 1024)
				_ = rConn.SetWriteBuffer(2 * 1024 * 1024)

				s = &udpSession{conn: rConn}
				s.lastActive.Store(time.Now().Unix())
				sessions[key] = s
				// 启动反向回包协程
				m.wg.Go(func() {
					defer func() {
						_ = s.conn.Close()
						mu.Lock()
						delete(sessions, key)
						mu.Unlock()
					}()

					for {
						buf := bufferPool.Get().([]byte)
						_ = s.conn.SetReadDeadline(time.Now().Add(DefaultUDPTimeout))
						rn, err := s.conn.Read(buf)
						if err != nil {
							putBack(buf)
							return
						}

						s.lastActive.Store(time.Now().Unix())
						_, _ = conn.WriteToUDP(buf[:rn], cliAddr)
						putBack(buf)
					}
				})
			}
			mu.Unlock()
		}

		s.lastActive.Store(time.Now().Unix())
		_, _ = s.conn.Write(buf[:n])
		putBack(buf)
	}
}

// --- 辅助方法 ---
func (m *Manager) isClosed(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection") || m.ctx.Err() != nil
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
		log.Fatalf("[ERROR] 无法读取配置文件: %v\n", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("[ERROR] 配置文件解析错误: %v\n", err)
	}

	mgr := NewManager(ctx)
	mgr.Start(cfg)

	<-ctx.Done()
	log.Println("[INFO] 接收到退出信号，正在释放资源...")
	mgr.Wait()
}

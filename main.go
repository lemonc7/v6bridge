package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// TunnelItem 代表单个隧道配置项
type TunnelItem struct {
	Name   string `yaml:"name"`
	Remote int    `yaml:"remote"`
	Local  int    `yaml:"local"`
	Proto  string `yaml:"proto"`
}

// Config map[string][]TunnelItem
// Key 是 Hostname
type Config map[string][]TunnelItem

var bufferPool = sync.Pool{
	New: func() any {
		// 每次池子中没有可用对象时，创建一个2KB的buffer(足够容纳绝大多数游戏包，MTU通常是1500)
		return make([]byte, 2048)
	},
}

// 确保放回池子的是完整容量的切片
func putBack(b []byte) {
	if b != nil {
		bufferPool.Put(b[:cap(b)])
	}
}

type udpSession struct {
	conn       *net.UDPConn
	lastActive int64
}

type udpKey struct {
	ip   [16]byte
	port int
}

func newUDPKey(addr *net.UDPAddr) udpKey {
	k := udpKey{port: addr.Port}
	copy(k.ip[:], addr.IP.To16())
	return k
}

func main() {
	fmt.Println(">> v6bridge | 端口映射工具, 主要用于游戏ipv6联机 \n>> 详情参考 GitHub: https://github.com/lemonc7/v6bridge")
	// 通过context实现优雅关闭
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM)
	defer stop()

	// 1. 尝试加载 config.yml
	data, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("[ERROR] 读取配置文件失败: %v\n", err)
	}

	// 2. 解析配置
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("[ERROR] 解析配置文件失败: %v\n", err)
	}

	// 3. 并发运行所有隧道
	var wg sync.WaitGroup
	for host, tunnels := range cfg {
		for _, t := range tunnels {
			// 检查端口范围 (1-65535)
			if t.Local <= 0 || t.Local > 65535 || t.Remote <= 0 || t.Remote > 65535 {
				log.Printf("[WARN] (%s) 跳过无效配置 (端口必须在 1-65535 之间) Host: %s, Local: %d, Remote: %d\n", t.Name, host, t.Local, t.Remote)
				continue
			}

			wg.Go(func() {
				startTunnelService(ctx, host, t)
			})
		}
	}

	// 等待退出信号
	<-ctx.Done()
	log.Println("[INFO] 收到退出信号，正在释放资源...")

	// 同步等待所有协程退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[INFO] 所有隧道已安全关闭")
	case <-time.After(3 * time.Second):
		log.Println("[WARN] 等待超时，强制退出")
	}
}

func startTunnelService(ctx context.Context, host string, t TunnelItem) {
	proto := strings.ToLower(t.Proto)
	if proto == "" {
		proto = "udp"
	}

	name := t.Name
	if name == "" {
		name = fmt.Sprintf("%s-%d", proto, t.Local)
	}

	local := fmt.Sprintf(":%d", t.Local)
	remote := net.JoinHostPort(host, strconv.Itoa(t.Remote))

	if proto == "tcp" {
		startTCPBridge(ctx, local, remote, name)
	} else {
		startUDPBridge(ctx, local, remote, name)
	}
}

func startUDPBridge(ctx context.Context, local, remote, name string) {
	// 解析本地UDP
	lAddr, err := net.ResolveUDPAddr("udp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) 解析本地地址 %s 失败: %v\n", name, local, err)
		return
	}

	// 解析远程UDP
	rAddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		log.Printf("[ERROR] (%s) 解析远程地址 %s 失败: %v\n", name, remote, err)
		return
	}

	// 在本地UDP端口启动监听，准备接收数据包
	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 监听 %s 失败: %v\n", name, local, err)
		return
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	// 游戏场景: 增加内核缓冲区，防止瞬时高频包丢失
	_ = conn.SetReadBuffer(2 * 1024 * 1024)
	_ = conn.SetWriteBuffer(2 * 1024 * 1024)

	sessions := make(map[udpKey]*udpSession)
	var mu sync.RWMutex

	// 清理过期的 UDP 会话
	go func() {
		// 每隔30s检查一次
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				mu.Lock()
				for _, s := range sessions {
					s.conn.Close()
				}
				mu.Unlock()
				return
			case <-ticker.C:
				now := time.Now().Unix()
				mu.Lock()
				for k, s := range sessions {
					// 清理掉120s没活动的会话
					if now-s.lastActive > 120 {
						s.conn.Close()
						delete(sessions, k)
					}
				}
				mu.Unlock()
			}
		}
	}()

	log.Printf("[INFO] (%s) [UDP] 隧道启动 %s -> %s\n", name, lAddr, rAddr)

	for {
		// 从池中获取buffer
		buf := bufferPool.Get().([]byte)
		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// 没读到数据，立刻还回去
			putBack(buf)
			if ctx.Err() != nil {
				return
			}
			continue
		}

		key := newUDPKey(cliAddr)
		// 找到或创建新的 Session
		mu.RLock()
		s, ok := sessions[key] // 检查是否已经为该客户端创建了远程连接
		mu.RUnlock()

		if !ok {
			// 未找到，创建新会话
			rConn, err := net.DialUDP("udp", nil, rAddr)
			if err != nil {
				putBack(buf)
				log.Printf("[WARN] (%s) 转发远程失败: %v", name, err)
				continue
			}
			s = &udpSession{
				conn:       rConn,
				lastActive: time.Now().Unix(),
			}
			mu.Lock()
			sessions[key] = s
			mu.Unlock()

			// 启动反向回包协程
			go udpReverseLoop(ctx, conn, cliAddr, key, s, sessions, &mu, name)
		}
		s.lastActive = time.Now().Unix()

		_, _ = s.conn.Write(buf[:n])
		// 用完了，放回池中
		putBack(buf)
	}
}

func udpReverseLoop(
	ctx context.Context,
	localConn *net.UDPConn,
	clientAddr *net.UDPAddr,
	key udpKey,
	s *udpSession,
	sessions map[udpKey]*udpSession,
	mu *sync.RWMutex,
	name string,
) {
	defer func() {
		// 会话结束，关闭远程连接，并从map中移除
		s.conn.Close()
		mu.Lock()
		delete(sessions, key)
		mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		buf := bufferPool.Get().([]byte)
		// 设置读取超时
		_ = s.conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		// 读取远程服务器返回客户端的包
		rn, _, rErr := s.conn.ReadFromUDP(buf)
		if rErr != nil {
			putBack(buf)
			if ctx.Err() != nil {
				return
			}
			if err, ok := rErr.(net.Error); ok && err.Timeout() {
				log.Printf("[INFO] (%s) 会话闲置超时已清理\n", name)
			} else {
				log.Printf("[WARN] (%s) 会话读取远程数据中断: %v\n", name, rErr)
			}
			return
		}
		s.lastActive = time.Now().Unix()
		// 转发回客户端
		_, _ = localConn.WriteToUDP(buf[:rn], clientAddr)
		putBack(buf)
	}
}

func startTCPBridge(ctx context.Context, local, remote, name string) {
	lAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) 解析本地地址 %s 失败: %v", name, local, err)
		return
	}

	listener, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 监听 %s 失败: %v", name, local, err)
		return
	}

	// 监听Ctx
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	log.Printf("[INFO] (%s) [TCP] 隧道启动 %s -> %s\n", name, lAddr, remote)

	for {
		clientConn, err := listener.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[WARN] [%s] 接受连接错误: %v", name, err)
			continue
		}

		// 设置TCP KeepAlive 检查死连接
		_ = clientConn.SetKeepAlive(true)
		_ = clientConn.SetKeepAlivePeriod(30 * time.Second)
		go handleTCPConnection(ctx, clientConn, remote, name)
	}
}

func handleTCPConnection(ctx context.Context, clientConn *net.TCPConn, remoteAddr string, name string) {
	defer clientConn.Close()
	// 使用带超时的Dialer
	d := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	c, err := d.DialContext(ctx, "tcp", remoteAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 远程连接失败: %v", name, err)
		return
	}
	remoteConn := c.(*net.TCPConn)
	defer remoteConn.Close()

	var wg sync.WaitGroup
	// Client -> Remote
	wg.Go(func() {
		_, err := io.Copy(remoteConn, clientConn)
		if err != nil && err != io.EOF {
			log.Printf("[WARN] (%s) 客户端->远程 数据流异常终止: %v\n", name, err)
		}
		// 告诉远程: 我发完了
		remoteConn.CloseWrite()
	})

	// Remote -> Client
	wg.Go(func() {
		_, err := io.Copy(clientConn, remoteConn)
		if err != nil && err != io.EOF {
			log.Printf("[WARN] (%s) 远程->客户端 数据流异常终止: %v\n", name, err)
		}
		// 告诉客户端: 远程发完了
		clientConn.CloseWrite()
	})

	// 阻塞等待直到拷贝结束或外部强制退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		log.Printf("[INFO] (%s) 系统退出，强制断开活跃的 TCP 连接\n", name)
	case <-done:
		// 正常退出
	}
}

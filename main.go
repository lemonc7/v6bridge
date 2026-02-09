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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. 尝试加载 config.yml
	configFile, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("[ERROR] 读取配置文件失败: %v\n", err)
	}

	// 2. 解析配置
	var config Config
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		log.Fatalf("[ERROR] 解析配置文件失败: %v\n", err)
	}

	// 3. 并发运行所有隧道
	var wg sync.WaitGroup
	for host, tunnels := range config {
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
	log.Println("[INFO] 正在下线所有隧道，请稍等...")

	// 同步等待所有协程退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[INFO] 资源清理完毕，安全退出")
	case <-time.After(3 * time.Second):
		log.Println("[WARN] 部分连接未在规定时间内关闭，强制结束")
	}
}

func startTunnelService(ctx context.Context, host string, t TunnelItem) {
	proto := strings.ToLower(t.Proto)
	if proto == "" {
		proto = "udp"
	}

	// 如果没有名字，生成一个
	name := t.Name
	if name == "" {
		name = fmt.Sprintf("%s-%d", proto, t.Local)
	}

	localAddr := fmt.Sprintf(":%d", t.Local)
	remoteAddr := net.JoinHostPort(host, strconv.Itoa(t.Remote))

	if strings.ToLower(proto) == "tcp" {
		startTCPBridge(ctx, localAddr, remoteAddr, name)
	} else {
		startUDPBridge(ctx, localAddr, remoteAddr, name)
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

	// 游戏场景: 增加内核缓冲区，防止瞬时高频包丢失
	if err := conn.SetReadBuffer(2 * 1024 * 1024); err != nil {
		log.Printf("[WARN] (%s) 无法设置内核读缓冲区: %v\n", name, err)
	}
	_ = conn.SetWriteBuffer(2 * 1024 * 1024)

	// 监听ctx，关闭监视器退出循环
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	sessions := make(map[string]*net.UDPConn)
	var sessionsLock sync.RWMutex

	log.Printf("[INFO] (%s) [UDP] 隧道启动 %s -> %s\n", name, lAddr, rAddr)

	for {
		// 从池中获取buffer
		buf := bufferPool.Get().([]byte)
		// 设置读取超时，防止主循环异常卡死
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			log.Printf("[ERROR] (%s) 设置读取超时失败: %v\n", name, err)
			putBack(buf)
			break
		}

		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// 没读到数据，立刻还回去
			putBack(buf)
			select {
			case <-ctx.Done():
				return //收到程序关闭通知，直接退出
			default:
				log.Printf("[WARN] (%s) 读取 UDP 错误: %v\n", name, err)
				continue
			}
		}

		key := cliAddr.String()

		// 找到或创建新的 Session
		sessionsLock.RLock()
		session, ok := sessions[key]
		sessionsLock.RUnlock()

		if !ok {
			// 未找到，加写锁创建新会话
			sessionsLock.Lock()
			// 双重检查，防止并发冲突
			if session, ok = sessions[key]; !ok {
				rConn, err := net.DialUDP("udp", nil, rAddr)
				if err != nil {
					log.Printf("[ERROR] (%s) 连接远程主机失败: %v", name, err)
					sessionsLock.Unlock()
					putBack(buf)
					continue
				}
				session = rConn
				sessions[key] = session

				// 启动异步协程处理回包 (Remote -> Local)
				go func(remoteConn *net.UDPConn, clientAddr *net.UDPAddr, key string) {
					defer remoteConn.Close()
					defer func() {
						sessionsLock.Lock()
						delete(sessions, key)
						sessionsLock.Unlock()
					}()

					// 从池中获取buffer
					respBuf := bufferPool.Get().([]byte)
					defer putBack(respBuf)

					for {
						_ = remoteConn.SetReadDeadline(time.Now().Add(2 * time.Minute))
						rn, _, rErr := remoteConn.ReadFromUDP(respBuf)
						if rErr != nil {
							if err, ok := rErr.(net.Error); ok && err.Timeout() {
								log.Printf("[INFO] (%s) 会话闲置超时已清理\n", name)
							} else {
								log.Printf("[WARN] (%s) 会话读取远程数据中断: %v\n", name, err)
							}
							return
						}
						// 写回数据给客户端
						_, wErr := conn.WriteToUDP(respBuf[:rn], clientAddr)
						if wErr != nil {
							log.Printf("[WARN] (%s) 向客户端回包失败: %v\n", name, wErr)
							return
						}
					}
				}(session, cliAddr, key)
			}
			sessionsLock.Unlock()
		}

		_, err = session.Write(buf[:n])
		// 用完了，放回池中
		putBack(buf)
		if err != nil {
			log.Printf("[ERROR] (%s) 转发数据到远程失败: %v", name, err)
		}
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
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("[ERROR] [%s] 接受连接错误: %v", name, err)
				continue
			}
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
	d := net.Dialer{Timeout: 10 * time.Second}
	c, err := d.DialContext(ctx, "tcp", remoteAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 连接远程失败: %v", name, err)
		return
	}
	remoteConn := c.(*net.TCPConn)
	defer remoteConn.Close()

	_ = remoteConn.SetKeepAlive(true)
	_ = remoteConn.SetKeepAlivePeriod(30 * time.Second)

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

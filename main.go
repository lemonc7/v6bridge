package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// TunnelItem 代表单个隧道配置项
// 使用简写字段名，与 config.yml 保持一致
type TunnelItem struct {
	Name   string `yaml:"name"`
	Remote int    `yaml:"remote"`
	Local  int    `yaml:"local"`
	Proto  string `yaml:"proto"`
}

// Config 主要是 map[string][]TunnelItem
// Key 是 Hostname
type Config map[string][]TunnelItem

func main() {
	// 1. 尝试加载 config.yml
	var config Config
	configFile, err := os.ReadFile("config.yml")
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("错误: 未找到配置文件 'config.yml'。请确保该文件存在于当前目录下。")
		} else {
			log.Fatalf("错误: 读取 'config.yml' 失败: %v", err)
		}
	}

	// 2. 解析配置
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		log.Fatalf("错误: 解析 'config.yml' 失败 (格式不正确): %v", err)
	}

	if len(config) == 0 {
		log.Fatalf("错误: 'config.yml' 内容为空或不包含任何有效的主机配置。")
	}

	fmt.Println("已从 config.yml 加载配置")

	// 3. 并发运行所有隧道
	var wg sync.WaitGroup
	for host, tunnels := range config {
		for _, t := range tunnels {
			// 检查端口范围 (1-65535)
			if t.Local <= 0 || t.Local > 65535 || t.Remote <= 0 || t.Remote > 65535 {
				log.Printf("警告: [%s] 跳过无效配置 (端口必须在 1-65535 之间) Host: %s, Local: %d, Remote: %d", t.Name, host, t.Local, t.Remote)
				continue
			}

			wg.Add(1)
			go func(h string, p string, lp, rp int, n string) {
				defer wg.Done()
				if err := startTunnelService(h, p, lp, rp, n); err != nil {
					log.Printf("启动隧道失败 [%s] %s:%d -> %s:%d: %v", n, p, lp, h, rp, err)
				}
			}(host, t.Proto, t.Local, t.Remote, t.Name)
		}
	}
	wg.Wait()
}

func startTunnelService(host, protocol string, localPort, remotePort int, name string) error {
	if protocol == "" {
		protocol = "udp"
	}

	// 如果没有名字，生成一个
	tunnelName := name
	if tunnelName == "" {
		tunnelName = fmt.Sprintf("%s-%d", protocol, localPort)
	}

	localAddr := fmt.Sprintf(":%d", localPort)
	remoteAddr := ""

	// 1. 首先检查 Host 是否已经是 IP 地址
	ip := net.ParseIP(host)
	if ip != nil {
		// 是直接的 IP 地址 (v4 或 v6)
		if ip.To4() == nil {
			// IPv6
			remoteAddr = fmt.Sprintf("[%s]:%d", ip.String(), remotePort)
			fmt.Printf("[%s] 使用直接配置的 IPv6 地址: %s\n", tunnelName, ip.String())
		} else {
			// IPv4
			remoteAddr = fmt.Sprintf("%s:%d", ip.String(), remotePort)
			fmt.Printf("[%s] 使用直接配置的 IPv4 地址: %s\n", tunnelName, ip.String())
		}
	} else {
		// 2. 是域名，进行解析
		ips, err := net.LookupIP(host)
		if err == nil {
			var ipv6 net.IP
			for _, resolveData := range ips {
				if resolveData.To4() == nil {
					ipv6 = resolveData
					break
				}
			}
			if ipv6 != nil {
				remoteAddr = fmt.Sprintf("[%s]:%d", ipv6.String(), remotePort)
				fmt.Printf("[%s] 已成功将 %s 解析为 %s\n", tunnelName, host, ipv6.String())
			} else {
				// 未解析到 V6，尝试回退到默认 (如 V4)
				if len(ips) > 0 {
					remoteAddr = fmt.Sprintf("%s:%d", ips[0].String(), remotePort)
					fmt.Printf("[%s] 警告: %s 未解析到 IPv6，回退使用 IP: %s\n", tunnelName, host, ips[0].String())
				} else {
					remoteAddr = fmt.Sprintf("%s:%d", host, remotePort)
				}
			}
		} else {
			// 解析失败，按原样使用
			remoteAddr = fmt.Sprintf("%s:%d", host, remotePort)
			log.Printf("警告: [%s] 无法解析主机 '%s': %v。将按原样使用。", tunnelName, host, err)
		}
	}

	runBridge(localAddr, remoteAddr, protocol, tunnelName)
	return nil
}

func runBridge(localAddr, remoteAddr, protocol, name string) {
	protocol = strings.ToLower(protocol)
	if protocol != "tcp" && protocol != "udp" {
		log.Printf("[%s] 无效协议: %s。必须是 tcp 或 udp。", name, protocol)
		return
	}

	fmt.Printf("正在启动 %s 桥接 [%s]: %s -> %s\n", strings.ToUpper(protocol), name, localAddr, remoteAddr)

	if protocol == "udp" {
		startUDPBridge(localAddr, remoteAddr, name)
	} else {
		startTCPBridge(localAddr, remoteAddr, name)
	}
}

func startUDPBridge(local, remote, name string) {
	lAddr, err := net.ResolveUDPAddr("udp", local)
	if err != nil {
		log.Printf("[%s] 解析本地地址 %s 失败: %v", name, local, err)
		return
	}

	rAddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		log.Printf("[%s] 解析远程地址 %s 失败: %v", name, remote, err)
		return
	}

	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		log.Printf("[%s] 监听 %s 失败: %v", name, local, err)
		return
	}
	defer conn.Close()

	log.Printf("[%s] [UDP] 正在监听 %s 并转发至 %s\n", name, lAddr, rAddr)

	sessions := make(map[string]*net.UDPConn)
	var sessionsLock sync.Mutex

	buf := make([]byte, 65535)

	for {
		n, cliAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("[%s] 读取 UDP 错误: %v", name, err)
			continue
		}

		sessionsLock.Lock()
		session, ok := sessions[cliAddr.String()]
		if !ok {
			rConn, err := net.DialUDP("udp", nil, rAddr)
			if err != nil {
				log.Printf("[%s] 连接远程主机失败: %v", name, err)
				sessionsLock.Unlock()
				continue
			}

			session = rConn
			sessions[cliAddr.String()] = session

			go func(remoteConn *net.UDPConn, clientAddr *net.UDPAddr) {
				defer remoteConn.Close()
				defer func() {
					sessionsLock.Lock()
					delete(sessions, clientAddr.String())
					sessionsLock.Unlock()
				}()

				respBuf := make([]byte, 65535)
				for {
					remoteConn.SetReadDeadline(time.Now().Add(5 * time.Minute))
					rn, _, rErr := remoteConn.ReadFromUDP(respBuf)
					if rErr != nil {
						return
					}
					_, wErr := conn.WriteToUDP(respBuf[:rn], clientAddr)
					if wErr != nil {
						return
					}
				}
			}(session, cliAddr)
		}
		sessionsLock.Unlock()

		_, err = session.Write(buf[:n])
		if err != nil {
			log.Printf("[%s] 写入远程错误: %v", name, err)
		}
	}
}

func startTCPBridge(local, remote, name string) {
	lAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		log.Printf("[%s] 解析本地地址 %s 失败: %v", name, local, err)
		return
	}

	rAddr, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		log.Printf("[%s] 解析远程地址 %s 失败: %v", name, remote, err)
		return
	}

	listener, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		log.Printf("[%s] 监听 %s 失败: %v", name, local, err)
		return
	}
	defer listener.Close()

	log.Printf("[%s] [TCP] 正在监听 %s 并转发至 %s\n", name, lAddr, rAddr)

	for {
		clientConn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("[%s] 接受连接错误: %v", name, err)
			continue
		}

		go handleTCPConnection(clientConn, rAddr, name)
	}
}

func handleTCPConnection(clientConn *net.TCPConn, remoteAddr *net.TCPAddr, name string) {
	defer clientConn.Close()

	remoteConn, err := net.DialTCP("tcp", nil, remoteAddr)
	if err != nil {
		log.Printf("[%s] 连接远程主机失败: %v", name, err)
		return
	}
	defer remoteConn.Close()

	go io.Copy(remoteConn, clientConn)
	io.Copy(clientConn, remoteConn)
}

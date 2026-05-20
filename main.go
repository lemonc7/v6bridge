package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	cfg, err := LoadConfig("config.yml")
	if err != nil {
		logger.Fatalf("[ERROR] %v", err)
	}
	if len(cfg.Tunnels) == 0 {
		logger.Fatalf("[ERROR] 未发现有效配置，程序退出")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Println(">> v6bridge | 端口映射工具, 主要用于游戏 ipv6 联机")
	logger.Println(">> 详情参考 GitHub: https://github.com/lemonc7/v6bridge")
	bufPool := NewBufferPool(cfg.Network.BufferSize)

	var wg sync.WaitGroup
	for _, tunnel := range cfg.Tunnels {
		for _, svc := range tunnel.Services {
			localAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(svc.Local))
			remoteAddr := JoinRemoteAddr(tunnel.Host, svc.Remote)

			logger.Printf("[INFO] (%s) 启动监听: %d -> %s (%s)", svc.Name, svc.Local, remoteAddr, svc.Proto)

			switch svc.Proto {
			case ProtocolTCP:
				wg.Go(func() {
					StartTCPProxy(ctx, logger, cfg.Network, bufPool, svc.Name, localAddr, remoteAddr)
				})
			case ProtocolUDP:
				wg.Go(func() {
					StartUDPProxy(ctx, logger, cfg.Network, bufPool, svc.Name, localAddr, remoteAddr)
				})
			case ProtocolBoth:
				wg.Go(func() {
					StartTCPProxy(ctx, logger, cfg.Network, bufPool, svc.Name, localAddr, remoteAddr)
				})
				wg.Go(func() {
					StartUDPProxy(ctx, logger, cfg.Network, bufPool, svc.Name, localAddr, remoteAddr)
				})
			}
		}
	}

	logger.Println("[INFO] 所有隧道已建立，运行中... (按 Ctrl+C 退出)")
	<-ctx.Done()
	logger.Println("收到退出信号，程序关闭")
	wg.Wait()
}

func JoinRemoteAddr(host string, port int) string {
	host = strings.TrimSpace(host)
	trimmed := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	return net.JoinHostPort(trimmed, strconv.Itoa(port))
}

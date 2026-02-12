package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	fmt.Println(">> v6bridge | 端口映射工具, 主要用于游戏ipv6联机 \n>> 详情参考 GitHub: https://github.com/lemonc7/v6bridge")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	cfg := loadConfig("config.yml")
	b := NewBridge(cfg)

	var (
		wg    sync.WaitGroup
		stats []*ServiceStats
	)

	// 遍历配置，启动所有隧道
	for _, tunnel := range cfg.Tunnels {
		for _, t := range tunnel.Services {
			if t.Local <= 0 || t.Local > 65535 || t.Remote <= 0 || t.Remote > 65535 {
				log.Printf("[WARN] (%s) 跳过无效端口配置: %s, Local: %d, Remote: %d",
					t.Name, tunnel.Host, t.Local, t.Remote)
				continue
			}

			proto := strings.ToLower(t.Proto)
			if proto == "" {
				proto = "udp"
			}
			name := t.Name
			if name == "" {
				name = fmt.Sprintf("%s-%d", proto, t.Local)
			}

			local := fmt.Sprintf(":%d", t.Local)
			remote := net.JoinHostPort(tunnel.Host, strconv.Itoa(t.Remote))

			stat := &ServiceStats{
				Name:      name,
				Proto:     proto,
				LocalPort: t.Local,
			}
			stats = append(stats, stat)

			if proto == "tcp" {
				wg.Go(func() {
					serveTCP(ctx, b, &wg, local, remote, stat)
				})
			} else {
				wg.Go(func() {
					serveUDP(ctx, b, &wg, local, remote, stat)
				})
			}
		}
	}

	// 启动定期报告
	reportInterval := time.Duration(cfg.Setting.ReportInterval) * time.Second
	if reportInterval > 0 {
		wg.Go(func() {
			reportStats(ctx, stats, reportInterval)
		})
	}

	<-ctx.Done()
	log.Println("[INFO] 接收到退出信号，正在释放资源...")

	// 等待所有协程退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		printFinalStats(stats)
		log.Println("[INFO] 所有隧道已完成清理并退出")
	case <-time.After(5 * time.Second):
		printFinalStats(stats)
		log.Println("[WARN] 等待部分连接关闭超时，正在强制退出")
	}
}

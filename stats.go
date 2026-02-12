package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// ServiceStats 单个服务的流量统计
type ServiceStats struct {
	Name      string
	Proto     string
	LocalPort int
	Sent      atomic.Uint64
	Recv      atomic.Uint64
}

// reportStats 定期打印流量报告
func reportStats(ctx context.Context, stats []*ServiceStats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printStats(stats)
		}
	}
}

func printStats(stats []*ServiceStats) {
	now := time.Now().Format("15:04:05")
	fmt.Printf("\n[%s] 实时流量报告:\n", now)
	for _, s := range stats {
		sent := formatBytes(s.Sent.Load())
		recv := formatBytes(s.Recv.Load())
		fmt.Printf("  • %-12s [%s:%d]  ↑ %-10s  ↓ %s\n", s.Name, strings.ToUpper(s.Proto), s.LocalPort, sent, recv)
	}
}

func printFinalStats(stats []*ServiceStats) {
	var totalSent, totalRecv uint64
	fmt.Printf("\n[FINAL] 运行总结报告:\n")
	for _, s := range stats {
		sentVal := s.Sent.Load()
		recvVal := s.Recv.Load()
		totalSent += sentVal
		totalRecv += recvVal
		fmt.Printf("  - %-12s: ↑ %-10s  ↓ %s\n", s.Name, formatBytes(sentVal), formatBytes(recvVal))
	}
	fmt.Printf("\n累计数据交换总量: ↑ %s  ↓ %s\n", formatBytes(totalSent), formatBytes(totalRecv))
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

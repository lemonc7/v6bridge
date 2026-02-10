package main

import (
	"net"
	"testing"
	"time"
)

func BenchmarkUDPForwarding(b *testing.B) {
	ctx := b.Context()

	remoteAddr := "127.0.0.1:40001"
	localAddr := "127.0.0.1:40002"

	// 1. 初始化配置 (适配你最新的 Setting 名称)
	var cfg Config
	cfg.Setting.SessionTimeout = 120
	cfg.Setting.CleanupInterval = 30
	cfg.Setting.SocketBufSize = 4     // 4MB
	cfg.Setting.PacketBufferSize = 64 // 64KB

	// 2. 模拟后端接收端 (Receiver)
	rAddr, _ := net.ResolveUDPAddr("udp", remoteAddr)
	remoteConn, err := net.ListenUDP("udp", rAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer remoteConn.Close()

	go func() {
		buf := make([]byte, 2048)
		for {
			_, _, err := remoteConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
		}
	}()

	// 3. 启动转发器 (Manager)
	mgr := NewManager(ctx, cfg) // 传入配置
	go mgr.runUDP(localAddr, remoteAddr, "bench-udp")
	time.Sleep(500 * time.Millisecond) // 等待监听就绪

	// 4. 并发客户端测试
	payload := []byte("high-performance-test-payload-12345")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// 每个并发协程建议持有自己的连接，或者使用全局连接但注意系统调用竞争
		// UDP 是无连接的，Dial 只是固定了目标地址
		cliConn, err := net.Dial("udp", localAddr)
		if err != nil {
			return
		}
		defer cliConn.Close()
		for pb.Next() {
			_, err := cliConn.Write(payload)
			if err != nil {
				break
			}
		}
	})
}

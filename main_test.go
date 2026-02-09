package main

import (
	"net"
	"testing"
	"time"
)

// --- 单元测试：验证逻辑正确性 ---

// TestUDPBridge_Correctness 测试 UDP 是否能正确转发双向数据
func TestUDPBridge_Correctness(t *testing.T) {
	ctx := t.Context()

	remoteAddr := "127.0.0.1:20001"
	localAddr := "127.0.0.1:20002"
	testMsg := "ping-udp-v6bridge"

	// 1. 模拟远程服务器：收到什么就回显什么
	rAddr, _ := net.ResolveUDPAddr("udp", remoteAddr)
	remoteConn, err := net.ListenUDP("udp", rAddr)
	if err != nil {
		t.Fatalf("启动模拟远程服务器失败: %v", err)
	}
	defer remoteConn.Close()

	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := remoteConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = remoteConn.WriteToUDP(buf[:n], addr)
		}
	}()

	// 2. 启动转发服务
	mgr := NewManager(ctx)
	go mgr.runUDP(localAddr, remoteAddr, "test-udp")
	time.Sleep(200 * time.Millisecond) // 等待监听就绪

	// 3. 模拟客户端发送数据到本地端口
	cliConn, err := net.Dial("udp", localAddr)
	if err != nil {
		t.Fatalf("客户端连接本地端口失败: %v", err)
	}
	defer cliConn.Close()

	_, _ = cliConn.Write([]byte(testMsg))

	// 4. 接收回包验证
	cliConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp := make([]byte, 2048)
	n, err := cliConn.Read(resp)
	if err != nil {
		t.Fatalf("未收到回包: %v", err)
	}

	if string(resp[:n]) != testMsg {
		t.Errorf("数据不一致: 期望 %s, 得到 %s", testMsg, string(resp[:n]))
	}
}

// TestTCPBridge_Correctness 测试 TCP 转发和 CloseWrite 逻辑
func TestTCPBridge_Correctness(t *testing.T) {
	ctx := t.Context()

	remoteAddr := "127.0.0.1:30001"
	localAddr := "127.0.0.1:30002"
	testMsg := "hello-tcp-v6bridge"

	// 1. 模拟远程 TCP 服务器
	ln, err := net.Listen("tcp", remoteAddr)
	if err != nil {
		t.Fatalf("启动模拟远程 TCP 失败: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		defer conn.Close()
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		_, _ = conn.Write(buf[:n])
	}()

	// 2. 启动转发服务
	mgr := NewManager(ctx)
	go mgr.runTCP(localAddr, remoteAddr, "test-tcp")
	time.Sleep(200 * time.Millisecond)

	// 3. 客户端测试
	cliConn, err := net.Dial("tcp", localAddr)
	if err != nil {
		t.Fatalf("客户端连接失败: %v", err)
	}
	defer cliConn.Close()

	_, _ = cliConn.Write([]byte(testMsg))
	resp := make([]byte, 1024)
	n, _ := cliConn.Read(resp)

	if string(resp[:n]) != testMsg {
		t.Errorf("TCP 数据转发错误")
	}
}

// --- 压力测试：验证性能极限 ---

// BenchmarkUDPForwarding 压测 UDP 转发能力及内存分配
func BenchmarkUDPForwarding(b *testing.B) {
	ctx := b.Context()

	remoteAddr := "127.0.0.1:40001"
	localAddr := "127.0.0.1:40002"

	// 1. 模拟后端接收端
	rAddr, _ := net.ResolveUDPAddr("udp", remoteAddr)
	remoteConn, _ := net.ListenUDP("udp", rAddr)
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

	// 2. 启动转发器
	mgr := NewManager(ctx)
	go mgr.runUDP(localAddr, remoteAddr, "bench-udp")
	time.Sleep(500 * time.Millisecond)

	// 3. 客户端
	cliConn, _ := net.Dial("udp", localAddr)
	defer cliConn.Close()
	payload := []byte("high-performance-test-payload-12345")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := cliConn.Write(payload)
			if err != nil {
				return
			}
		}
	})
}

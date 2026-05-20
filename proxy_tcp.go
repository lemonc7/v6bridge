package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

func StartTCPProxy(ctx context.Context, logger *log.Logger, network NetworkConfig, name, local, remote string) {
	localAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		logger.Printf("[ERROR] (%s) TCP 本地地址无效 (%s): %v", name, local, err)
		return
	}

	listener, err := net.ListenTCP("tcp", localAddr)
	if err != nil {
		logger.Printf("[ERROR] (%s) TCP 绑定本地端口失败 (%s): %v", name, local, err)
		return
	}
	defer listener.Close()
	logger.Printf("[INFO] (%s) TCP 隧道已启动: %s -> %s", name, local, remote)

	stop := context.AfterFunc(ctx, func() {
		_ = listener.Close()
	})
	defer func() {
		stop()
		logger.Printf("[INFO] (%s) TCP 隧道已关闭: %s", name, local)
	}()

	for {
		client, err := listener.AcceptTCP()
		if err != nil {
			if isClosed(ctx, err) {
				return
			}
			logger.Printf("[ERROR] (%s) 接收 TCP 连接错误: %v", name, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		logger.Printf("[INFO] (%s) 新 TCP 客户端连接: %s", name, client.RemoteAddr())
		go handleTCPConn(ctx, logger, network, name, client, remote)
	}
}

func handleTCPConn(ctx context.Context, logger *log.Logger, network NetworkConfig, name string, client *net.TCPConn, remote string) {
	defer client.Close()
	setTCPOptions(client, network.BufferSize)

	dialer := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.DialContext(ctx, "tcp", remote)
	if err != nil {
		logger.Printf("[ERROR] (%s) TCP 远程连接失败: %v", name, err)
		return
	}

	server, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		logger.Printf("[ERROR] (%s) TCP 远程连接类型异常: %T", name, conn)
		return
	}
	defer server.Close()
	setTCPOptions(server, network.BufferSize)
	logger.Printf("[INFO] (%s) TCP 已连接远程: %s -> %s", name, client.RemoteAddr(), remote)

	// context取消时，关闭两端的socket
	stop := context.AfterFunc(ctx, func() {
		_ = client.Close()
		_ = server.Close()
	})
	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() {
		pipeTCP(ctx, logger, network, name, "客户端->服务端", server, client)
	})
	wg.Go(func() {
		pipeTCP(ctx, logger, network, name, "服务端->客户端", client, server)
	})
	wg.Wait()
	logger.Printf("[INFO] (%s) TCP 连接结束: %s", name, client.RemoteAddr())
}

// 转发数据
func pipeTCP(ctx context.Context, logger *log.Logger, network NetworkConfig, name, direction string, dst *net.TCPConn, src *net.TCPConn) {
	buf := make([]byte, network.BufferSize)

	if _, err := io.CopyBuffer(dst, src, buf); err != nil && !isClosed(ctx, err) {
		logger.Printf("[WARN] (%s) %s TCP 数据转发异常: %v", name, direction, err)
	}
	_ = dst.CloseWrite()
}

// 优化TCP连接
func setTCPOptions(conn *net.TCPConn, bufferSize int) {
	_ = conn.SetReadBuffer(bufferSize)
	_ = conn.SetWriteBuffer(bufferSize)
	_ = conn.SetKeepAlive(true)
	_ = conn.SetKeepAlivePeriod(30 * time.Second)
	_ = conn.SetNoDelay(true)
}

func isClosed(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)
}

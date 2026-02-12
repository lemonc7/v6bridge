package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// serveTCP 启动 TCP 隧道监听并转发连接
func serveTCP(
	ctx context.Context,
	b *Bridge,
	wg *sync.WaitGroup,
	local string,
	remote string,
	stat *ServiceStats,
) {
	l, err := net.Listen("tcp", local)
	if err != nil {
		log.Printf("[ERROR] (%s) TCP端口监听失败 %s: %v", stat.Name, local, err)
		return
	}
	defer l.Close()

	stop := context.AfterFunc(ctx, func() {
		_ = l.Close()
	})
	defer stop()

	log.Printf("[INFO] (%s) [TCP] 隧道已启动: %s -> %s", stat.Name, local, remote)

	listener := l.(*net.TCPListener)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if isClosed(ctx, err) {
				return
			}
			log.Printf("[WARN] (%s) 接受TCP连接错误: %v", stat.Name, err)
			continue
		}

		wg.Go(func() {
			handleTCPConn(ctx, b, conn, remote, stat)
		})
	}
}

// handleTCPConn 处理单个 TCP 连接的双向转发
func handleTCPConn(
	ctx context.Context,
	b *Bridge,
	clientConn *net.TCPConn,
	remoteAddr string,
	stat *ServiceStats,
) {
	defer clientConn.Close()

	// 优化本地端连接
	_ = clientConn.SetReadBuffer(b.socketBufSize)
	_ = clientConn.SetWriteBuffer(b.socketBufSize)
	_ = clientConn.SetKeepAlive(true)
	_ = clientConn.SetKeepAlivePeriod(30 * time.Second)
	_ = clientConn.SetNoDelay(true)

	d := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	c, err := d.DialContext(ctx, "tcp", remoteAddr)
	if err != nil {
		log.Printf("[ERROR] (%s) 无法连接远程TCP服务: %v", stat.Name, err)
		return
	}
	defer c.Close()

	// ctx 取消时关闭两端 socket
	stop := context.AfterFunc(ctx, func() {
		_ = clientConn.Close()
		_ = c.Close()
	})
	defer stop()

	remoteConn := c.(*net.TCPConn)
	// 优化远程端连接
	_ = remoteConn.SetReadBuffer(b.socketBufSize)
	_ = remoteConn.SetWriteBuffer(b.socketBufSize)
	_ = remoteConn.SetNoDelay(true)

	// 双向转发
	done := make(chan struct{}, 1)
	go func() {
		buf := b.getBuf()
		defer b.putBuf(buf)
		// 客户端 -> 服务端
		cw := &countWriter{w: remoteConn, c: &stat.Sent}
		if _, err := io.CopyBuffer(cw, clientConn, buf); err != nil {
			if !isClosed(ctx, err) {
				log.Printf("[WARN] (%s) 客户端->服务端: TCP数据转发异常: %v", stat.Name, err)
			}
		}
		_ = remoteConn.CloseWrite()
		done <- struct{}{}
	}()

	buf := b.getBuf()
	defer b.putBuf(buf)
	// 服务端 -> 客户端
	cw := &countWriter{w: clientConn, c: &stat.Recv}
	if _, err := io.CopyBuffer(cw, remoteConn, buf); err != nil {
		if !isClosed(ctx, err) {
			log.Printf("[WARN] (%s) 服务端->客户端: TCP数据转发异常: %v", stat.Name, err)
		}
	}
	_ = clientConn.CloseWrite()

	<-done
}

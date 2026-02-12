package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Bridge 共享的桥接配置和资源池
type Bridge struct {
	sessionTimeout time.Duration
	socketBufSize  int
	bufferPool     sync.Pool
}

// NewBridge 根据配置创建 Bridge
func NewBridge(cfg Config) *Bridge {
	s := cfg.Setting
	return &Bridge{
		sessionTimeout: time.Duration(s.SessionTimeout) * time.Second,
		socketBufSize:  s.SocketBufSize * 1024 * 1024,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, s.PacketBufferSize*1024)
			},
		},
	}
}

func (b *Bridge) getBuf() []byte {
	return b.bufferPool.Get().([]byte)
}

func (b *Bridge) putBuf(buf []byte) {
	if buf != nil {
		b.bufferPool.Put(buf[:cap(buf)])
	}
}

// isClosed 判断错误是否因连接关闭或 context 取消导致
func isClosed(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		ctx.Err() != nil
}

// countWriter 包装 Writer 并统计写入字节数
type countWriter struct {
	w io.Writer
	c *atomic.Uint64
}

func (cw *countWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.c.Add(uint64(n))
	return
}

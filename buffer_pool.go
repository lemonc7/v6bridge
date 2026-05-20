package main

import "sync"

type BufferPool struct {
	size int
	pool sync.Pool
}

func NewBufferPool(size int) *BufferPool {
	bp := &BufferPool{size: size}
	bp.pool.New = func() any {
		buf := make([]byte, size)
		return &buf
	}
	return bp
}

func (p *BufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

func (p *BufferPool) Put(bufp *[]byte) {
	if bufp == nil || cap(*bufp) != p.size {
		return
	}
	*bufp = (*bufp)[:p.size]
	p.pool.Put(bufp)
}

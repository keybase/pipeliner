package pipeliner

import (
	"golang.org/x/net/context"
	"sync"
)

type Pipeliner struct {
	sync.RWMutex
	Window int
	numOut int
	ch     chan struct{}
	err    error
}

func NewPipeliner(w int) *Pipeliner {
	return &Pipeliner{
		Window: w,
		ch:     make(chan struct{}),
	}
}

func (p *Pipeliner) getError() error {
	p.RLock()
	defer p.RUnlock()
	return p.err
}

func (p *Pipeliner) hasRoom() bool {
	p.RLock()
	defer p.RUnlock()
	return p.numOut < p.Window
}

func (p *Pipeliner) launchOne() {
	p.Lock()
	defer p.Unlock()
	p.numOut++
}

func (p *Pipeliner) WaitForRoom(ctx context.Context) (err error) {
	for {
		p.checkContextDone(ctx)
		if err := p.getError(); err != nil {
			return err
		}
		if p.hasRoom() {
			break
		}
		p.wait(ctx)
	}
	p.launchOne()
	return nil
}

func (p *Pipeliner) CompleteOne(e error) {
	p.setError(e)
	p.landOne()
	p.ch <- struct{}{}
}

func (p *Pipeliner) landOne() {
	p.Lock()
	defer p.Unlock()
	p.numOut--
}

func (p *Pipeliner) hasOutstanding() bool {
	p.RLock()
	defer p.RUnlock()
	return p.numOut > 0
}

func (p *Pipeliner) setError(e error) {
	p.RLock()
	defer p.RUnlock()
	if e != nil && p.err == nil {
		p.err = e
	}
}

func (p *Pipeliner) checkContextDone(ctx context.Context) {
	select {
	case <-ctx.Done():
		p.setError(ctx.Err())
	default:
	}
}

func (p *Pipeliner) wait(ctx context.Context) {
	select {
	case <-p.ch:
	case <-ctx.Done():
		p.setError(ctx.Err())
	}
}

func (p *Pipeliner) Flush(ctx context.Context) (err error) {
	for p.hasOutstanding() {
		p.wait(ctx)
	}
	return p.getError()
}

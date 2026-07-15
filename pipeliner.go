package pipeliner

import (
	"context"
	"sync"
)

// Pipeliner coordinates a flow of parallel requests, rate-limiting so that
// only a fixed number are outstanding at any one given time.
//
// Once an error has been recorded (via CompleteOne, or via context
// cancellation), it is sticky: every subsequent call to WaitForRoom or
// Flush on the same instance returns that error. A Pipeliner is therefore
// single-use per batch — construct a new one for each independent batch of
// work rather than reusing an instance after an error or cancellation.
type Pipeliner struct {
	mu      sync.RWMutex
	window  int
	numOut  int
	ch      chan struct{}
	err     error
	pending sync.WaitGroup // counts reservations awaiting a CompleteOne call
}

// NewPipeliner makes a pipeliner with window size `w`.
func NewPipeliner(w int) *Pipeliner {
	return &Pipeliner{
		window: w,
		ch:     make(chan struct{}),
	}
}

func (p *Pipeliner) getError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.err
}

// tryReserve atomically checks for room in the window and, if available,
// reserves a slot. Checking and reserving under a single lock acquisition
// prevents concurrent callers from both observing room and over-committing
// past the window.
func (p *Pipeliner) tryReserve() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.numOut < p.window {
		p.numOut++
		p.pending.Add(1)
		return true
	}
	return false
}

// WaitForRoom will block until there is room in the window to fire
// another request. It returns an error if any prior request failed,
// instructing the caller to stop firing off new requests. The error
// originates either from CompleteOne(), or from a context-based
// cancellation
func (p *Pipeliner) WaitForRoom(ctx context.Context) error {
	for {
		p.checkContextDone(ctx)
		if err := p.getError(); err != nil {
			p.drain()
			return err
		}
		if p.tryReserve() {
			return nil
		}
		p.wait(ctx)
	}
}

// CompleteOne should be called when a request is completed, to make
// room for subsequent requests. Call it with an error if you want the
// rest of the pipeline to be short-circuited. This is the error that
// is returned from WaitForRoom.
func (p *Pipeliner) CompleteOne(e error) {
	defer p.pending.Done()
	p.setError(e)
	p.landOne()
	p.ch <- struct{}{}
}

func (p *Pipeliner) landOne() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.numOut--
}

func (p *Pipeliner) hasOutstanding() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numOut > 0
}

func (p *Pipeliner) setError(e error) {
	p.mu.Lock()
	defer p.mu.Unlock()
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

// drain blocks until every reserved-but-not-yet-completed request has called
// CompleteOne, so WaitForRoom/Flush can never return an error while leaving
// a goroutine blocked sending on ch. A "receive while numOut > 0" loop won't
// do: numOut can hit zero before the last CompleteOne's send actually lands.
// So instead we race a reader against pending.Wait, which only unblocks once
// every reservation's CompleteOne call has truly returned.
func (p *Pipeliner) drain() {
	done := make(chan struct{})
	go func() {
		p.pending.Wait()
		close(done)
	}()
	for {
		select {
		case <-p.ch:
		case <-done:
			return
		}
	}
}

// Flush any outstanding requests, blocking until the last completes.
// Returns an error set by CompleteOne, or a context-based error
// if any request was canceled mid-flight.
func (p *Pipeliner) Flush(ctx context.Context) error {
	for {
		p.checkContextDone(ctx)
		if err := p.getError(); err != nil {
			p.drain()
			return err
		}
		if !p.hasOutstanding() {
			break
		}
		p.wait(ctx)
	}
	return p.getError()
}

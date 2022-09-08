package sdk

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orcastor/orcas/core"
)

var (
	// ErrFull chan full.
	ErrFull    = errors.New("fanout: chan full")
	ErrTimeout = errors.New("fanout: chan full, wait timeout")
)

type options struct {
	worker int32
	buffer int32
}

// Option fanout option
type Option func(*options)

// Worker specifies the worker of fanout
func Worker(n int) Option {
	if n <= 0 {
		panic("fanout: worker should > 0")
	}
	return func(o *options) {
		o.worker = int32(n)
	}
}

// Buffer specifies the buffer of fanout
func Buffer(n int) Option {
	if n <= 0 {
		panic("fanout: buffer should > 0")
	}
	return func(o *options) {
		o.buffer = int32(n)
	}
}

type item struct {
	f   func(c core.Ctx)
	ctx core.Ctx
}

// Fanout async consume data from chan.
type Fanout struct {
	taskNum     int32
	ch          chan item
	options     *options
	waiter      sync.WaitGroup
	ctx         core.Ctx
	cancel      func()
	state       int32
	closeWorker chan struct{}
	closeChan   chan struct{}
	once        sync.Once
}

// NewFanout new a fanout struct.
func NewFanout(opts ...Option) *Fanout {
	o := &options{
		worker: 16,
		buffer: 1024,
	}
	for _, op := range opts {
		op(o)
	}
	c := &Fanout{
		taskNum:     0,
		ch:          make(chan item, o.buffer),
		options:     o,
		closeChan:   make(chan struct{}),
		once:        sync.Once{},
		closeWorker: make(chan struct{}),
	}
	atomic.StoreInt32(&c.state, 1)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.waiter.Add(int(o.worker))
	for i := 0; i < int(o.worker); i++ {
		go c.proc()
	}
	return c
}

// TuneWorker Dynamically adjust the worker number
func (c *Fanout) TuneWorker(n int) {
	if n > int(c.options.worker) {
		needAddNum := n - int(c.options.worker)
		for i := 0; i < needAddNum; i++ {
			c.waiter.Add(1)
			go c.proc()
			atomic.StoreInt32(&c.options.worker, atomic.LoadInt32(&c.options.worker)+1)
		}
	} else {
		needAddNum := int(c.options.worker) - n
		for i := 0; i < needAddNum; i++ {
			go func() {
				c.closeChan <- struct{}{}
			}()
		}
	}
}

// TunePool Dynamically adjust the gorutine pool size
func (c *Fanout) TunePool(n int) {
	if n == int(c.options.buffer) {
		return
	}
	dirtyCh := make(chan item, n)
	for {
		select {
		case task := <-c.ch:
			dirtyCh <- task
		default:
			c.ch = dirtyCh
			atomic.StoreInt32(&c.options.buffer, int32(n))
			return
		}
	}
}

func (c *Fanout) proc() {
	defer func() {
		atomic.StoreInt32(&c.options.worker, atomic.LoadInt32(&c.options.worker)-1)
		c.waiter.Done()
	}()
	for {
		select {
		case t := <-c.ch:
			c.wrapFunc(t.f)(t.ctx)
			atomic.StoreInt32(&c.taskNum, atomic.LoadInt32(&c.taskNum)-1)
			if atomic.LoadInt32(&c.state) == 0 && len(c.ch) == 0 {
				c.once.Do(
					func() { close(c.closeChan) })
				return
			}
		case <-c.ctx.Done():
			return
		case <-c.closeWorker:
			return
		case <-c.closeChan:
			return
		}
	}
}

func (c *Fanout) wrapFunc(f func(c core.Ctx)) (res func(core.Ctx)) {
	res = func(ctx core.Ctx) {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 64*1024)
				buf = buf[:runtime.Stack(buf, false)]
				fmt.Printf("panic in fanout proc, err: %s, stack: %s", r, buf)
			}
		}()
		f(ctx)
	}
	return
}

// MustDo save a callback func.
func (c *Fanout) MustDo(ctx core.Ctx, f func(ctx core.Ctx)) (err error) {
	if err = c.Do(ctx, f); err != nil {
		f(ctx)
	}
	return err
}

// Do save a callback func.
func (c *Fanout) Do(ctx core.Ctx, f func(ctx core.Ctx)) (err error) {
	if f == nil || c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	atomic.StoreInt32(&c.taskNum, atomic.LoadInt32(&c.taskNum)+1)
	select {
	case c.ch <- item{f: f, ctx: ctx}:
	default:
		err = ErrFull
	}
	return
}

func (c *Fanout) DoWhen(ctx core.Ctx, f func(ctx core.Ctx), cond bool) (err error) {
	if cond {
		return c.Do(ctx, f)
	}
	return nil
}

// Do save a callback func, wait for timeout if the channel is full.
func (c *Fanout) DoWait(ctx core.Ctx, f func(ctx core.Ctx), timeout time.Duration) (err error) {
	if f == nil || c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	cancelCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case c.ch <- item{f: f, ctx: ctx}:
			return
		case <-cancelCtx.Done():
			return ErrTimeout
		}
	}
}

// DoRetry
// Auto retry call func with back off, if return err
// Notice: no retry if panic
func (c *Fanout) DoRetry(ctx core.Ctx, f func(ctx core.Ctx) error, maxRetry int) (err error) {
	return c.Do(ctx, func(ctx core.Ctx) {
		for i := maxRetry; i > 0; i-- {
			if f(ctx) == nil {
				break
			}
			time.Sleep(time.Duration((maxRetry-i)*(maxRetry-i)+1) * time.Millisecond)
		}
	})
}

// Close close fanout.
func (c *Fanout) Close() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	c.cancel()
	c.waiter.Wait()
	return nil
}

// Wait wait fanout.
func (c *Fanout) Wait() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	atomic.StoreInt32(&c.state, 0)
	c.waiter.Wait()
	c.cancel() // in case of ctx leak
	return nil
}

// Shutdown wait fanout with given timeout.
func (c *Fanout) Shutdown(timeout time.Duration) {
	if len(c.ch) == 0 {
		fmt.Println("[fanout] no task remaining, wait running task to be finished")
		c.cancel()
	}
	closeDone := make(chan struct{})
	pctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func(ctx core.Ctx) {
		defer close(closeDone)
		_ = c.Wait()
	}(pctx)
	for {
		select {
		case <-pctx.Done():
			fmt.Printf("[fanout] close timeout(%s), force exit\n", timeout)
			c.cancel()
			return
		case <-closeDone:
			fmt.Println("[fanout] closed gracefully")
			return
		}
	}
}

package actorx

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/RussellLuo/actorx/internal/channelutil"
)

type config struct {
	mailbox     int
	pending     int
	calls       int
	workers     int
	streamItems int
	chunkBytes  int
	instances   int

	channel           Channel
	contextFactory    ContextFactory
	contextFactorySet bool
}

// Option configures a Runtime before it takes ownership of its channel.
type Option func(*config)

// WithContextFactory installs the Runtime's context extension policy. Spawn
// calls factory once per instance after validating Bindings, before binding the
// channel or entering Started. Local Context.Spawn children use the same factory
// with their own Actor. The last option wins; an explicitly nil final factory
// returns ErrSetup from NewRuntime. Omitting the option keeps local contexts.
func WithContextFactory(factory ContextFactory) Option {
	return func(c *config) {
		c.contextFactory = factory
		c.contextFactorySet = true
	}
}

// WithMailboxCapacity bounds queued messages and reserved task completions per actor.
func WithMailboxCapacity(n int) Option { return func(c *config) { c.mailbox = n } }

// WithPendingLimit bounds unfinished requests on each actor, including Send.
func WithPendingLimit(n int) Option { return func(c *config) { c.pending = n } }

// WithCallLimit bounds caller-side Call/Stream associations across the Runtime.
func WithCallLimit(n int) Option { return func(c *config) { c.calls = n } }

// WithTaskLimit bounds owned tasks per actor until result delivery or discard.
func WithTaskLimit(n int) Option { return func(c *config) { c.workers = n } }

// WithInstanceLimit bounds all instances, including constructing, stopping and
// local Context.Spawn objects, until complete exit. Zero (the default) disables
// this bound. A full Runtime rejects Spawn with ErrFull before reading Bindings.
func WithInstanceLimit(n int) Option { return func(c *config) { c.instances = n } }

// WithStreamLimits bounds queued chunks and each producer-declared payload size.
// A stream retains at most items*chunkBytes bytes plus one in-flight chunk.
func WithStreamLimits(items, chunkBytes int) Option {
	return func(c *config) {
		c.streamItems = items
		c.chunkBytes = chunkBytes
	}
}

// WithChannel supplies a channel that will be exclusively owned by the Runtime.
func WithChannel(ch Channel) Option { return func(c *config) { c.channel = ch } }

// Runtime owns actor execution, including initialization and asynchronous cleanup.
type Runtime struct {
	mu        sync.Mutex
	cfg       config
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	calls     *callState
	actors    map[string]*actor
	spawning  sync.WaitGroup
	closeOnce sync.Once
	done      chan struct{}
	closeErr  error
}

// NewRuntime validates configuration, then opens and exclusively owns a channel.
// ctx bounds initialization only; after success Runtime owns its lifetime.
func NewRuntime(ctx context.Context, options ...Option) (*Runtime, error) {
	c := config{
		mailbox:     64,
		pending:     256,
		calls:       256,
		workers:     16,
		streamItems: 8,
		chunkBytes:  64 << 10,
		channel:     NewLocalChannel(),
	}

	for _, o := range options {
		if o == nil {
			return nil, ErrSetup
		}

		o(&c)
	}

	if c.mailbox < 1 || c.pending < 1 || c.calls < 1 || c.workers < 1 || c.streamItems < 1 || c.chunkBytes < 1 || c.instances < 0 {
		return nil, fmt.Errorf("%w: capacities must be positive and instance limit nonnegative", ErrSetup)
	}
	if channelutil.IsNil(c.channel) {
		return nil, ErrSetup
	}
	if c.contextFactorySet && c.contextFactory == nil {
		return nil, fmt.Errorf("%w: nil context factory", ErrSetup)
	}
	if err := c.channel.Open(ctx); err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		cfg:    c,
		ctx:    runCtx,
		calls:  newCallState(c.channel, c),
		cancel: cancel,
		actors: make(map[string]*actor),
		done:   make(chan struct{}),
	}, nil
}

// Spawn installs Bindings once and waits for Started before opening message admission.
// Success returns the instance address; failure returns a zero address after
// lifecycle cleanup and rollback. The state object becomes exclusively owned by
// the actor: callers must not access its mutable state or spawn it again.
func (rt *Runtime) Spawn(value Actor) (Addr, error) {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return Addr{}, ErrClosed
	}
	if channelutil.IsNil(value) {
		rt.mu.Unlock()
		return Addr{}, ErrSetup
	}
	if rt.cfg.instances > 0 && len(rt.actors) >= rt.cfg.instances {
		rt.mu.Unlock()
		return Addr{}, ErrFull
	}

	rt.spawning.Add(1)

	a := newActor(rt)
	// Track binding construction too, so Close can diagnose and wait for a blocked Spawn.
	rt.actors[a.id] = a
	rt.mu.Unlock()

	defer rt.spawning.Done()

	err := func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = errors.Join(ErrSetup, panicError(v))
			}
		}()

		if err := a.install(value.Bindings()); err != nil {
			return err
		}
		if rt.cfg.contextFactory != nil {
			a.contextWrapper, err = rt.cfg.contextFactory(value)
			if err != nil {
				return errors.Join(ErrSetup, err)
			}
			if a.contextWrapper == nil {
				return fmt.Errorf("%w: context factory returned nil wrapper", ErrSetup)
			}
		}
		return nil
	}()

	// Network readiness must not hold the Runtime lock. Close cancels this
	// wait; any successful binding is owned by a even if publication then loses.
	if err == nil {
		a.setPhase(phaseBinding)
		bindCtx, cancel := context.WithCancel(rt.ctx)
		stop := context.AfterFunc(a.ctx, cancel)
		a.releaseBinding, err = rt.cfg.channel.Bind(bindCtx, a.id, actorReceiver{a})
		stop()
		cancel()
		if err == nil && a.releaseBinding == nil {
			err = ErrSetup
		}
	}

	rt.mu.Lock()
	if rt.closed {
		err = errors.Join(err, ErrClosed)
	}
	if err == nil {
		a.mu.Lock()
		if a.stopped {
			err = ErrStopped
		} else {
			a.entered = true
			a.phase = phaseStarted
		}

		a.mu.Unlock()
	}

	rt.mu.Unlock()

	if err != nil {
		a.record(err)
		a.stop(nil)
	}

	go a.run()
	if err == nil {
		err = <-a.ready
	}
	if err != nil {
		<-a.exit.done
		return Addr{}, a.exit.result.Reason
	}

	return a.addr(), nil
}

// Close establishes the close boundary synchronously. A timeout only stops this
// wait; cleanup continues and subsequent Close calls can wait for it again.
func (rt *Runtime) Close(ctx context.Context) error {
	rt.closeOnce.Do(func() {
		rt.mu.Lock()
		rt.closed = true
		rt.cancel()
		actors := make([]*actor, 0, len(rt.actors))
		for _, a := range rt.actors {
			actors = append(actors, a)
		}

		rt.mu.Unlock()

		rt.calls.stop()
		for _, a := range actors {
			a.stop(nil)
		}

		go func() {
			rt.spawning.Wait()
			for _, a := range actors {
				<-a.exit.done
			}

			rt.closeErr = rt.cfg.channel.Close()
			close(rt.done)
		}()
	})

	select {
	case <-rt.done:
		return rt.closeErr
	case <-ctx.Done():
		return rt.closeTimeout(ctx.Err())
	}
}

// Diagnostic snapshots do not expose mutable state or add an author-facing API.
func (rt *Runtime) closeTimeout(cause error) error {
	rt.mu.Lock()
	actors := make([]*actor, 0, len(rt.actors))
	for _, a := range rt.actors {
		actors = append(actors, a)
	}

	rt.mu.Unlock()

	var waiting []string
	for _, a := range actors {
		a.mu.Lock()
		waiting = append(waiting, fmt.Sprintf("%s phase=%s queued=%d requests=%d work=%d reserved=%d",
			a.id, a.phase, len(a.queue), len(a.requests), len(a.work), a.reserved))
		a.mu.Unlock()
	}

	sort.Strings(waiting)
	return fmt.Errorf("actorx: Close: %w; waiting actors=%d [%s]", cause, len(waiting), strings.Join(waiting, "; "))
}

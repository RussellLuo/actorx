package dist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

type startupProbe struct {
	fail                      error
	entered, unblock, stopped chan struct{}
	starts, stops             *atomic.Int32
}

func (p *startupProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleMessage(p.start), actorx.HandleMessage(p.stop)}
}

func (p *startupProbe) start(actorx.Context, actorx.Started) error {
	if p.starts != nil {
		p.starts.Add(1)
	}
	if p.entered != nil {
		close(p.entered)
		<-p.unblock
	}
	return p.fail
}

func (p *startupProbe) stop(actorx.Context, actorx.Stopped) error {
	if p.stops != nil {
		p.stops.Add(1)
	}
	if p.stopped != nil {
		close(p.stopped)
	}
	return nil
}

func TestConstructionFailuresAndHeldGeneration(t *testing.T) {
	sentinel := errors.New("Started failed")
	var stops atomic.Int32
	cases := map[string]ActorSpec[*startupProbe]{
		"nil":     {Type: "probe", New: func(ActorKey) *startupProbe { return nil }},
		"panic":   {Type: "probe", New: func(ActorKey) *startupProbe { panic("New failed") }},
		"started": {Type: "probe", New: func(ActorKey) *startupProbe { return &startupProbe{fail: sentinel, stops: &stops} }},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			rt := testRuntime(t, WithActor(spec))
			key := ActorKey{"probe", "1"}
			gen := token(t)
			entry, err := rt.createLocal(testContext(t), key, gen)
			if err == nil || entry == nil || !entry.local.IsZero() {
				t.Fatal(entry, err)
			}

			// Failure remains attached to this generation until explicit release.
			again, againErr := rt.createLocal(testContext(t), key, gen)
			if again != entry || againErr != err {
				t.Fatal("lost held failure", againErr)
			}
			if _, err := rt.createLocal(testContext(t), key, token(t)); !errors.Is(err, ErrActivationUnknown) {
				t.Fatal(err)
			}

			if err := rt.releaseLocal(testContext(t), key, gen); err != nil {
				t.Fatal(err)
			}
			if _, err := rt.createLocal(testContext(t), key, token(t)); err == nil {
				t.Fatal("expected new failure")
			}
		})
	}

	if stops.Load() != 2 {
		t.Fatal("Started rollback did not run Stopped", stops.Load())
	}
}

func TestCloseDuringNewAndCallerCancellation(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var news, starts atomic.Int32
	rt := testRuntime(t, WithActor(ActorSpec[*startupProbe]{Type: "probe", New: func(key ActorKey) *startupProbe {
		if key != (ActorKey{"probe", "1"}) {
			panic("wrong ActorKey")
		}
		news.Add(1)
		close(entered)
		<-unblock
		return &startupProbe{starts: &starts}
	}}))

	ctx, cancel := context.WithCancel(context.Background())
	key := ActorKey{"probe", "1"}
	gen := token(t)
	first := make(chan error, 1)
	go func() {
		_, err := rt.createLocal(ctx, key, gen)
		first <- err
	}()
	await(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// A duplicate waiting caller has no ownership over the existing construction.
	second := make(chan error, 1)
	go func() {
		_, err := rt.createLocal(testContext(t), key, gen)
		second <- err
	}()
	deadline, cancelClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelClose()
	if err := rt.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if news.Load() != 1 || rt.creating != 1 {
		t.Fatal("lost in-flight accounting")
	}

	// Management remains responsive after the business engine has closed.
	if _, err := rt.factories["probe"].Call[*creation](testContext(t), createRequest{ActorKey{"probe", "2"}, token(t)}); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("Factory stopped early", err)
	}
	close(unblock)
	<-second

	if err := rt.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 0 || rt.creating != 0 {
		t.Fatal("late object started or token released early", starts.Load(), rt.creating)
	}
}

func TestCloseDuringStartedKeepsManagementAlive(t *testing.T) {
	entered, unblock, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	rt := testRuntime(t, WithActor(ActorSpec[*startupProbe]{Type: "probe", New: func(ActorKey) *startupProbe {
		return &startupProbe{entered: entered, unblock: unblock, stopped: stopped}
	}}))
	result := make(chan error, 1)
	go func() {
		_, err := rt.createLocal(context.Background(), ActorKey{"probe", "1"}, token(t))
		result <- err
	}()
	await(t, entered)

	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := rt.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := rt.factories["probe"].Call[*creation](testContext(t), createRequest{ActorKey{"probe", "2"}, token(t)}); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	close(unblock)
	await(t, stopped)

	if err := <-result; err == nil {
		t.Fatal("startup won shutdown")
	}
	if err := rt.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryFailureClosesAdmissionDuringStartup(t *testing.T) {
	entered, unblock, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	rt := testRuntime(t, WithActor(ActorSpec[*startupProbe]{Type: "probe", New: func(ActorKey) *startupProbe {
		return &startupProbe{entered: entered, unblock: unblock, stopped: stopped}
	}}))
	result := make(chan error, 1)
	go func() {
		_, err := rt.createLocal(context.Background(), ActorKey{"probe", "1"}, token(t))
		result <- err
	}()
	await(t, entered)
	if err := rt.factories["probe"].Stop(errors.New("Factory fault")); err != nil {
		t.Fatal(err)
	}

	// Stopping must close admission even when a worker cannot return yet. Waiting
	// only for Factory.Wait would leave this boundary open indefinitely.
	deadline := time.After(5 * time.Second)
	for {
		if _, err := rt.Addr("probe", "new"); errors.Is(err, actorx.ErrClosed) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Factory fault did not close admission")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(unblock)
	await(t, stopped)
	if err := <-result; err == nil {
		t.Fatal("discarded result reported success")
	}
	if err := rt.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if rt.creating != 0 {
		t.Fatal("discard leaked task token")
	}
	for _, entry := range rt.ledger {
		select {
		case <-entry.done:
		default:
			t.Fatal("unsettled ledger entry")
		}
	}
}

func TestGenerationBudgetAndLocalRelease(t *testing.T) {
	var calls atomic.Int32
	rt := testRuntime(t, WithActor(ActorSpec[*startupProbe]{Type: "probe", New: func(ActorKey) *startupProbe {
		calls.Add(1)
		return nil
	}}))
	var firstKey ActorKey
	var firstGen string
	for i := range maxGenerations {
		key := ActorKey{"probe", fmt.Sprint(i)}
		gen := token(t)
		if i == 0 {
			firstKey, firstGen = key, gen
		}
		if _, err := rt.createLocal(testContext(t), key, gen); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(err)
		}
	}

	if _, err := rt.createLocal(testContext(t), ActorKey{"probe", "overflow"}, token(t)); !errors.Is(err, actorx.ErrFull) || calls.Load() != maxGenerations {
		t.Fatal(err, calls.Load())
	}

	// Releasing one held failure restores exactly one generation slot.
	if err := rt.releaseLocal(testContext(t), firstKey, firstGen); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.createLocal(testContext(t), firstKey, token(t)); !errors.Is(err, actorx.ErrSetup) || calls.Load() != maxGenerations+1 {
		t.Fatal(err, calls.Load())
	}
}

func TestCreationBudgetsKeepManagementIndependent(t *testing.T) {
	entered := make(chan struct{}, maxCreating)
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	release := func() { unblockOnce.Do(func() { close(unblock) }) }
	defer release()
	var options []Option
	for i := range 5 {
		options = append(options, WithActor(ActorSpec[*counter]{Type: fmt.Sprintf("probe%d", i), New: func(ActorKey) *counter {
			entered <- struct{}{}
			<-unblock
			return &counter{}
		}}))
	}
	options = append(options, func(cfg *runtimeOptions) { cfg.localTest = true })
	rt, err := NewRuntime(testContext(t), options...)
	if err != nil {
		t.Fatal(err)
	}

	// Close runs after the deferred unblock, including on test failure.
	defer func() {
		release()
		_ = rt.Close(context.Background())
	}()
	ctx := testContext(t)
	for typ := range 4 {
		for id := range maxFactoryCreating {
			_, err := rt.factories[fmt.Sprintf("probe%d", typ)].Call[*creation](ctx, createRequest{ActorKey{fmt.Sprintf("probe%d", typ), fmt.Sprint(id)}, token(t)})
			if err != nil {
				t.Fatal(err)
			}
			await(t, entered)
		}
	}

	for _, typ := range []string{"probe0", "probe4"} {
		if _, err := rt.factories[typ].Call[*creation](ctx, createRequest{ActorKey{typ, "overflow"}, token(t)}); !errors.Is(err, actorx.ErrFull) {
			t.Fatal(err)
		}
	}
	if rt.creating != maxCreating {
		t.Fatal("global construction bound", rt.creating)
	}

	// No business Actor has started, but management remains able to reject
	// requests. It has its own worker/mailbox/call budgets.
	rt.beginClose()
	release()
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDiscardWaitsForCompleteSuccessfulInstance(t *testing.T) {
	rt := testRuntime(t)
	stopped, unblock := make(chan struct{}), make(chan struct{})
	local, err := rt.business.Spawn(&discardProbe{stopped: stopped, unblock: unblock})
	if err != nil {
		t.Fatal(err)
	}
	entry := &creation{key: ActorKey{"probe", "1"}, generation: token(t), done: make(chan struct{})}
	rt.mu.Lock()
	rt.ledger[entry.generation] = entry
	rt.creating++
	rt.perType["probe"]++
	rt.work.Add(1)
	rt.mu.Unlock()
	done := make(chan struct{})
	go func() {
		rt.settle(entry, local, actorx.ErrStopped, true)
		close(done)
	}()
	await(t, stopped)

	// Stopped has entered but is still blocked; neither result nor budget is free.
	select {
	case <-entry.done:
		t.Fatal("discard settled before Stopped finished")
	default:
	}
	rt.mu.Lock()
	creating := rt.creating
	rt.mu.Unlock()
	if creating != 1 {
		t.Fatal("discard returned token early")
	}

	close(unblock)
	await(t, done)
	if !entry.local.IsZero() || !errors.Is(entry.err, actorx.ErrStopped) {
		t.Fatal(entry.local, entry.err)
	}
	if _, err := local.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

type discardProbe struct{ stopped, unblock chan struct{} }

func (p *discardProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleMessage(p.stop)}
}

func (p *discardProbe) stop(actorx.Context, actorx.Stopped) error {
	close(p.stopped)
	<-p.unblock
	return nil
}

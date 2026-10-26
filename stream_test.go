package actorx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type streamRequest struct {
	N    int
	Fail bool
}

func streamActor(t *testing.T, rt *Runtime, writes *atomic.Int32) *actor {
	return spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleStream(func(ctx Context, msg streamRequest, s StreamReply[int]) error {
				_, err := StartTask(ctx,
					func(ctx context.Context) (struct{}, error) {
						for i := 0; i < msg.N; i++ {
							if err := s.Write(ctx, i, 8); err != nil {
								return struct{}{}, err
							}
							if writes != nil {
								writes.Add(1)
							}
						}

						if msg.Fail {
							return struct{}{}, errors.New("partial failure")
						}

						return struct{}{}, nil
					},
					func(_ struct{}, err error) bool {
						s.Complete(err)
						return true
					},
					nil,
				)
				return err
			}),
			Handle(func(ctx Context, _ get) (int, error) { return 1, nil }),
		}
	})
}

func TestStreamOrderPartialFailureAndModes(t *testing.T) {
	rt := runtimeFor(t, WithStreamLimits(2, 8))
	a := streamActor(t, rt, nil)

	// Both successful and failed producers must drain all accepted chunks in order.
	for _, fail := range []bool{false, true} {
		values := []int{}
		err := a.addr().Stream[int](context.Background(), streamRequest{10, fail}, func(v int) error {
			values = append(values, v)
			return nil
		})
		if (err != nil) != fail {
			t.Fatal(err)
		}
		if len(values) != 10 {
			t.Fatal(values)
		}

		for i, v := range values {
			if i != v {
				t.Fatal(values)
			}
		}
	}

	if _, err := a.addr().Call[any](context.Background(), streamRequest{}); !errors.Is(err, ErrMode) {
		t.Fatal(err)
	}
	if err := a.addr().Send(context.Background(), streamRequest{}); !errors.Is(err, ErrMode) {
		t.Fatal(err)
	}
	if err := a.addr().Stream[int](
		context.Background(), get{}, func(int) error { return nil },
	); !errors.Is(err, ErrMode) {
		t.Fatal(err)
	}
	if err := a.addr().Stream[int](context.Background(), streamRequest{}, nil); !errors.Is(err, ErrSetup) {
		t.Fatal(err)
	}
	if err := a.addr().Stream[string](
		context.Background(), streamRequest{1, false}, func(string) error { return nil },
	); !errors.Is(err, ErrReplyType) {
		t.Fatal(err)
	}
}

func TestStreamBackpressureCallbackErrorAndStop(t *testing.T) {
	rt := runtimeFor(t, WithStreamLimits(1, 8))
	var writes atomic.Int32
	a := streamActor(t, rt, &writes)
	entered := make(chan struct{})
	release := make(chan struct{})
	sentinel := errors.New("consumer failed")
	result := make(chan error, 1)
	go func() {
		result <- a.addr().Stream[int](context.Background(), streamRequest{100, false}, func(int) error {
			close(entered)
			<-release
			return sentinel
		})
	}()
	await(t, entered)

	// One in the callback, one queued; the third write cannot succeed.
	time.Sleep(20 * time.Millisecond)
	if n := writes.Load(); n > 2 {
		t.Fatal("unbounded stream", n)
	}
	if call[int](t, a, get{}) != 1 {
		t.Fatal("slow consumer blocked actor")
	}

	close(release)
	if err := <-result; err != sentinel {
		t.Fatal(err)
	}

	a.addr().Stop(nil)
	waitExitFor(t, a.addr())

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.work) != 0 {
		t.Fatal("leaked producer")
	}
}

func TestStreamCancellationAndChunkLimit(t *testing.T) {
	rt := runtimeFor(t, WithStreamLimits(1, 4))
	a := streamActor(t, rt, nil)

	if err := a.addr().Stream[int](
		context.Background(), streamRequest{1, false}, func(int) error { return nil },
	); !errors.Is(err, ErrChunkSize) {
		t.Fatal(err)
	}

	rt2 := runtimeFor(t, WithStreamLimits(1, 8))
	a2 := streamActor(t, rt2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	err := a2.addr().Stream[int](ctx, streamRequest{1000, false}, func(int) error {
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	a2.addr().Stop(nil)
	waitExitFor(t, a2.addr())
}

func TestStreamStopWithoutTerminal(t *testing.T) {
	rt := runtimeFor(t)
	entered := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleStream(func(ctx Context, _ get, _ StreamReply[int]) error {
			close(entered)
			return nil
		})}
	})

	result := make(chan error, 1)
	go func() {
		result <- a.addr().Stream[int](context.Background(), get{}, func(int) error { return nil })
	}()
	await(t, entered)

	a.addr().Stop(nil)
	if err := <-result; !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	waitExitFor(t, a.addr())
}

func TestStreamWindowHeldUntilCallbackReturns(t *testing.T) {
	rt := runtimeFor(t, WithCallLimit(1))
	a := streamActor(t, rt, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- a.addr().Stream[int](context.Background(), streamRequest{1, false}, func(int) error {
			close(entered)
			<-release
			return nil
		})
	}()
	await(t, entered)

	// Wait for producer completion without going through a second Call slot.
	deadline := time.Now().Add(3 * time.Second)
	for {
		a.mu.Lock()
		n := len(a.requests)
		a.mu.Unlock()

		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("producer did not terminate")
		}

		time.Sleep(time.Millisecond)
	}

	if _, err := a.addr().Call[int](context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal("consumer window released too early", err)
	}

	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if v := call[int](t, a, get{}); v != 1 {
		t.Fatal(v)
	}
}

func TestStreamCancelDuringFinalDrain(t *testing.T) {
	rt := runtimeFor(t)
	terminated := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleStream(func(ctx Context, _ get, s StreamReply[int]) error {
			_, err := StartTask(ctx,
				func(ctx context.Context) (int, error) { return 0, s.Write(ctx, 1, 1) },
				func(_ int, err error) bool {
					s.Complete(err)
					close(terminated)
					return true
				},
				nil,
			)
			return err
		})}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := a.addr().Stream[int](ctx, get{}, func(int) error {
		await(t, terminated)
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancel during terminal drain was lost", err)
	}
}

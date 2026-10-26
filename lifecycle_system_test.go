package actorx

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestLifecycleStartedLeavesOneSlotForTask(t *testing.T) {
	rt := runtimeFor(t, WithMailboxCapacity(1), WithPendingLimit(1), WithCallLimit(1), WithTaskLimit(1))
	delivered := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ Started) error {
				_, err := StartTask(ctx,
					func(context.Context) (int, error) { return 7, nil },
					func(v int, err error) bool {
						if v != 7 || err != nil || ctx.State() != StateRunning {
							t.Error("startup completion", v, err, ctx.State())
						}

						ctx.Stop(nil) // May exit before the Spawn caller resumes.
						close(delivered)
						return true
					},
					nil,
				)
				if err != nil {
					return err
				}

				// Fill the sole mailbox slot before Started returns. The lifecycle
				// notice must neither consume that slot nor run the completion early.
				ctx.actorContext().actor.workers.Wait()
				_, err = StartTask(ctx,
					func(context.Context) (int, error) {
						t.Error("exhausted startup capacity still launched work")
						return 0, nil
					},
					func(int, error) bool { return true },
					nil,
				)
				if !errors.Is(err, ErrFull) {
					t.Error("startup bypassed mailbox/I/O budget", err)
				}

				select {
				case <-delivered:
					t.Error("completion ran before initialization finished")
				default:
				}

				return nil
			}),
		}
	})
	await(t, delivered)
	a.addr().Stop(nil)
	waitExitFor(t, a.addr())
	if err := a.reason(); err != nil {
		t.Fatal(err)
	}
	if exit := waitExitFor(t, a.addr()); exit.Reason != nil {
		t.Fatal(exit)
	}
}

func TestLifecycleNoticesWithFullMailbox(t *testing.T) {
	for _, closeRuntime := range []bool{false, true} {
		t.Run(map[bool]string{false: "Stop", true: "Close"}[closeRuntime], func(t *testing.T) {
			rt := runtimeFor(t, WithMailboxCapacity(1), WithPendingLimit(2), WithCallLimit(1))
			entered, cancelled := make(chan struct{}), make(chan struct{})
			release, stopping := make(chan struct{}), make(chan struct{})
			releaseAtCleanup(t, release)
			var order []string // The actor loop writes; channel barriers publish it.
			a := spawnFor(t, rt, func() []Binding {
				return []Binding{
					HandleMessage(func(Context, Started) error {
						order = append(order, "started")
						return nil
					}),
					HandleMessage(func(ctx Context, _ wait) error {
						order = append(order, "handler")
						close(entered)
						<-ctx.Done()
						close(cancelled)
						<-release
						return nil
					}),
					Handle(func(Context, get) (int, error) {
						order = append(order, "queued")
						return 7, nil
					}),
					HandleMessage(func(Context, Stopping) error {
						order = append(order, "stopping")
						close(stopping)
						return nil
					}),
					HandleMessage(func(Context, Stopped) error {
						order = append(order, "stopped")
						return nil
					}),
				}
			})
			if err := a.addr().Send(context.Background(), wait{}); err != nil {
				t.Fatal(err)
			}

			await(t, entered)
			queued, err := a.addr().prepare(context.Background(), get{}, callMode)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrFull) {
				t.Fatal("mailbox was not full", err)
			}
			if closeRuntime {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := rt.Close(ctx); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				a.addr().Stop(nil)
			}

			await(t, cancelled)
			await(t, queued.done)
			if !errors.Is(queued.err, ErrStopped) && !errors.Is(queued.err, ErrClosed) {
				t.Fatal("queued Call was not settled", queued.err)
			}

			select {
			case <-stopping:
				t.Fatal("lifecycle handler overlapped the active business handler")
			default:
			}

			select {
			case <-a.exit.done:
				t.Fatal("Done preceded the active handler's return")
			default:
			}

			close(release)
			waitExitFor(t, a.addr())
			want := []string{"started", "handler", "stopping", "stopped"}
			if !reflect.DeepEqual(order, want) || a.reason() != nil {
				t.Fatal("shutdown order", order, a.reason())
			}
		})
	}
}

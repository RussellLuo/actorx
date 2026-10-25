package actorx

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBindingsInstallationCopiesSlice(t *testing.T) {
	rt := runtimeFor(t)
	entries := []Binding{Handle(func(*Context, get) (int, error) { return 7, nil })}
	addr, err := rt.Spawn(bindingsFunc(func() []Binding { return entries }))
	if err != nil {
		t.Fatal(err)
	}
	entries[0] = Handle(func(*Context, get) (int, error) { return 99, nil })
	if got, err := addr.Call[int](context.Background(), get{}); err != nil || got != 7 {
		t.Fatal(got, err)
	}
}

// Resource ownership differs before and after entry to Started. A constructor's
// exclusive resource remains the caller's responsibility if binding fails.
func TestResourceOwnershipAcrossStartupFailure(t *testing.T) {
	for _, beforeStart := range []bool{false, true} {
		for _, panicStart := range []bool{false, true} {
			t.Run(map[bool]string{false: "started", true: "binding"}[beforeStart]+map[bool]string{false: "-error", true: "-panic"}[panicStart], func(t *testing.T) {
				file, err := os.CreateTemp(t.TempDir(), "owned-*")
				if err != nil {
					t.Fatal(err)
				}
				var once sync.Once
				var releases, stopped atomic.Int32
				release := func() {
					once.Do(func() {
						releases.Add(1)
						if err := file.Close(); err != nil {
							t.Error(err)
						}
					})
				}
				defer release()
				failure := errors.New("partial initialization")
				var options []Option
				if beforeStart {
					options = append(options, WithChannel(&spawnFailureChannel{err: failure}))
				}
				rt := runtimeFor(t, options...)
				var adopted *os.File
				addr, err := rt.Spawn(bindingsFunc(func() []Binding {
					return []Binding{
						HandleMessage(func(*Context, Started) error {
							adopted = file
							if panicStart {
								panic(failure)
							}
							return failure
						}),
						HandleMessage(func(*Context, Stopping) error {
							if adopted != nil {
								release()
							}
							return nil
						}),
						HandleMessage(func(*Context, Stopped) error { stopped.Add(1); return nil }),
					}
				}))
				if !addr.IsZero() || err == nil {
					t.Fatal(addr, err)
				}
				if beforeStart {
					if stopped.Load() != 0 || releases.Load() != 0 {
						t.Fatal("pre-start lifecycle ran")
					}
					if _, err := file.Stat(); err != nil {
						t.Fatal("caller resource closed prematurely", err)
					}
					release() // caller releases on failure; idempotent with entered-startup cleanup.
				} else if stopped.Load() != 1 {
					t.Fatal("Spawn returned before Stopped")
				}
				release()
				if releases.Load() != 1 {
					t.Fatal(releases.Load())
				}
				if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("file still open", err)
				}
			})
		}
	}
}

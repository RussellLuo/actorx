package actorx

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestResourceCompletionPanicSharesReleaseWithLifecycle(t *testing.T) {
	rt := runtimeFor(t)
	file, err := os.CreateTemp(t.TempDir(), "resource-*")
	if err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	var releases atomic.Int32
	release := func() {
		once.Do(func() {
			releases.Add(1)
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	var adopted *os.File
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(func(*Context, Stopping) error {
			if adopted != nil {
				release()
			}
			return nil
		}), HandleMessage(func(ctx *Context, _ get) error {
			_, err := ctx.WithContext(context.Background()).StartTask(
				func(context.Context) (*os.File, error) {
					return file, nil
				},
				func(value *os.File, _ error) bool {
					adopted = value
					panic("after storing resource, before confirming transfer")
				},
				func(*os.File) { release() },
			)
			return err
		})}
	})

	if err := a.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal(err)
	}

	waitExitFor(t, a.addr())

	var pe *PanicError
	if !errors.As(a.reason(), &pe) || releases.Load() != 1 {
		t.Fatalf("panic=%v, releases=%d", a.reason(), releases.Load())
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("file was not released", err)
	}
}

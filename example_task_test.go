package actorx_test

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/RussellLuo/actorx"
)

// Both discard and Stopped may encounter a resource after a completion panic.
// One idempotent release operation protects the underlying file in either path.
type ownedFile struct {
	file *os.File
	once sync.Once
}

func (f *ownedFile) release() {
	if f != nil {
		f.once.Do(func() {
			f.file.Close()
			os.Remove(f.file.Name())
		})
	}
}

type openFile struct{}

type fileActor struct {
	file    *ownedFile
	opening bool
}

func (f *fileActor) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleDeferred(f.Open), actorx.HandleMessage(f.Stopped)}
}

func (f *fileActor) Stopped(*actorx.Context, actorx.Stopped) error {
	f.file.release()
	return nil
}

func (f *fileActor) Open(ctx *actorx.Context, _ openFile, reply actorx.Reply[string]) error {
	if f.file != nil || f.opening {
		return fmt.Errorf("file already acquired")
	}

	f.opening = true
	_, err := ctx.StartTask(
		func(context.Context) (*ownedFile, error) {
			file, err := os.CreateTemp("", "actorx-resource-*")
			if err != nil {
				return nil, err
			}

			return &ownedFile{file: file}, nil
		},
		func(file *ownedFile, err error) bool {
			f.opening = false
			if err != nil {
				reply.Complete("", err)
				return false
			}

			f.file = file
			reply.Complete(file.file.Name(), nil)
			return true
		},
		func(file *ownedFile) { file.release() },
	)
	if err != nil {
		f.opening = false
	}

	return err
}

func ExampleContext_StartTask() {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		panic(err)
	}

	defer rt.Close(context.Background())

	a, err := rt.Spawn(&fileActor{})
	if err != nil {
		panic(err)
	}

	name, err := a.Call[string](context.Background(), openFile{})
	if err != nil {
		panic(err)
	}

	fmt.Println("resource acquired")
	if err := rt.Close(context.Background()); err != nil {
		panic(err)
	}
	if _, err := os.Stat(name); os.IsNotExist(err) {
		fmt.Println("resource released")
	}
	// Output:
	// resource acquired
	// resource released
}

package actorx_test

import (
	"context"
	"testing"

	"github.com/go-aie/actorx"
)

type resourceMessage struct {
	Values map[string]int
	Notify chan int
	Apply  func(int) int
}

func TestLocalChannelResourceValues(t *testing.T) {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer rt.Close(context.Background())

	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.Handle(func(_ actorx.Context, msg resourceMessage) (chan int, error) {
				msg.Values["n"] = msg.Apply(msg.Values["n"])
				return msg.Notify, nil
			}),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	values, notify := map[string]int{"n": 3}, make(chan int)
	result, err := a.Call[chan int](context.Background(), resourceMessage{values, notify, func(n int) int { return n + 4 }})
	if err != nil || result != notify || values["n"] != 7 {
		t.Fatal("local values were encoded or copied", result, values, err)
	}
}

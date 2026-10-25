package actorx_test

import (
	"fmt"

	"github.com/RussellLuo/actorx"
)

type ReadCount struct{}

type Counter struct{ count int }

// Handwritten bindings capture method values without reading the receiver.
func (c *Counter) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.HandleMessage(c.Started),
		actorx.Handle(c.Read),
	}
}

func (c *Counter) Started(*actorx.Context, actorx.Started) error { c.count = 0; return nil }
func (c *Counter) Read(*actorx.Context, ReadCount) (int, error)  { return c.count, nil }

func ExampleDescribe() {
	// No Counter instance, Runtime, or handler execution is needed for metadata.
	descriptions, err := actorx.Describe((*Counter)(nil).Bindings())
	if err != nil {
		panic(err)
	}
	for _, d := range descriptions {
		fmt.Println(d.MessageType().Name(), d.ResultType(), d.Validate(actorx.CallKind))
	}
	// Output: ReadCount int <nil>
}

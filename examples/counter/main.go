// Counter demonstrates generated bindings for a local actor with multiple messages.
//
//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/go-aie/actorx"
)

type Add struct{ N int }

type Get struct{}

type Reset struct{}

//actorx:actor
type Counter struct{ n int }

func (c *Counter) Add(ctx actorx.Context, msg Add) (int, error) {
	c.n += msg.N
	return c.n, nil
}

func (c *Counter) Get(ctx actorx.Context, _ Get) (int, error) { return c.n, nil }

func (c *Counter) Reset(ctx actorx.Context, _ Reset) error {
	c.n = 0
	return nil
}

func main() {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Close(context.Background())

	addr, err := rt.Spawn(&Counter{})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Send and Call share the mailbox, so Add{2} is applied before Add{3}.
	if err := addr.Send(ctx, Add{2}); err != nil {
		log.Fatal(err)
	}

	n, err := addr.Call[int](ctx, Add{3})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("counter:", n)

	if _, err := addr.Call[any](ctx, Reset{}); err != nil {
		log.Fatal(err)
	}

	n, err = addr.Call[int](ctx, Get{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reset:", n)
}

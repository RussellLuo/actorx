//go:generate go run github.com/RussellLuo/actorx/cmd/actorx-gen
package main

import (
	"github.com/RussellLuo/actorx"
	"github.com/RussellLuo/actorx/dist"
)

type Add struct {
	N int `json:"n"`
}

//actorx:actor
type Counter struct {
	value int
}

func (c *Counter) Add(_ actorx.Context, m Add) (int, error) {
	c.value += m.N
	return c.value, nil
}

var CounterSpec = dist.ActorSpec[*Counter]{
	Type: "click.counter",
	New: func(dist.ActorKey) *Counter {
		return &Counter{}
	},
}

type Click struct{}

//actorx:actor
type Button struct {
	counterID string
}

func (b *Button) Click(ctx actorx.Context, _ Click) (int, error) {
	target, err := dist.MustFrom(ctx).Addr(CounterSpec.Type, b.counterID)
	if err != nil {
		return 0, err
	}

	// Carry the original request's deadline and cancellation into the next hop.
	return target.Call[int](ctx, Add{N: 1})
}

// ButtonSpec pairs each Button with a Counter using the same logical ID.
var ButtonSpec = dist.ActorSpec[*Button]{
	Type: "click.button",
	New: func(key dist.ActorKey) *Button {
		return &Button{counterID: key.ID}
	},
}

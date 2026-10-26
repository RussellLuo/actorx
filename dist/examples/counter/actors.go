//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx/dist"
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
	Type: "counter",
	New: func(dist.ActorKey) *Counter {
		return &Counter{}
	},
}

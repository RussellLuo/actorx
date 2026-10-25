package actorx_test

import "github.com/RussellLuo/actorx"

type bindingsFunc func() []actorx.Binding

func (f bindingsFunc) Bindings() []actorx.Binding { return f() }

var (
	_ actorx.Actor                                             = bindingsFunc(nil)
	_ func(*actorx.Runtime, actorx.Actor) (actorx.Addr, error) = (*actorx.Runtime).Spawn
	_ func(*actorx.Context, actorx.Actor) (actorx.Addr, error) = (*actorx.Context).Spawn
	_ func([]actorx.Binding) ([]actorx.CallDescriptor, error)  = actorx.Describe
)

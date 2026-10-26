package main

import (
	"fmt"

	"github.com/go-aie/actorx"
)

func ExampleCounter_metadata() {
	// Use the same generated Bindings as Spawn, on a nil receiver.
	descriptions, err := actorx.Describe((*Counter)(nil).Bindings())
	if err != nil {
		panic(err)
	}
	for _, d := range descriptions {
		fmt.Println(d.MessageType().Name(), d.ResultType())
	}
	// Output:
	// Add int
	// Get int
	// Reset <nil>
}

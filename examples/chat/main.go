//go:generate go run ../../cmd/actorx-gen
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/RussellLuo/actorx"
)

type Ask struct{ Topic string }

type TextChunk struct{ Text string }

// Assistant streams a simulated answer without a model or network.
//
//actorx:actor
type Assistant struct{}

func (a *Assistant) Answer(ctx *actorx.Context, msg Ask, stream actorx.StreamReply[TextChunk]) error {
	// Write may wait for a slow reader, so produce chunks outside the handler.
	words := strings.Fields(msg.Topic + " use messages to coordinate work.")
	_, err := ctx.StartTask(
		func(ctx context.Context) (struct{}, error) {
			for i, word := range words {
				text := word
				if i > 0 {
					text = " " + text
				}

				if err := stream.Write(ctx, TextChunk{Text: text}, len(text)); err != nil {
					return struct{}{}, err
				}
			}

			return struct{}{}, nil
		},
		func(_ struct{}, err error) bool {
			// Publish the terminal result after the worker has finished writing chunks.
			stream.Complete(err)
			return true
		},
		nil,
	)
	return err
}

func run() error {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())

	addr, err := rt.Spawn(&Assistant{})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fmt.Print("answer: ")
	if err := addr.Stream[TextChunk](ctx, Ask{Topic: "Actors"}, func(chunk TextChunk) error {
		fmt.Print(chunk.Text)
		return nil
	}); err != nil {
		return err
	}
	fmt.Println()
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

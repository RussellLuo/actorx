//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-aie/actorx"
)

type Message struct{ Text string }

type Format struct{ Text string }

//actorx:actor
type Worker struct{}

func (w *Worker) Format(_ actorx.Context, msg Format) (string, error) {
	return strings.ToUpper(msg.Text), nil
}

// Session owns a dedicated worker address, rather than its mutable state.
//
//actorx:actor
type Session struct{ worker actorx.Addr }

func (s *Session) Started(ctx actorx.Context, _ actorx.Started) error {
	worker, err := ctx.Spawn(&Worker{})
	if err != nil {
		return err // Worker rollback is complete; Session chooses to fail too.
	}

	s.worker = worker
	return nil
}

func (s *Session) Receive(ctx actorx.Context, msg Message) (string, error) {
	// This dependency points one way; Worker never calls back into Session.
	return s.worker.Call[string](ctx, Format{Text: msg.Text})
}

func (s *Session) Stopping(_ actorx.Context, _ actorx.Stopping) error {
	if s.worker.IsZero() {
		return nil // Worker startup failed before its address was assigned.
	}
	if err := s.worker.Stop(nil); err != nil && !errors.Is(err, actorx.ErrClosed) {
		return err
	}

	// Stopping's context is canceled. Use a separate, bounded wait for this
	// independent actor, never for Session itself.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	exit, err := s.worker.Wait(ctx)
	if err != nil {
		return err
	}
	if exit.Reason != nil {
		return exit.Reason
	}
	fmt.Println("worker stopped")
	return nil
}

func run() error {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())

	addr, err := rt.Spawn(&Session{})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	text, err := addr.Call[string](ctx, Message{Text: "hello"})
	if err != nil {
		return err
	}
	fmt.Println("worker:", text)

	if err := addr.Stop(nil); err != nil {
		return err
	}

	exit, err := addr.Wait(ctx)
	if err != nil {
		return err
	}
	if exit.Reason != nil {
		return exit.Reason
	}
	fmt.Println("session stopped")
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

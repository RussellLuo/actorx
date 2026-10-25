//go:generate go run ../../cmd/actorx-gen
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/RussellLuo/actorx"
)

type Message struct{ Text string }

type Logout struct{}

//actorx:actor
type Session struct {
	messages int
}

// Local lifecycle notices use ordinary handlers; the notice type selects the phase.
func (s *Session) Started(ctx *actorx.Context, _ actorx.Started) error {
	s.messages = 0
	fmt.Printf("started: state=%s\n", stateName(ctx.State()))
	return nil
}

func (s *Session) Receive(ctx *actorx.Context, msg Message) error {
	s.messages++
	fmt.Printf("message: %s, state=%s\n", msg.Text, stateName(ctx.State()))
	return nil
}

// Logout is an ordinary business message; the handler explicitly requests self-stop.
func (s *Session) Logout(ctx *actorx.Context, _ Logout) error {
	ctx.Stop(nil)
	return nil
}

func (s *Session) Stopping(ctx *actorx.Context, msg actorx.Stopping) error {
	// Unblock owned work here if needed, before the runtime waits for workers.
	fmt.Printf("stopping: state=%s, reason=%v\n", stateName(ctx.State()), msg.Reason)
	return nil
}

func (s *Session) Stopped(ctx *actorx.Context, msg actorx.Stopped) error {
	// Owned work and resource cleanup have finished; Wait completes after return.
	fmt.Printf("stopped: state=%s, messages=%d, reason=%v\n", stateName(ctx.State()), s.messages, msg.Reason)
	return nil
}

func stateName(state actorx.State) string {
	switch state {
	case actorx.StateStarted:
		return "Started"
	case actorx.StateRunning:
		return "Running"
	case actorx.StateStopping:
		return "Stopping"
	case actorx.StateStopped:
		return "Stopped"
	default:
		return "unknown"
	}
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

	// Spawn returns only after Started succeeds and admission opens.
	fmt.Println("spawn returned")

	// Call waits for Receive to finish before we request Stop.
	if _, err := addr.Call[any](context.Background(), Message{"hello"}); err != nil {
		return err
	}

	if err := addr.Stop(nil); err != nil {
		return err
	}

	// Wait confirms complete exit; its error describes waiting, not the exit reason.
	exit, err := addr.Wait(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("done: err=%v\n", exit.Reason)
	return exit.Reason
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

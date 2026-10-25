package actorx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

func TestOldRevisionCannotOverwriteNewOperation(t *testing.T) {
	g := &revisionFixture{}
	old, err := g.begin()
	if err != nil {
		t.Fatal(err)
	}

	if err := g.cancelRevision(nil, cancelRevision{}); err != nil {
		t.Fatal(err)
	}

	current, err := g.begin()
	if err != nil {
		t.Fatal(err)
	}

	// An old completion must leave the new operation busy and uncounted.
	g.finish(old, nil)
	if !g.busy || g.revision != current || g.completed != 0 {
		t.Fatal("stale completion overwrote new state")
	}

	g.finish(current, nil)
	if g.busy || g.completed != 1 {
		t.Fatal("current completion was lost")
	}
}

func TestLateTaskCompletionCannotOverwriteCurrentOperation(t *testing.T) {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer rt.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	oldReady, newReady := make(chan struct{}), make(chan struct{})
	oldRelease, newRelease := make(chan struct{}, 1), make(chan struct{}, 1)

	// Release even after a failed assertion, so owned workers cannot trap Close.
	defer close(newRelease)
	defer close(oldRelease)

	oldSettled, newSettled := make(chan struct{}), make(chan struct{})
	g := &revisionFixture{}
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.HandleMessage(g.cancelRevision),
			actorx.Handle(g.getRevisionStatus),
			actorx.HandleDeferred(func(ctx *actorx.Context, msg revisionRequest, reply actorx.Reply[revisionResult]) error {
				revision, err := g.begin()
				if err != nil {
					return err
				}

				ready, release, settled := oldReady, oldRelease, oldSettled
				if msg.Prompt == "new" {
					ready, release, settled = newReady, newRelease, newSettled
				}

				g.cancel, err = ctx.StartTask(
					func(context.Context) (revisionResult, error) {
						close(ready)
						<-release // Deliberately model an I/O operation that ignores cancel.
						return revisionResult{Text: msg.Prompt}, nil
					},
					func(result revisionResult, err error) bool {
						g.finish(revision, err)
						reply.Complete(result, err)
						close(settled)
						return true
					},
					nil,
				)
				return err
			}),
		}
	}))

	if err != nil {
		t.Fatal(err)
	}

	await := func(ch <-chan struct{}) {
		t.Helper()

		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	addr := a
	if err := addr.Send(ctx, revisionRequest{Prompt: "old"}); err != nil {
		t.Fatal(err)
	}

	await(oldReady)

	if _, err := addr.Call[any](ctx, cancelRevision{}); err != nil {
		t.Fatal(err)
	}
	if err := addr.Send(ctx, revisionRequest{Prompt: "new"}); err != nil {
		t.Fatal(err)
	}

	await(newReady)

	// Send one token rather than close; deferred close remains safe on failure.
	oldRelease <- struct{}{}
	await(oldSettled)
	status, err := addr.Call[revisionStatus](ctx, getRevisionStatus{})
	if err != nil || !status.Busy || status.Completed != 0 || status.Revision != 3 {
		t.Fatalf("late result changed current operation: %+v, %v", status, err)
	}

	newRelease <- struct{}{}
	await(newSettled)
	status, err = addr.Call[revisionStatus](ctx, getRevisionStatus{})
	if err != nil || status.Busy || status.Completed != 1 {
		t.Fatalf("current result was lost: %+v, %v", status, err)
	}
}

// Revision handling is an application policy. Keep its late-completion
// regression here rather than adding task replacement to introductory examples.
type revisionRequest struct{ Prompt string }

type revisionResult struct{ Text string }

type cancelRevision struct{}

type getRevisionStatus struct{}

type revisionStatus struct {
	Busy                bool
	Revision, Completed int
}

type revisionFixture struct {
	busy                bool
	revision, completed int
	cancel              context.CancelFunc
}

func (g *revisionFixture) begin() (int, error) {
	if g.busy {
		return 0, errors.New("operation already active")
	}

	g.busy = true
	g.revision++
	return g.revision, nil
}

func (g *revisionFixture) finish(revision int, err error) {
	if g.revision != revision {
		return
	}

	g.busy = false
	g.cancel = nil
	if err == nil {
		g.completed++
	}
}

func (g *revisionFixture) cancelRevision(_ *actorx.Context, _ cancelRevision) error {
	if g.cancel != nil {
		g.cancel()
		g.cancel = nil
	}

	g.busy = false
	g.revision++
	return nil
}

func (g *revisionFixture) getRevisionStatus(_ *actorx.Context, _ getRevisionStatus) (revisionStatus, error) {
	return revisionStatus{g.busy, g.revision, g.completed}, nil
}

//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-aie/actorx"
)

type LookupProfile struct {
	UserID string
}

type Profile struct {
	Name  string
	Email string
}

type GetStatus struct{}

type Status struct {
	Loading bool
}

// Directory keeps state on the actor and runs the lookup outside its handler.
//
//actorx:actor
type Directory struct {
	loading bool
}

func (d *Directory) Lookup(ctx actorx.Context, msg LookupProfile, reply actorx.Reply[Profile]) error {
	if d.loading {
		return errors.New("profile lookup already in progress")
	}

	d.loading = true
	_, err := actorx.StartTask(ctx,
		func(ctx context.Context) (Profile, error) {
			return mockLookup(ctx, msg.UserID)
		},
		func(profile Profile, err error) bool {
			// Completion runs serially on the actor, so updating its state is safe.
			d.loading = false
			reply.Complete(profile, err)
			return true
		},
		nil,
	)
	if err != nil {
		// Admission failed before a worker started; no completion will reset loading.
		d.loading = false
	}

	return err
}

func (d *Directory) GetStatus(_ actorx.Context, _ GetStatus) (Status, error) {
	return Status{Loading: d.loading}, nil
}

// mockLookup stands in for a remote profile service; no network is used.
func mockLookup(ctx context.Context, userID string) (Profile, error) {
	timer := time.NewTimer(60 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return Profile{}, ctx.Err()
	case <-timer.C:
		return Profile{Name: userID, Email: userID + "@example.com"}, nil
	}
}

func run() error {
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())

	addr, err := rt.Spawn(&Directory{})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	profile, err := addr.Call[Profile](ctx, LookupProfile{UserID: "alice"})
	if err != nil {
		return err
	}
	fmt.Printf("profile: %s (%s)\n", profile.Name, profile.Email)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

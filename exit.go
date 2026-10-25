package actorx

import (
	"context"
	"fmt"
)

// Exit is the final result after handlers, cleanup, workers and rollback finish.
// Reason is nil for a normal exit. Shared error diagnostic data must not be mutated.
type Exit struct {
	Reason error
}

// exitState is shared by the actor and its original local addresses. It holds no
// actor or Runtime reference. result is immutable and readable only after done.
type exitState struct {
	done   chan struct{}
	result Exit
}

// Wait passively waits for this local instance's complete exit. The returned
// error describes waiting failure; Exit.Reason describes the actor's exit.
// Copies of an original local Addr share the result, even after Runtime.Close.
// Imported addresses have no local exit record. Never wait for yourself in a handler.
func (addr Addr) Wait(ctx context.Context) (Exit, error) {
	if addr.channel == nil || !ValidInstanceID(addr.id) {
		return Exit{}, ErrSetup
	}
	if addr.exit == nil {
		return Exit{}, fmt.Errorf("actorx: address has no local lifecycle capability")
	}

	// An already published result takes precedence over a canceled context.
	select {
	case <-addr.exit.done:
		return addr.exit.result, nil
	default:
	}

	select {
	case <-addr.exit.done:
		return addr.exit.result, nil
	case <-ctx.Done():
		// Completion may have raced cancellation. Check once more before failing.
		select {
		case <-addr.exit.done:
			return addr.exit.result, nil
		default:
			return Exit{}, ctx.Err()
		}
	}
}

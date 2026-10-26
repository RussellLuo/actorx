package actorx_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/RussellLuo/actorx"
)

func checkLimitExample(err error) {
	if err != nil {
		panic(err)
	}
}

func awaitLimitExample(ctx context.Context, signal <-chan struct{}) {
	select {
	case <-signal:
	case <-ctx.Done():
		panic(ctx.Err())
	}
}

func Example_mailbox() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithMailboxCapacity(1))
	checkLimitExample(err)
	defer func() { checkLimitExample(rt.Close(context.Background())) }()

	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release) // Also unblock the handler if the example fails early.

	type Job struct{ ID int }
	addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.HandleMessage(func(ctx actorx.Context, msg Job) error {
			if msg.ID == 1 {
				close(entered)
				// A controlled blocker makes mailbox occupancy observable. Real
				// blocking work belongs in StartTask, as shown by profile.
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})}
	}))
	checkLimitExample(err)
	checkLimitExample(addr.Send(ctx, Job{ID: 1}))
	awaitLimitExample(ctx, entered) // Job 1 is executing, so the single queue slot is free.
	checkLimitExample(addr.Send(ctx, Job{ID: 2}))
	err = addr.Send(ctx, Job{ID: 3})
	fmt.Println("mailbox full:", errors.Is(err, actorx.ErrFull))
	// Output:
	// mailbox full: true
}

func Example_stream() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithStreamLimits(1, 1))
	checkLimitExample(err)
	defer func() { checkLimitExample(rt.Close(context.Background())) }()

	thirdWrite, thirdWritten := make(chan struct{}), make(chan struct{})
	type Count struct{}
	addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.HandleStream(func(ctx actorx.Context, _ Count, stream actorx.StreamReply[string]) error {
			_, err := actorx.StartTask(ctx,
				func(ctx context.Context) (struct{}, error) {
					for _, text := range []string{"1", "2", "3"} {
						if text == "3" {
							close(thirdWrite)
						}
						if err := stream.Write(ctx, text, len(text)); err != nil {
							return struct{}{}, err
						}
					}

					close(thirdWritten)
					return struct{}{}, nil
				},
				func(_ struct{}, err error) bool {
					stream.Complete(err)
					return true
				}, nil)
			return err
		})}
	}))
	checkLimitExample(err)
	checkLimitExample(addr.Stream[string](ctx, Count{}, func(text string) error {
		if text == "1" {
			awaitLimitExample(ctx, thirdWrite) // Item 1 is in this callback; item 2 fills the window.
			select {
			case <-thirdWritten:
				return errors.New("third write exceeded the stream window")
			default:
				fmt.Println("window full: third write cannot finish")
			}
		}

		fmt.Println("consume:", text)
		return nil // Returning lets consumption continue and frees window capacity.
	}))
	// Output:
	// window full: third write cannot finish
	// consume: 1
	// consume: 2
	// consume: 3
}

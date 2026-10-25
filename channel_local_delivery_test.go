package actorx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

type localDeliveryReceiver struct {
	received chan actorx.Delivery
	bindings []actorx.CallDescriptor
	reads    int
}

func (*localDeliveryReceiver) Addresses() actorx.AddressImporter { return nil }

func (r *localDeliveryReceiver) Bindings() []actorx.CallDescriptor {
	r.reads++
	return r.bindings
}

func (r *localDeliveryReceiver) Receive(_ context.Context, d actorx.Delivery, reply actorx.Responder) error {
	r.received <- d
	reply.Complete(17, nil)
	return nil
}

type localDeliveryResult struct {
	value any
	err   error
}

type localDeliveryReply struct {
	once sync.Once
	done chan localDeliveryResult
}

func (*localDeliveryReply) Addresses() actorx.AddressImporter { return nil }

func (r *localDeliveryReply) Complete(v any, err error) bool {
	won := false
	r.once.Do(func() {
		won = true
		r.done <- localDeliveryResult{v, err}
	})
	return won
}

func (*localDeliveryReply) Write(context.Context, any, int) error { return actorx.ErrMode }

func (*localDeliveryReply) WriteConsumed(context.Context, any, int) error { return actorx.ErrMode }

func TestLocalChannelDeliveryBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch := actorx.NewLocalChannel()
	defer ch.Close()

	if err := ch.Open(ctx); err != nil {
		t.Fatal(err)
	}

	const id = "a_01234567890123456789012345678901"
	localReceiver := &localDeliveryReceiver{
		received: make(chan actorx.Delivery, 1),
		bindings: []actorx.CallDescriptor{
			actorx.Request[localQuery, int](),
			actorx.Streaming[localChunks, int](),
		},
	}
	var nilReceiver *localDeliveryReceiver
	for _, invalid := range []struct {
		id            string
		localReceiver actorx.Receiver
	}{
		{"", localReceiver}, {"ca.*", localReceiver}, {id, nil}, {id, nilReceiver},
	} {
		if release, err := ch.Bind(ctx, invalid.id, invalid.localReceiver); !errors.Is(err, actorx.ErrSetup) || release != nil {
			t.Fatal("invalid binding", err)
		}
	}

	release, err := ch.Bind(ctx, id, localReceiver)
	if err != nil {
		t.Fatal(err)
	}

	defer release()
	// A channel owns its frozen descriptions even if a provider retains and
	// changes the returned slice. Delivery must not ask for metadata again.
	localReceiver.bindings[0] = actorx.Streaming[localQuery, int]()

	d := actorx.Delivery{
		Target:      id,
		Correlation: "public-delivery",
		Kind:        actorx.CallKind,
		Message:     localQuery{},
	}
	reply := &localDeliveryReply{done: make(chan localDeliveryResult, 1)}
	for _, mutate := range []func(*actorx.Delivery){
		func(d *actorx.Delivery) { d.Target = "" },
		func(d *actorx.Delivery) { d.Target = "a_0123456789012345678901234567890*" },
		func(d *actorx.Delivery) { d.Correlation = "" },
		func(d *actorx.Delivery) { d.Kind = 255 },
	} {
		invalid := d
		mutate(&invalid)
		if err := ch.Deliver(ctx, invalid, reply); !errors.Is(err, actorx.ErrMode) {
			t.Fatal("invalid delivery", err)
		}
	}

	var nilReply *localDeliveryReply
	for _, invalid := range []actorx.Responder{nil, nilReply} {
		if err := ch.Deliver(ctx, d, invalid); !errors.Is(err, actorx.ErrMode) {
			t.Fatal("nil reply", err)
		}
	}
	for _, invalid := range []struct {
		message any
		kind    actorx.Kind
		want    error
	}{
		{localQuery{}, actorx.StreamKind, actorx.ErrMode},
		{localChunks{}, actorx.SendKind, actorx.ErrMode},
		{localChunks{}, actorx.CallKind, actorx.ErrMode},
		{localUnknown{}, actorx.SendKind, actorx.ErrUnhandled},
		{&localQuery{}, actorx.CallKind, actorx.ErrUnhandled},
		{nil, actorx.CallKind, actorx.ErrUnhandled},
		{actorx.Started{}, actorx.SendKind, actorx.ErrUnhandled},
		{actorx.Stopping{}, actorx.CallKind, actorx.ErrUnhandled},
		{(*actorx.Stopped)(nil), actorx.StreamKind, actorx.ErrUnhandled},
	} {
		bad := d
		bad.Message, bad.Kind = invalid.message, invalid.kind
		if err := ch.Deliver(ctx, bad, reply); !errors.Is(err, invalid.want) {
			t.Fatalf("%T, kind %v: got %v, want %v", bad.Message, bad.Kind, err, invalid.want)
		}
	}

	select {
	case <-localReceiver.received:
		t.Fatal("invalid delivery reached localReceiver")
	default:
	}

	if err := ch.Deliver(ctx, d, reply); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-localReceiver.received:
		if got != d {
			t.Fatal("logical delivery changed", got, d)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case got := <-reply.done:
		if got.value != 17 || got.err != nil {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if localReceiver.reads != 1 {
		t.Fatal("binding descriptions reread during delivery", localReceiver.reads)
	}
}

func TestLocalChannelBindingDescriptions(t *testing.T) {
	query := actorx.Request[localQuery, int]()
	for _, tc := range []struct {
		name     string
		bindings []actorx.CallDescriptor
	}{
		{"zero", []actorx.CallDescriptor{{}}},
		{"lifecycle", []actorx.CallDescriptor{actorx.Message[actorx.Started]()}},
		{"lifecycle pointer", []actorx.CallDescriptor{actorx.Message[*actorx.Stopped]()}},
		{"lifecycle result", []actorx.CallDescriptor{actorx.Request[localQuery, actorx.Stopping]()}},
		{"interface", []actorx.CallDescriptor{actorx.Message[any]()}},
		{"duplicate", []actorx.CallDescriptor{query, query}},
		{"conflicting mode", []actorx.CallDescriptor{query, actorx.Streaming[localQuery, int]()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ch := actorx.NewLocalChannel()
			defer ch.Close()
			if err := ch.Open(ctx); err != nil {
				t.Fatal(err)
			}

			const id = "a_01234567890123456789012345678901"
			r := &localDeliveryReceiver{bindings: tc.bindings}
			if release, err := ch.Bind(ctx, id, r); !errors.Is(err, actorx.ErrSetup) || release != nil {
				t.Fatal("invalid description accepted", err)
			}

			// Failed validation must leave the instance available for a valid bind.
			r.bindings = []actorx.CallDescriptor{query}
			release, err := ch.Bind(ctx, id, r)
			if err != nil {
				t.Fatal("failed bind retained instance", err)
			}
			defer release()
		})
	}
}

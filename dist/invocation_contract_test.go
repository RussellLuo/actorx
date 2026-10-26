package dist

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/RussellLuo/actorx"
)

// Number deliberately matches encoding/json.Number in name and underlying type.
type Number string

func TestCallerIdentityIncludesPackageAndResultName(t *testing.T) {
	c, err := buildContract([]actorx.CallDescriptor{actorx.Request[json.Number, json.Number]()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, Number("1"), actorx.CallKind, reflect.TypeFor[json.Number]()); !errors.Is(err, actorx.ErrUnhandled) {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, json.Number("1"), actorx.CallKind, reflect.TypeFor[Number]()); !errors.Is(err, actorx.ErrReplyType) {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, json.Number("1"), actorx.CallKind, reflect.TypeFor[json.Number]()); err != nil {
		t.Fatal(err)
	}
}

func TestInvocationContractRejections(t *testing.T) {
	c, err := buildContract([]actorx.CallDescriptor{
		actorx.Request[add, int](), actorx.Streaming[series, string](), actorx.Message[read](),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		message any
		kind    actorx.Kind
		result  reflect.Type
		want    error
	}{
		{"call", add{}, actorx.CallKind, reflect.TypeFor[int](), nil},
		{"send", add{}, actorx.SendKind, nil, nil},
		{"wrong result", add{}, actorx.CallKind, reflect.TypeFor[string](), actorx.ErrReplyType},
		{"wrong mode", series{}, actorx.SendKind, nil, actorx.ErrMode},
		{"unknown message", "wrong", actorx.CallKind, reflect.TypeFor[int](), actorx.ErrUnhandled},
		{"nil message", nil, actorx.CallKind, reflect.TypeFor[int](), actorx.ErrUnhandled},
		{"message-only result", read{}, actorx.CallKind, reflect.TypeFor[int](), actorx.ErrReplyType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := checkInvocation(c, tc.message, tc.kind, tc.result); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

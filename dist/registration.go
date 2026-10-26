package dist

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/RussellLuo/actorx"
	"github.com/RussellLuo/actorx/dist/internal/identity"
)

type registration struct {
	name         string
	contract     wireContract
	descriptions []actorx.CallDescriptor
	messages     map[string]actorx.CallDescriptor
	construct    func(ActorKey) (actorx.Actor, error)
}

type bindingSnapshot []actorx.Binding

func (b bindingSnapshot) Bindings() []actorx.Binding { return b }

// WithActor snapshots a declaration. Metadata is read during NewRuntime on T's
// nil receiver, without invoking New, Started or an ordinary handler.
func WithActor[T actorx.Actor](spec ActorSpec[T]) Option {
	return func(cfg *runtimeOptions) {
		cfg.specs = append(cfg.specs, func() (out registration, err error) {
			defer func() {
				if v := recover(); v != nil {
					out = registration{}
					err = fmt.Errorf("%w: metadata panic: %v", actorx.ErrSetup, v)
				}
			}()

			t := reflect.TypeFor[T]()
			if spec.New == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
				return out, actorx.ErrSetup
			}
			if err = identity.Validate(spec.Type, "metadata"); err != nil {
				return out, err
			}

			var zero T
			descriptions, err := actorx.Describe(zero.Bindings())
			if err != nil {
				return out, err
			}
			if len(descriptions) > 64 {
				return out, actorx.ErrSetup
			}
			contract, err := buildContract(descriptions)
			if err != nil {
				return out, err
			}
			out = registration{
				name:         spec.Type,
				descriptions: descriptions,
				contract:     contract,
				messages:     map[string]actorx.CallDescriptor{},
			}
			for _, d := range descriptions {
				id, e := typeIdentity(d.MessageType())
				if e != nil {
					return registration{}, e
				}
				out.messages[id] = d
			}

			// The live binding snapshot must match metadata before core Spawn uses it.
			out.construct = func(key ActorKey) (value actorx.Actor, err error) {
				defer func() {
					if v := recover(); v != nil {
						value = nil
						err = fmt.Errorf("%w: New/Bindings panic: %v", actorx.ErrSetup, v)
					}
				}()

				live := spec.New(key)
				if reflect.ValueOf(live).IsNil() {
					return nil, actorx.ErrSetup
				}

				bindings := slices.Clone(live.Bindings())
				actual, err := actorx.Describe(bindings)
				if err != nil {
					return nil, err
				}

				// Bindings order has no contract meaning. Compare the entire set of exact
				// local types and modes; the static schema was generated at registration.
				if !sameDescriptions(actual, descriptions) {
					return nil, ErrContract
				}
				return bindingSnapshot(bindings), nil
			}
			return out, nil
		})
	}
}

func sameDescriptions(a, b []actorx.CallDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for _, d := range a {
		if !slices.Contains(b, d) {
			return false
		}
	}
	return true
}

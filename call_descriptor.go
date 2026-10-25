package actorx

import "reflect"

// CallDescriptor is a read-only description shared by bindings and callers.
// A single-result description permits Send and Call; a stream permits only Stream.
// Network operation names and encoding belong to channel assembly.
// Its zero value is invalid. Local values are neither encoded nor copied.
type CallDescriptor struct {
	message, result reflect.Type
	stream          bool
	valid           bool
}

// Request declares a single-result invocation for a caller without a Binding.
// Assemblies with a Binding can reuse Binding.Descriptor instead.
func Request[Q, R any]() CallDescriptor {
	return newDescriptor(reflect.TypeFor[Q](), reflect.TypeFor[R](), false)
}

// Message declares an error-only invocation, supporting both Send and Call.
func Message[M any]() CallDescriptor { return newDescriptor(reflect.TypeFor[M](), nil, false) }

// Streaming declares a streaming invocation and its chunk type.
func Streaming[Q, C any]() CallDescriptor {
	return newDescriptor(reflect.TypeFor[Q](), reflect.TypeFor[C](), true)
}

func newDescriptor(message, result reflect.Type, stream bool) CallDescriptor {
	valid := message.Kind() != reflect.Interface &&
		lifecycleType(message) == noLifecyclePhase &&
		lifecycleType(result) == noLifecyclePhase
	return CallDescriptor{
		message: message,
		result:  result,
		stream:  stream,
		valid:   valid,
	}
}

// MessageType returns the concrete message type used for binding and admission.
func (d CallDescriptor) MessageType() reflect.Type { return d.message }

// ResultType is nil for an error-only invocation, otherwise the result or chunk type.
func (d CallDescriptor) ResultType() reflect.Type { return d.result }

// Validate checks that the description is valid for the selected invocation mode.
// Lifecycle events are runtime-only and cannot describe ordinary invocations.
func (d CallDescriptor) Validate(kind Kind) error {
	if !d.valid {
		return ErrSetup
	}
	if kind > StreamKind || (kind == StreamKind) != d.stream {
		return ErrMode
	}

	return nil
}

// Check applies the shared message type and mode check without encoding a value.
func (d CallDescriptor) Check(msg any, kind Kind) error {
	if err := d.Validate(kind); err != nil {
		return err
	}
	if reflect.TypeOf(msg) != d.message {
		return ErrUnhandled
	}

	return nil
}

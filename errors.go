// Package actorx provides a bounded, serial, in-process actor runtime.
package actorx

import (
	"errors"
	"fmt"
	"runtime/debug"
)

var (
	ErrClosed    = errors.New("actorx: runtime or channel closed")
	ErrStopped   = errors.New("actorx: stopped")
	ErrFull      = errors.New("actorx: capacity exhausted")
	ErrUnhandled = errors.New("actorx: no binding for message")
	ErrReplyType = errors.New("actorx: incompatible reply type")
	ErrMode      = errors.New("actorx: unsupported communication mode")
	ErrSetup     = errors.New("actorx: invalid setup or binding")
	ErrPhase     = errors.New("actorx: operation outside allowed lifecycle phase")
	ErrChunkSize = errors.New("actorx: invalid or excessive chunk size")
)

// PanicError preserves the panic value and stack for diagnostics.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("actorx: panic: %v", e.Value) }

func panicError(v any) error { return &PanicError{Value: v, Stack: debug.Stack()} }

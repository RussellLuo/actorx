// Package channelutil contains private nil checks used by the core.
// It owns no routing, lifecycle, invocation or transport state.
package channelutil

import "reflect"

// IsNil includes typed nil capabilities stored in an interface.
func IsNil(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		return rv.IsNil()
	}

	return false
}

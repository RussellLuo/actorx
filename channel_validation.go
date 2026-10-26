package actorx

import "github.com/go-aie/actorx/internal/channelutil"

// ValidInstanceID reports whether id is an Actorx instance identity: a_ followed
// by 32 lowercase hexadecimal digits. It does not check existence or reachability.
func ValidInstanceID(id string) bool {
	if len(id) != 34 || id[:2] != "a_" {
		return false
	}

	for _, c := range id[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}

	return true
}

// ValidateBinding checks the common instance identity and receiver capability.
// Channels apply their own readiness, duplicate-binding and transport checks.
func ValidateBinding(id string, receiver Receiver) error {
	if !ValidInstanceID(id) || channelutil.IsNil(receiver) {
		return ErrSetup
	}

	return nil
}

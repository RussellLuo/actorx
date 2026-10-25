package actorx

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func newID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}

	return "a_" + hex.EncodeToString(id[:])
}

type targetDescription struct {
	ID string `json:"id"`
}

// ExportAddr extracts only the instance ID. It neither probes nor persists a
// connection; the recipient supplies its own already configured channel.
// Local lifecycle capabilities are never exported.
func ExportAddr(addr Addr) ([]byte, error) {
	if !ValidInstanceID(addr.id) || addr.channel == nil {
		return nil, fmt.Errorf("%w: invalid address", ErrSetup)
	}

	return json.Marshal(targetDescription{addr.id})
}

// AddressImporter attaches decoded instance identities to the owning Runtime.
// Receiver and Responder expose this capability so codecs need no private state.
type AddressImporter interface {
	ImportAddr([]byte) (Addr, error)
}

// ImportAddr binds an instance identity to this Runtime's caller state. It does
// not bind, connect, activate, probe, or establish target reachability.
// It never restores local lifecycle capabilities, even in the source Runtime.
func (rt *Runtime) ImportAddr(data []byte) (Addr, error) {
	return rt.calls.ImportAddr(data)
}

func (ch *callState) ImportAddr(data []byte) (Addr, error) {
	var target targetDescription
	if err := json.Unmarshal(data, &target); err != nil {
		return Addr{}, fmt.Errorf("%w: address: %v", ErrSetup, err)
	}
	if !ValidInstanceID(target.ID) {
		return Addr{}, fmt.Errorf("%w: invalid instance ID", ErrSetup)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return Addr{}, ErrClosed
	}

	return Addr{id: target.ID, channel: ch}, nil
}

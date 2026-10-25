package actorx

// Describe validates all bindings using the same rules as Spawn, then returns
// an independent slice of ordinary message descriptions in binding order.
// Lifecycle entries are validated but omitted. Empty or lifecycle-only bindings
// return an empty slice. Invalid bindings return no descriptions and an error
// matching ErrSetup.
//
// Descriptions retain only types and modes, never handlers or their receivers.
// Describe neither modifies the bindings nor invokes handlers. To read metadata
// without constructing an actor, pass bindings built on a concrete nil pointer,
// for example Describe((*Counter)(nil).Bindings()). Bindings must obey Actor's
// construction-only contract; Spawn still requires a non-nil instance.
func Describe(bindings []Binding) ([]CallDescriptor, error) {
	if err := validateBindings(bindings); err != nil {
		return nil, err
	}

	var out []CallDescriptor
	for _, b := range bindings {
		if b.phase == noLifecyclePhase {
			out = append(out, b.Descriptor())
		}
	}

	return out, nil
}

// Package actorx runs stateful actors in one process with bounded mailboxes,
// serial message handling, owned asynchronous tasks, and complete lifecycle shutdown.
//
// An Actor is an application object supplying Bindings. Bindings only constructs
// typed handler entries; acquire instance resources in Started, unblock workers
// in Stopping, and finish cleanup in Stopped. Runtime.Spawn installs the entries
// and waits for startup before returning an Addr. Use that address for Send,
// Call, Stream, Stop and Wait. Wait observes complete exit; Stop only requests it.
// Runtime.Close stops all owned actors and waits for their shutdown.
//
// Describe validates bindings and returns ordinary message types and modes
// without retaining handlers or business objects. Generated and handwritten
// Bindings support metadata reads on a concrete nil pointer, while Spawn requires
// a live instance. Each live instance binds its own receiver.
//
// Context.StartTask runs work in a worker and returns its completion to the
// owning actor's serial execution. Workers use input snapshots; handlers and
// completion callbacks own mutable actor state. Shutdown waits for tasks and
// discards results that were not adopted.
//
// The core depends only on the standard library. Optional Channel implementations
// provide transport; logical addressing, activation and distributed ownership
// belong outside this package.
package actorx

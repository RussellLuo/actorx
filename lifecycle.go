package actorx

import "errors"

// Internal execution phases are diagnostic details, separate from public State.
const (
	phaseSetup      = "setup"
	phaseBinding    = "binding"
	phaseStarted    = "started"
	phaseIdle       = "idle"
	phaseHandler    = "handler"
	phaseCompletion = "completion"
	phaseStopping   = "stopping"
	phaseWorkers    = "workers"
	phaseRollback   = "rollback"
	phaseStopped    = "stopped"
	phaseDone       = "done"
)

// State describes the execution unit's lifecycle, not business readiness.
type State uint8

const (
	StateStarted State = iota
	StateRunning
	StateStopping
	StateStopped
)

// Started is a local lifecycle notice handled before Spawn returns and ordinary
// admission. The runtime dispatches it at the phase boundary, without queueing.
type Started struct{}

// Stopping is a local lifecycle notice handled before cleanup and worker waits.
// Reason is a cause snapshot.
type Stopping struct{ Reason error }

// Stopped is a local lifecycle notice handled after resource rollback,
// before the final Exit is published.
// Reason is a cause snapshot; Exit.Reason includes this handler's failure too.
type Stopped struct{ Reason error }

// State returns a concurrent-safe snapshot. A context without an actor reports
// StateStopped. StateStopped may precede complete exit while its message handler runs.
func (ctx *Context) State() State {
	if ctx == nil || ctx.actor == nil {
		return StateStopped
	}

	ctx.actor.mu.Lock()
	defer ctx.actor.mu.Unlock()

	return ctx.actor.state
}

// start runs on the actor's execution loop. Spawn waits for the initialization
// result; failed initialization stops the actor and is followed by full cleanup.
func (a *actor) start() {
	a.setPhase(phaseStarted)
	err := a.dispatch(item{msg: Started{}})

	// Publish only after initialization succeeds, under Runtime -> Actor lock
	// order. Stop/Close during Started must never be overwritten by Running.
	a.rt.mu.Lock()
	if a.rt.closed {
		err = errors.Join(err, ErrClosed)
	}

	a.mu.Lock()
	if a.stopped {
		err = errors.Join(err, ErrStopped)
	}
	if err == nil {
		a.started = true
		a.state = StateRunning
		a.phase = phaseIdle
	}

	a.mu.Unlock()
	a.rt.mu.Unlock()

	if err != nil {
		a.record(err)
		a.stop(nil)
	}

	a.ready <- err
}

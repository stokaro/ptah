package embedrun

import "time"

// EventKind is what happened.
type EventKind string

const (
	// EventClaimed is a worker taking the run.
	EventClaimed EventKind = "claimed"
	// EventPhase is a lifecycle transition.
	EventPhase EventKind = "phase"
	// EventCheckpoint is a completed batch.
	EventCheckpoint EventKind = "checkpoint"
	// EventPaused is a run stopped at a safe boundary.
	EventPaused EventKind = "paused"
	// EventResumed is a paused run continuing.
	EventResumed EventKind = "resumed"
	// EventFailed is a run stopped by a failure.
	EventFailed EventKind = "failed"
	// EventFenced is a stale worker refused, which is worth recording: it is
	// the evidence that two processes believed they held one run.
	EventFenced EventKind = "fenced"
	// EventCutover is the active pointer moving.
	EventCutover EventKind = "cutover"
	// EventRollback is the active pointer moving back.
	EventRollback EventKind = "rollback"
)

// Event is one entry in a run's audit trail.
//
// There is no field for source content and none for a vector, and that absence
// is the design rather than an omission. An audit trail able to carry the
// corpus would become a second copy of it -- outside the access control, the
// retention policy and the deletion path the corpus itself has -- and the
// difference between "we log what happened" and "we log the data" is exactly
// the difference an operator cannot audit after the fact (stokaro/ptah#2068).
//
// What it carries instead are identities, counts and reasons: enough to
// reconstruct what a run did, and nothing that reconstructs what it embedded.
type Event struct {
	// RunID is the run this belongs to.
	RunID string
	// Kind is what happened.
	Kind EventKind
	// At is when.
	At time.Time
	// Actor is the worker or operator responsible.
	Actor string
	// FencingToken is the token the actor held, which is what makes a refused
	// commit reconstructable afterwards.
	FencingToken int64
	// FromPhase and ToPhase are set on a transition.
	FromPhase Phase
	ToPhase   Phase
	// Detail is prose about the event: a pause reason, a failure detail, the
	// generation a cutover moved to.
	//
	// It is prose ABOUT the run and never row content. Nothing in this package
	// puts source text here, and a caller that did would be putting the corpus
	// in the audit log by hand.
	Detail string
	// Counts is the run's progress at the moment of the event, so a reader can
	// see the shape of a run without replaying every checkpoint.
	Counts Progress
}

// NewEvent records one transition against a run.
func NewEvent(run *Run, kind EventKind, actor, detail string) Event {
	return Event{
		RunID:        run.ID,
		Kind:         kind,
		At:           time.Now().UTC(),
		Actor:        actor,
		FencingToken: run.FencingToken,
		ToPhase:      run.Phase,
		Detail:       detail,
		Counts:       run.Progress,
	}
}

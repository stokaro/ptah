package embedpg

import (
	"fmt"
	"time"
)

// OutboxRelease is what a retirement did about the change capture on a source.
//
// It is a result rather than a bare error because retirement has to SAY what
// happened either way. "Retire removes the generation and its bookkeeping" is
// what the guide promises, and an operator told nothing cannot tell a removal
// from the silence that preceded stokaro/ptah#2649.
type OutboxRelease struct {
	// RetiredAt is the database time committed to the generation registry and
	// its terminalized runs. It is sampled after lifecycle locks are held, so
	// evidence reports the durable instant rather than a caller's stale clock.
	RetiredAt time.Time
	// Watched is whether this generation was built with an outbox at all.
	//
	// Read off the generation's own recorded specification, never off the file
	// the invocation happened to carry.
	Watched bool
	// Source is the relation the outbox belongs to, as a diagnostic says it.
	Source string
	// Remaining is how many live outbox-mode generations are still fed from
	// that source.
	Remaining int
	// Removed is whether the triggers, capture function and event table are
	// gone. False with Remaining > 0 means they were deliberately left.
	Removed bool
}

// Sentence is what an operator is told about the change capture.
//
// Empty for a generation no outbox watched, because there is nothing to report
// about a mechanism that was never installed -- and a sentence saying so on
// every immutable retirement would train a reader to skip the line that
// matters.
func (r OutboxRelease) Sentence() string {
	switch {
	case !r.Watched:
		return ""
	case r.Removed:
		return fmt.Sprintf("the outbox is gone: its triggers, capture function and event "+
			"table were the last thing Ptah had on %s", r.Source)
	default:
		return fmt.Sprintf("the outbox stays: %d other generation(s) still read %s",
			r.Remaining, r.Source)
	}
}

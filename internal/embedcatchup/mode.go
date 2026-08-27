// Package embedcatchup accounts for source changes that happen while a
// generation is being built.
//
// A backfill over a table nobody is writing to is a loop. A backfill over a
// live table is a race, and what this package decides is which races the
// selected mode can actually prove it won -- because "the backfill finished" and
// "the backfill covers the source" are different claims, and only the second one
// justifies a cutover (stokaro/ptah#2068).
package embedcatchup

import (
	"fmt"
	"slices"
	"time"
)

// Mode is how source changes during a migration are accounted for.
//
// There is no mode for "we hope nothing changed". A source that can change and
// has no mode is reported as unprovable, which is the epic's cutover rule and
// the reason ModeNone exists as a value rather than as an absence.
type Mode string

const (
	// ModeNone is no mode at all.
	ModeNone Mode = ""
	// ModeImmutable is a source that cannot change, or one the operator has
	// explicitly paused.
	//
	// The strongest mode and the one that costs the most: it is either an
	// immutable corpus or application downtime.
	ModeImmutable Mode = "immutable"
	// ModeDualWrite is the application writing both generations.
	//
	// Ptah cannot prove this from a configuration statement. What it can do is
	// require the writer to produce evidence, and report the result as partial
	// when the evidence is not there.
	ModeDualWrite Mode = "dual_write"
	// ModeOutbox is a transactional outbox in the source database.
	//
	// The preferred live-write mode, because the outbox row and the source
	// change are one transaction: a change that committed has an event, and a
	// change that rolled back has none. Nothing outside the database has to be
	// trusted for that.
	ModeOutbox Mode = "outbox"
)

// Modes lists what this build supports.
//
// External CDC is deliberately absent rather than present and unimplemented.
// The epic puts it outside the first vertical, and a mode that could be
// selected and then silently did nothing is worse than one that cannot be
// selected at all.
var Modes = []Mode{ModeImmutable, ModeDualWrite, ModeOutbox}

// ParseMode reads a mode name.
func ParseMode(raw string) (Mode, error) {
	candidate := Mode(raw)
	if candidate == ModeNone {
		return ModeNone, nil
	}
	if slices.Contains(Modes, candidate) {
		return candidate, nil
	}
	return ModeNone, fmt.Errorf(
		"unknown consistency mode %q; this build has %v", raw, Modes)
}

// Guarantee is what a mode proved, and what it did not.
type Guarantee struct {
	// Mode is the mode assessed.
	Mode Mode
	// Complete reports whether the backfill can be said to cover the source as
	// it is now.
	Complete bool
	// Blockers are the reasons it cannot, empty when it can.
	Blockers []string
	// Partial names what the mode cannot establish even when nothing blocks.
	//
	// It is separate from a blocker because it does not stop a cutover: it
	// tells an operator what they are accepting. A dual-write migration whose
	// writer reports healthy is not the same evidence as an outbox with no
	// unprocessed events, and a report that presented them alike would be
	// hiding the difference an operator is entitled to.
	Partial []string
}

// SourceState is what is true about the source.
type SourceState struct {
	// Mutable reports whether the source can change during the run.
	Mutable bool
	// Paused reports whether the operator has stopped writes, which is what
	// makes a mutable source behave like an immutable one for the duration.
	Paused bool
}

// Assess answers whether the selected mode has proved its completion condition.
func Assess(mode Mode, source SourceState, outbox Barrier, dual DualWriteEvidence, now time.Time) Guarantee {
	guarantee := Guarantee{Mode: mode}
	if !source.Mutable {
		// Nothing to account for. The mode is irrelevant and saying so beats
		// requiring one.
		guarantee.Complete = true
		return guarantee
	}

	switch mode {
	case ModeNone:
		guarantee.Blockers = append(guarantee.Blockers,
			"the source can change and no consistency mode was selected, so nothing establishes "+
				"that the backfill covers the source as it is now")
	case ModeImmutable:
		assessPaused(&guarantee, source)
	case ModeOutbox:
		assessOutbox(&guarantee, outbox)
	case ModeDualWrite:
		assessDualWrite(&guarantee, dual, now)
	default:
		guarantee.Blockers = append(guarantee.Blockers,
			fmt.Sprintf("consistency mode %q is not one this build can act on", mode))
	}
	guarantee.Complete = len(guarantee.Blockers) == 0
	return guarantee
}

// assessPaused holds the immutable mode to what makes it immutable.
func assessPaused(guarantee *Guarantee, source SourceState) {
	if source.Paused {
		return
	}
	// The mode was selected and the thing it names is not true. That is a
	// different failure from selecting no mode, and an operator told the
	// generic message would go looking for a configuration they already wrote.
	guarantee.Blockers = append(guarantee.Blockers,
		"the immutable mode was selected and the source is neither immutable nor paused")
}

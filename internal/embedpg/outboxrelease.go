package embedpg

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedstore"
)

// OutboxRelease is what a retirement did about the change capture on a source.
//
// It is a result rather than a bare error because retirement has to SAY what
// happened either way. "Retire removes the generation and its bookkeeping" is
// what the guide promises, and an operator told nothing cannot tell a removal
// from the silence that preceded stokaro/ptah#2649.
type OutboxRelease struct {
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

// ReleaseOutbox takes the change capture off a source when the retired
// generation was the last one reading it.
//
// An outbox belongs to a SOURCE TABLE rather than to a generation -- two
// generations over one source share its changes -- so retirement can only
// remove it once nothing is left to feed. Until stokaro/ptah#2649 nothing
// removed it at all: both triggers went on firing on the operator's table for
// every write, and the event table grew with nothing that would ever read or
// trim it.
//
// Three things here are read from the REGISTRY and the generation's own
// recorded specification rather than from the invocation's file, and each one
// was a way to get the wrong answer:
//
//   - which source the outbox is on. The first fix asked the registry how many
//     generations held vectors in the retired generation's TARGET table, while
//     the outbox is keyed on the source. A specification whose target differs
//     from its source is accepted, so two generations could share one source
//     and one outbox while having different targets -- and retiring either
//     counted zero and destroyed the survivor's change capture, which surfaced
//     only when its next catch-up failed on a missing relation.
//   - whether there was an outbox. The mode came off the invocation's
//     specification, so retiring with an immutable file skipped the removal in
//     silence.
//   - which outbox to uninstall. Built from the invocation's specification, a
//     file naming another source would have uninstalled another table's
//     triggers.
//
// The generation must be marked retired before this is called: the question is
// "was this the last one", and the count excludes it by identity as well, so
// the order is belt and braces rather than either alone.
func (s *Store) ReleaseOutbox(
	ctx context.Context, registered embedstore.Generation,
) (OutboxRelease, error) {
	source := embedstore.QualifiedName(registered.SourceSchema, registered.SourceTable)
	if registered.ConsistencyMode != string(embedcatchup.ModeOutbox) {
		return OutboxRelease{Source: source}, nil
	}
	recorded, err := RecordedSpec(registered,
		"watched by an outbox this retirement would have to remove")
	if err != nil {
		return OutboxRelease{}, err
	}
	remaining, err := s.LiveOutboxReadersOf(
		ctx, registered.SourceSchema, registered.SourceTable, registered.Identity)
	if err != nil {
		return OutboxRelease{}, err
	}
	release := OutboxRelease{Watched: true, Source: source, Remaining: remaining}
	if remaining > 0 {
		return release, nil
	}
	outbox, err := NewOutbox(s.db, recorded.Spec)
	if err != nil {
		return OutboxRelease{}, err
	}
	if err := outbox.Uninstall(ctx); err != nil {
		return OutboxRelease{}, fmt.Errorf("remove the outbox on %s: %w", source, err)
	}
	release.Removed = true
	return release, nil
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

package embedreport

import (
	"time"

	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedspec"
)

// BuildRelease is what a generation change proposes, as the record a registry
// holds.
//
// It is built here rather than in the command that publishes it because the
// mapping is a capability rather than an interface detail: what a release says
// about a generation is the same answer whether a person asked for it or a
// protocol did, and the surface that publishes it is free to change without
// taking the answer with it.
//
// Two arguments come from outside the specification. The generation being
// replaced is the plan's resolved answer rather than the operator's --current
// flag, so a release names what the database says queries read now. The time is
// passed in rather than taken here, so a caller publishing a release and a
// cutover in one run records one moment for both.
func BuildRelease(loaded embedspec.Loaded, plan Plan, at time.Time) embedrelease.Release {
	identity := loaded.Spec.Identity()
	target := loaded.Spec.Target
	return embedrelease.Release{
		Generation: plan.Desired,
		Replaces:   plan.Current,
		SpecDigest: loaded.Digest,
		// The corpus is absent by construction: a corpus is named at
		// `evaluate` time and a release is written before anything has run, so
		// there is nothing here to name. The field stays empty rather than
		// carrying a path nobody measured against.
		Target:          target.Schema + "." + target.Table + "." + target.Column,
		Reproducibility: string(identity.Reproducibility),
		Reason:          identity.ReproducibilityReason,
		CreatedAt:       at.UTC(),
	}
}

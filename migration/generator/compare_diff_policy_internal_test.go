package generator

// White-box testing required: compareOptionsWithDiffPolicy is the unexported
// seam that tells the comparison what planGeneratedMigrationSpecs will delete
// from its answer, and the property under test is which policy kinds reach it.
// There is no exported surface that reports the forwarded options.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/migration/diffpolicy"
)

// TestCompareOptionsWithDiffPolicyForwardsEverySkipTheGuardReads pins the
// forwarding the SQLite virtual-table guard depends on.
//
// The guard runs inside the comparison and refuses on the statements it
// predicts, while the filter that deletes those statements runs afterwards in
// planGeneratedMigrationSpecs. Forwarding only the table drop left the guard
// refusing plans this package had already emptied: measured on the command,
// `ptah migrations generate` with `diff.skip: [drop_table, drop_column]`
// against an fts4 database exited 2 for a column drop on an ordinary table,
// while the same run with the guard's opt-in set exited 0 and wrote no
// migration file at all.
//
// drop_enum is deliberately not forwarded; the census in
// internal/sqlitevirtual is what holds that decision in place.
func TestCompareOptionsWithDiffPolicyForwardsEverySkipTheGuardReads(t *testing.T) {
	tests := []struct {
		name  string
		kinds []diffpolicy.ChangeKind
		read  func(*config.CompareOptions) bool
	}{
		{
			name:  "drop_table",
			kinds: []diffpolicy.ChangeKind{diffpolicy.DropTable},
			read:  func(opts *config.CompareOptions) bool { return opts.SkipTableDrops },
		},
		{
			name:  "drop_column",
			kinds: []diffpolicy.ChangeKind{diffpolicy.DropColumn},
			read:  func(opts *config.CompareOptions) bool { return opts.SkipColumnDrops },
		},
		{
			name:  "drop_index",
			kinds: []diffpolicy.ChangeKind{diffpolicy.DropIndex},
			read:  func(opts *config.CompareOptions) bool { return opts.SkipIndexDrops },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" reaches the comparison when the policy skips it", func(t *testing.T) {
			c := qt.New(t)

			merged := compareOptionsWithDiffPolicy(nil, DiffPolicy{SkipChangeKinds: tt.kinds})

			c.Assert(tt.read(merged), qt.IsTrue)
		})

		// The control: one kind must not switch on another kind's field, which
		// is what a copy-paste in this function would produce.
		t.Run(tt.name+" is not set by an unrelated skip", func(t *testing.T) {
			c := qt.New(t)

			merged := compareOptionsWithDiffPolicy(nil, DiffPolicy{
				SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropEnum},
			})

			c.Assert(tt.read(merged), qt.IsFalse)
		})
	}
}

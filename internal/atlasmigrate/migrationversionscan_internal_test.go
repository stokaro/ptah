package atlasmigrate

// White-box testing required: the rows below have to name the second the scan
// starts from, and the wall clock MigrationVersion reads has no exported seam.
// Reaching the collision through GenerateDiff would mean racing two diffs into
// one real second, and reaching the two-file run would mean a dialect that
// plans a concurrent index -- a fixture that passes or fails by timing and by
// dialect rather than by the rule under test. Everything asserted is otherwise
// observable: the version in the name of the file the diff publishes.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestFirstFreeMigrationVersionRun pins the `...235960` half of
// stokaro/ptah#938 on `migrate diff`.
//
// Two ways to reach it without touching the clock:
//
//   - two diffs inside one second, where the collision step used to add one to
//     a version numbered :59;
//   - one diff whose plan is split in two files, the concurrent-index half
//     staged at version+1 (BuildMigrationFileContents), where a base at :59 put
//     the second file on the sixtieth second.
//
// Both produce a fourteen-digit version that is not an instant, which is what
// the issue names.
//
// The cheaper wrong implementation is the loop this branch shipped --
// `for collidesWithTakenVersions(taken, version, count) { version++ }` --
// which answers 20260809042360 on rows two and three and never looks at the
// slots after the base.
func TestFirstFreeMigrationVersionRun(t *testing.T) {
	tests := []struct {
		name    string
		taken   []int64
		version int64
		count   int
		want    int64
	}{
		{
			name:    "an empty directory keeps the clock",
			version: 20260809042359,
			count:   1,
			want:    20260809042359,
		},
		{
			name:    "a second diff in the same second at :59 rolls the minute",
			taken:   []int64{20260809042359},
			version: 20260809042359,
			count:   1,
			want:    20260809042400,
		},
		{
			name:    "a two-file plan may not straddle the minute",
			version: 20260809042359,
			count:   2,
			want:    20260809042400,
		},
		{
			name:    "a two-file plan mid-minute is left where it is",
			version: 20260809042358,
			count:   2,
			want:    20260809042358,
		},
		{
			name:    "a collision mid-minute still steps by one",
			taken:   []int64{20260809042358},
			version: 20260809042358,
			count:   1,
			want:    20260809042359,
		},
		{
			name:    "a two-file plan steps past a taken second slot",
			taken:   []int64{20260809042359},
			version: 20260809042358,
			count:   2,
			want:    20260809042400,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			taken := make(map[int64]struct{}, len(test.taken))
			for _, version := range test.taken {
				taken[version] = struct{}{}
			}

			base, err := firstFreeMigrationVersionRun(taken, test.version, test.count)

			c.Assert(err, qt.IsNil)
			c.Assert(base, qt.Equals, test.want)
		})
	}
}

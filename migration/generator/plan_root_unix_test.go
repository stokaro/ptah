//go:build unix

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// TestMigrationPlanWriteFiles_RefusesDirectoryReplacedBeforePublication is the
// black-box half of the rooted-writer regression: the replacement happens in
// the product's own window between planning and publication, so no test seam is
// involved.
//
// The two rows differ only in whether a confinement root is configured, and
// that is the assertion. The plan holds the migration directory open from the
// moment it is built, so the refusal comes from the handle and is owed to every
// caller -- a project run under an allowed root and a direct CLI run with an
// absolute --dir get the same answer. A guard that only worked under a root
// would leave the direct-CLI shape, which is the common one, unprotected.
// TestMigrationPlanBindRefusesADirectoryReachedThroughAnAncestorOutsideTheRoot
// pins the thing the root does contribute.
//
// Both rows must leave the substitute untouched, and the retained directory
// too: an error that still wrote somewhere is not a refusal.
//
// //go:build unix because the swap is a symlink replacement.
func TestMigrationPlanWriteFiles_RefusesDirectoryReplacedBeforePublication(t *testing.T) {
	const wantErr = `migration directory changed after migration planning: ` +
		`opened directory path changed: .*`

	tests := []struct {
		name string
		// allowedRoot returns the confinement root, or "" for the direct-CLI
		// shape.
		allowedRoot func(root string) string
	}{
		{
			name:        "no allowed root",
			allowedRoot: func(string) string { return "" },
		},
		{
			name:        "under an allowed root",
			allowedRoot: func(root string) string { return root },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			bound := filepath.Join(root, "real")
			c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
			selected := filepath.Join(root, "migrations")
			c.Assert(os.Symlink(bound, selected), qt.IsNil)

			plan, err := generator.NewMigrationPlanForTest(
				selected,
				test.allowedRoot(root),
				"",
				[]generator.MigrationPlanSpecForTest{{
					Version: 1700000000,
					Name:    "create_users",
					UpSQL:   "CREATE TABLE users (id INTEGER);\n",
					DownSQL: "DROP TABLE users;\n",
				}},
			)
			c.Assert(err, qt.IsNil)

			c.Assert(os.Remove(selected), qt.IsNil)
			c.Assert(os.Symlink(decoy, selected), qt.IsNil)

			files, err := plan.WriteFilesContext(t.Context())

			c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
			c.Assert(err, qt.ErrorMatches, wantErr)
			c.Assert(files, qt.IsNil)
			decoyEntries, err := os.ReadDir(decoy)
			c.Assert(err, qt.IsNil)
			c.Assert(decoyEntries, qt.HasLen, 0)
			boundEntries, err := os.ReadDir(bound)
			c.Assert(err, qt.IsNil)
			c.Assert(boundEntries, qt.HasLen, 0)
		})
	}
}

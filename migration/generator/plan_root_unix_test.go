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
// The two rows separate the two guards, because either one alone would make
// this pass for the wrong reason. Without a confinement root the run is stopped
// by the plan's recorded identity; with one it is stopped earlier, when the
// rooted open refuses a directory that now resolves outside the root. Both must
// leave the substitute untouched.
//
// //go:build unix because the swap is a symlink replacement.
func TestMigrationPlanWriteFiles_RefusesDirectoryReplacedBeforePublication(t *testing.T) {
	tests := []struct {
		name string
		// allowedRoot returns the confinement root, or "" for the direct-CLI
		// shape.
		allowedRoot func(root string) string
		wantErr     string
	}{
		{
			name:        "no allowed root: the recorded identity refuses it",
			allowedRoot: func(string) string { return "" },
			wantErr:     "migration directory changed after migration planning",
		},
		{
			name:        "under an allowed root: the rooted open refuses it",
			allowedRoot: func(root string) string { return root },
			wantErr:     ".*outside allowed root.*",
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

			c.Assert(err, qt.ErrorMatches, test.wantErr)
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

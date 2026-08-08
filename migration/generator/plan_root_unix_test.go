//go:build unix

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// The two rows below decide, explicitly, what a migration directory containing
// a symbolic link is. They are not about a race: nothing moves while they run.
//
// Binding the directory changed the answer, and the change is only partly
// visible from the outside. Reading the directory by pathname followed any link
// in it; reading it through the bound handle follows a link that stays inside
// the directory and refuses one that leaves it. So a shared migration linked in
// from a sibling directory -- `migrations/1000000000_shared.up.sql ->
// ../shared/1000000000_shared.up.sql`, a layout master accepted -- is now
// refused, and that is deliberate rather than incidental: the directory is read,
// checksummed and published through the object the run opened, and bytes living
// outside it are not part of a directory Ptah can seal. Following the link
// again would give back the escape the binding exists to close.
//
// The refusal has to name what it refused, or an operator meets it as
// "path escapes from parent" and reads it as a bug. And the link that stays
// inside the directory has to keep working, or the refusal is a much wider one
// than the rule it enforces.

// TestMigrationPlanBindRefusesAMigrationFileLinkedInFromOutsideTheDirectory
// pins the refusal and its wording.
func TestMigrationPlanBindRefusesAMigrationFileLinkedInFromOutsideTheDirectory(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	shared := filepath.Join(root, "shared")
	outputDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(shared, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(shared, "1000000000_shared.up.sql"),
		[]byte("CREATE TABLE shared (id INTEGER);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.Symlink(
		filepath.Join("..", "shared", "1000000000_shared.up.sql"),
		filepath.Join(outputDir, "1000000000_shared.up.sql"),
	), qt.IsNil)

	plan, err := generator.NewMigrationPlanForTest(
		outputDir,
		"",
		"",
		[]generator.MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
		}},
	)

	c.Assert(err, qt.ErrorMatches,
		`migration directory .*: symbolic links resolving outside it: `+
			`1000000000_shared\.up\.sql; a migration file linked in from another `+
			`directory is refused because the whole directory is read, checksummed `+
			`and published through the directory itself: .*`)
	c.Assert(plan, qt.IsNil)
	entries, err := os.ReadDir(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
}

// TestMigrationPlanPublishesBesideAMigrationFileLinkedWithinTheDirectory is the
// other direction, and it is what keeps the refusal above narrow: a link whose
// target is inside the migration directory resolves through the bound handle
// like any other entry, so the plan is built and the batch is published beside
// it.
func TestMigrationPlanPublishesBesideAMigrationFileLinkedWithinTheDirectory(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(outputDir, "1000000000_shared.up.sql.source"),
		[]byte("CREATE TABLE shared (id INTEGER);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.Symlink(
		"1000000000_shared.up.sql.source",
		filepath.Join(outputDir, "1000000000_shared.up.sql"),
	), qt.IsNil)

	plan, err := generator.NewMigrationPlanForTest(
		outputDir,
		"",
		"",
		[]generator.MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
		}},
	)
	c.Assert(err, qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	entries, err := os.ReadDir(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 4)
}

// TestMigrationPlanBindReportsANonSymlinkDirectoryFailureUnchanged keeps the
// explanation from becoming a story the code tells about every failure.
//
// The directory here has a symbolic link in it, and something else entirely is
// wrong with it: a metadata file spelled `ATLAS.SUM`. The link is not why the
// read failed and must not be named, or the next operator to hit an unrelated
// problem is sent to look at a file that is fine.
func TestMigrationPlanBindReportsANonSymlinkDirectoryFailureUnchanged(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(outputDir, "1000000000_shared.up.sql.source"),
		[]byte("CREATE TABLE shared (id INTEGER);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.Symlink(
		"1000000000_shared.up.sql.source",
		filepath.Join(outputDir, "1000000000_shared.up.sql"),
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(outputDir, "ATLAS.SUM"),
		[]byte("h1:0000\n"), 0o600,
	), qt.IsNil)

	plan, err := generator.NewMigrationPlanForTest(
		outputDir,
		"",
		"",
		[]generator.MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
		}},
	)

	c.Assert(err, qt.ErrorMatches,
		`.*migration metadata file "ATLAS\.SUM" must use canonical name "atlas\.sum"`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "symbolic links resolving outside it")
	c.Assert(plan, qt.IsNil)
}

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

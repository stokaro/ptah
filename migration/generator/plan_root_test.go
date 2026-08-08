package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// A plan records which filesystem object it was built against, not merely what
// that object contained. The window these fixtures use is the product's own:
// PlanMigration and WriteFilesContext are separate calls, so a caller running
// pre-publication work between them leaves a real gap, and the directory can be
// replaced inside it (stokaro/ptah#1118).
//
// Measured on master, the replacement below published both migration files into
// the substitute and returned no error, because the pre-publication comparison
// only asked whether the contents matched and two empty directories match.
//
// The control that this surface can report a publication at all is
// TestMigrationPlanWriteFiles_PublishesMultiplePairsAndReports next door: the
// same constructor and the same call publish six artifacts when nothing is
// replaced.

// TestMigrationPlanWriteFiles_RefusesRecreatedDirectory is deliberately not
// build-tagged. Remove-and-recreate of one pathname is the substitution an
// attacker reaches for first -- it needs no second directory to point at -- and
// it is the one where an identity comparison can be answered wrongly by the
// operating system rather than by the product. Measured on 20 cycles of one
// pathname: macOS APFS reissued the inode 0 times, so a detached fs.FileInfo
// looked like a working guard there, while ext4 reissued it 20 times out of 20
// and the same guard never fired. The plan holds the directory open now, which
// takes the identifier out of circulation and makes the answer the same
// everywhere.
//
// The removal is os.RemoveAll rather than os.Remove because the plan is holding
// a handle on the directory while the test removes it. Go's os.RemoveAll
// deletes through the parent with POSIX semantics on every platform it
// supports, so the name is free immediately and the recreate below succeeds;
// os.Remove on Windows goes through RemoveDirectory, which marks an open
// directory for deletion on close and leaves the name unusable until then. The
// hostile step has to be one an attacker can actually complete, or the test
// would be asserting that the attack is impossible for a reason no operator
// should have to rely on.
func TestMigrationPlanWriteFiles_RefusesRecreatedDirectory(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

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

	// Same pathname, same (empty) contents, different filesystem object.
	c.Assert(os.RemoveAll(outputDir), qt.IsNil)
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	entries, err := os.ReadDir(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// TestMigrationPlanWriteFiles_RefusesADirectoryThatAppearedAfterPlanning is the
// absent-directory half of the same question. A plan built for a directory that
// does not exist yet holds its parent, not the directory, so the identity it
// has to defend is the free name rather than an object: something that
// materialized at that name while the plan was outstanding is not the
// destination the plan was built for, whatever it contains.
//
// TestMigrationPlanWriteFiles_CreatesMissingOutputParents is the control -- the
// same missing-directory shape, nothing racing it, published normally.
func TestMigrationPlanWriteFiles_RefusesADirectoryThatAppearedAfterPlanning(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")

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

	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	entries, err := os.ReadDir(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

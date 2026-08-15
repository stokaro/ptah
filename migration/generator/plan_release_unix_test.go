//go:build unix

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// A plan holds the migration directory open from the moment it is built, which
// is the whole point of stokaro/ptah#1118 -- and means the plan owns two
// descriptors that something has to give back. A successful publication gave
// them back; a failed one did not, and os.Root only closes from a finalizer, so
// the hold outlived the call by an unbounded amount of time. On Windows that is
// also a window in which nothing else can rename or remove the directory, which
// is how the CI job noticed: four `TestMigrationPlanWriteFiles_` rows whose
// publication fails could not have their t.TempDir removed afterwards.
//
// //go:build unix because the measurement is /dev/fd. The Windows publication
// job measures the same property from the other side, by being able to delete
// the temporary directory once the test returns.

// openDescriptorCount counts this process's open descriptors, including the one
// the listing itself is using -- consistently, so a difference between two
// counts is a difference in what the product holds.
func openDescriptorCount(tb testing.TB) int {
	c := qt.New(tb)
	c.Helper()
	dir, err := os.Open("/dev/fd")
	c.Assert(err, qt.IsNil)
	names, err := dir.Readdirnames(-1)
	c.Assert(err, qt.IsNil)
	c.Assert(dir.Close(), qt.IsNil)
	return len(names)
}

// TestMigrationPlanWriteFiles_ReleasesTheDirectoryWhenPublicationFails asserts
// the release, and asserts that the measurement can see the handles at all --
// without the second assertion a product that never bound anything would look
// like a product that released everything.
func TestMigrationPlanWriteFiles_ReleasesTheDirectoryWhenPublicationFails(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)
	beforePlanning := openDescriptorCount(c.TB)

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
	whileOutstanding := openDescriptorCount(c.TB)

	// Same pathname, different filesystem object: the publication is refused.
	c.Assert(os.RemoveAll(outputDir), qt.IsNil)
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())
	afterFailure := openDescriptorCount(c.TB)

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	c.Assert(
		whileOutstanding > beforePlanning,
		qt.IsTrue,
		qt.Commentf("descriptors: %d before planning, %d while the plan was outstanding",
			beforePlanning, whileOutstanding),
	)
	c.Assert(
		afterFailure,
		qt.Equals,
		beforePlanning,
		qt.Commentf("descriptors: %d before planning, %d while outstanding, %d after the failure",
			beforePlanning, whileOutstanding, afterFailure),
	)
}

// TestMigrationPlanWriteFiles_ReleasesTheDirectoryWhenPublicationSucceeds is
// the control for the row above: the same measurement over the path that
// already released, so a release that only ever happened on failure would be
// visible as this row going red.
func TestMigrationPlanWriteFiles_ReleasesTheDirectoryWhenPublicationSucceeds(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)
	beforePlanning := openDescriptorCount(c.TB)

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
	whileOutstanding := openDescriptorCount(c.TB)

	files, err := plan.WriteFilesContext(t.Context())
	afterSuccess := openDescriptorCount(c.TB)

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(
		whileOutstanding > beforePlanning,
		qt.IsTrue,
		qt.Commentf("descriptors: %d before planning, %d while the plan was outstanding",
			beforePlanning, whileOutstanding),
	)
	c.Assert(
		afterSuccess,
		qt.Equals,
		beforePlanning,
		qt.Commentf("descriptors: %d before planning, %d while outstanding, %d after publication",
			beforePlanning, whileOutstanding, afterSuccess),
	)
}

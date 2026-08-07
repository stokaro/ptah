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
	c.Assert(os.Remove(outputDir), qt.IsNil)
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	entries, err := os.ReadDir(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

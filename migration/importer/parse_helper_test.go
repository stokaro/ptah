package importer_test

import (
	"io/fs"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// parseMigrations reads a source directory and returns just the migrations.
//
// Parse also reports which files it consumed and which it declined, which most
// of these tests are not about. The two that are assert on the full result
// instead.
func parseMigrations(t testing.TB, parser importer.Parser, fsys fs.FS) ([]importer.SourceMigration, error) {
	t.Helper()

	parsed, err := parser.Parse(fsys)
	if err != nil {
		return nil, err
	}
	qt.Assert(t, parsed, qt.IsNotNil)
	return parsed.Migrations, nil
}

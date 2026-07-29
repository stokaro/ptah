package migrationsnapshot_test

import (
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migrationsnapshot"
)

func TestCapture_IncludesOnlyMigrationInputs(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		".ptah-lint.yaml": {Data: []byte("rules: {}\n")},
		"1.sql":           {Data: []byte("SELECT 1;\n")},
		"README.md":       {Data: []byte("large unrelated file\n")},
		"atlas.sum":       {Data: []byte("h1:test\n")},
		"nested/2.SQL":    {Data: []byte("SELECT 2;\n")},
		"ptah.sum":        {Data: []byte("h1:test\n")},
	}

	snapshot, err := migrationsnapshot.Capture(source)
	c.Assert(err, qt.IsNil)

	c.Assert(fstest.TestFS(
		snapshot,
		".ptah-lint.yaml",
		"1.sql",
		"atlas.sum",
		"nested/2.SQL",
		"ptah.sum",
	), qt.IsNil)
	_, err = fs.Stat(snapshot, "README.md")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestCaptureStable_RejectsChangingDirectory(t *testing.T) {
	c := qt.New(t)
	source := &changingFS{
		FS: fstest.MapFS{
			"1.sql": {Data: []byte("SELECT 1;\n")},
		},
		first:  []byte("SELECT 1;\n"),
		second: []byte("SELECT 2;\n"),
	}

	_, err := migrationsnapshot.CaptureStable(source)

	c.Assert(err, qt.ErrorIs, migrationsnapshot.ErrChangedDuringCapture)
}

type changingFS struct {
	fs.FS
	first  []byte
	second []byte
	reads  int
}

func (f *changingFS) ReadFile(string) ([]byte, error) {
	f.reads++
	if f.reads == 1 {
		return slices.Clone(f.first), nil
	}
	return slices.Clone(f.second), nil
}

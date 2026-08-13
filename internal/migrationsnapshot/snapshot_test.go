package migrationsnapshot_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationsnapshot"
)

func TestCaptureDirectory_MissingRootIsEmptyHistory(t *testing.T) {
	c := qt.New(t)

	snapshot, err := migrationsnapshot.CaptureDirectory(filepath.Join(t.TempDir(), "missing"))

	c.Assert(err, qt.IsNil)
	entries, err := fs.ReadDir(snapshot, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// TestCaptureExistingDirectory_MissingRootIsAnError is the other half of
// TestCaptureDirectory_MissingRootIsEmptyHistory, and the pair is the point:
// the two captures answer the same missing path differently, so a verb picks
// the one that matches what an absent directory means to it.
//
// A verb that CREATES the directory it publishes into starts from no history; a
// verb that READS one has been handed a path that is not there.
func TestCaptureExistingDirectory_MissingRootIsAnError(t *testing.T) {
	c := qt.New(t)
	missing := filepath.Join(t.TempDir(), "missing")

	_, err := migrationsnapshot.CaptureExistingDirectory(missing)

	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(err.Error(), qt.Contains, "read migration directory "+missing)
}

func TestCaptureExistingDirectory_HappyPathReadsThePresentDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	snapshot, err := migrationsnapshot.CaptureExistingDirectory(dir)

	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(snapshot, "1.sql"), qt.IsNil)
}

func TestRequireExistingDirectory_MissingRootIsAnError(t *testing.T) {
	c := qt.New(t)
	missing := filepath.Join(t.TempDir(), "missing")

	err := migrationsnapshot.RequireExistingDirectory(missing)

	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(err.Error(), qt.Contains, "read migration directory "+missing)
}

func TestRequireExistingDirectory_HappyPathAcceptsAnEmptyDirectory(t *testing.T) {
	c := qt.New(t)

	err := migrationsnapshot.RequireExistingDirectory(t.TempDir())

	c.Assert(err, qt.IsNil)
}

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

func TestCapture_FailurePathRejectsNoncanonicalMetadataNames(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name      string
		filename  string
		canonical string
	}{
		{name: "atlas sum", filename: "ATLAS.SUM", canonical: "atlas.sum"},
		{name: "ptah sum", filename: "PTAH.SUM", canonical: "ptah.sum"},
		{name: "lint policy", filename: ".PTAH-LINT.YAML", canonical: ".ptah-lint.yaml"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			source := fstest.MapFS{
				test.filename: {Data: []byte("metadata\n")},
			}

			_, err := migrationsnapshot.Capture(source)

			c.Assert(
				err,
				qt.ErrorMatches,
				`migration metadata file "`+test.filename+`" must use canonical name "`+test.canonical+`"`,
			)
		})
	}
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

func TestCaptureStable_HappyPathAcceptsUnobservedABAChange(t *testing.T) {
	c := qt.New(t)
	initial := []byte("SELECT 1;\n")
	intermediate := []byte("SELECT 2;\n")
	source := &recordingABAFS{
		MapFS: fstest.MapFS{
			"1.sql": {Data: slices.Clone(initial)},
		},
		transitions: map[int][][]byte{
			0: {intermediate, initial},
		},
		history: [][]byte{slices.Clone(initial)},
	}

	snapshot, err := migrationsnapshot.CaptureStable(source)
	c.Assert(err, qt.IsNil)
	captured, err := fs.ReadFile(snapshot, "1.sql")
	c.Assert(err, qt.IsNil)

	c.Assert(captured, qt.DeepEquals, initial)
	c.Assert(source.observations, qt.DeepEquals, [][]byte{initial, initial})
	c.Assert(source.history, qt.DeepEquals, [][]byte{initial, intermediate, initial})
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

type recordingABAFS struct {
	fstest.MapFS
	transitions  map[int][][]byte
	observations [][]byte
	history      [][]byte
	reads        int
}

func (f *recordingABAFS) ReadFile(name string) ([]byte, error) {
	observed := slices.Clone(f.MapFS[name].Data)
	f.observations = append(f.observations, slices.Clone(observed))
	for _, contents := range f.transitions[f.reads] {
		f.MapFS[name].Data = slices.Clone(contents)
		f.history = append(f.history, slices.Clone(contents))
	}
	f.reads++
	return observed, nil
}

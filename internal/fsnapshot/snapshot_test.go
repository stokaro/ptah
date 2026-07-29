package fsnapshot_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsnapshot"
)

func TestCapture_ProducesIndependentCompleteSnapshot(t *testing.T) {
	c := qt.New(t)
	source := &countingFS{
		FS: fstest.MapFS{
			"atlas.sum":        {Data: []byte("sum")},
			"migrations/1.sql": {Data: []byte("SELECT 1;")},
		},
		reads: map[string]int{},
	}

	snapshot, err := fsnapshot.Capture(source)
	c.Assert(err, qt.IsNil)
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		"atlas.sum":        1,
		"migrations/1.sql": 1,
	})
	c.Assert(fstest.TestFS(snapshot, "atlas.sum", "migrations/1.sql"), qt.IsNil)

	contents, err := fs.ReadFile(snapshot, "migrations/1.sql")
	c.Assert(err, qt.IsNil)
	contents[0] = 'X'
	fresh, err := fs.ReadFile(snapshot, "migrations/1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(fresh), qt.Equals, "SELECT 1;")
}

func TestCapture_ClonesExistingSnapshotWithoutReadingSourceAgain(t *testing.T) {
	c := qt.New(t)
	source := &countingFS{
		FS: fstest.MapFS{
			"1.sql": {Data: []byte("SELECT 1;")},
		},
		reads: map[string]int{},
	}

	first, err := fsnapshot.Capture(source)
	c.Assert(err, qt.IsNil)
	second, err := fsnapshot.Capture(first)
	c.Assert(err, qt.IsNil)

	c.Assert(source.reads, qt.DeepEquals, map[string]int{"1.sql": 1})
	firstContents, err := fs.ReadFile(first, "1.sql")
	c.Assert(err, qt.IsNil)
	secondContents, err := fs.ReadFile(second, "1.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(secondContents, qt.DeepEquals, firstContents)
}

func TestSnapshotEqualComparesPathsAndContents(t *testing.T) {
	c := qt.New(t)
	left, err := fsnapshot.FromFiles(map[string][]byte{
		"1.sql": []byte("SELECT 1;\n"),
	})
	c.Assert(err, qt.IsNil)
	equal, err := fsnapshot.FromFiles(map[string][]byte{
		"1.sql": []byte("SELECT 1;\n"),
	})
	c.Assert(err, qt.IsNil)
	differentContents, err := fsnapshot.FromFiles(map[string][]byte{
		"1.sql": []byte("SELECT 2;\n"),
	})
	c.Assert(err, qt.IsNil)
	differentPaths, err := fsnapshot.FromFiles(map[string][]byte{
		"2.sql": []byte("SELECT 1;\n"),
	})
	c.Assert(err, qt.IsNil)

	c.Assert(left.Equal(equal), qt.IsTrue)
	c.Assert(left.Equal(differentContents), qt.IsFalse)
	c.Assert(left.Equal(differentPaths), qt.IsFalse)
}

func TestFromFiles_ClonesInput(t *testing.T) {
	c := qt.New(t)
	files := map[string][]byte{
		"nested/schema.sql": []byte("CREATE TABLE users (id INTEGER);"),
	}

	snapshot, err := fsnapshot.FromFiles(files)
	c.Assert(err, qt.IsNil)
	files["nested/schema.sql"][0] = 'X'
	delete(files, "nested/schema.sql")

	contents, err := fs.ReadFile(snapshot, "nested/schema.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "CREATE TABLE users (id INTEGER);")
	c.Assert(fstest.TestFS(snapshot, "nested/schema.sql"), qt.IsNil)
}

func TestFromFiles_RejectsInvalidPaths(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		path string
	}{
		{name: "root", path: "."},
		{name: "parent traversal", path: "../schema.sql"},
		{name: "absolute", path: "/schema.sql"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, err := fsnapshot.FromFiles(map[string][]byte{tt.path: []byte("schema")})
			c.Assert(err, qt.ErrorMatches, `invalid snapshot file path ".*"`)
		})
	}
}

func TestFromFiles_RejectsFileDirectoryConflict(t *testing.T) {
	c := qt.New(t)

	_, err := fsnapshot.FromFiles(map[string][]byte{
		"schema":          []byte("file"),
		"schema/main.sql": []byte("nested"),
	})

	c.Assert(err, qt.ErrorMatches, `invalid snapshot file paths "schema" and "schema/main.sql": a file cannot contain another file`)
}

func TestTakeFiles_DoesNotCloneOwnedContents(t *testing.T) {
	c := qt.New(t)
	files := map[string][]byte{
		"migration.sql": []byte("SELECT 1;\n"),
	}

	snapshot, err := fsnapshot.TakeFiles(files)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(snapshot, "migration.sql"), qt.IsNil)

	allocations := testing.AllocsPerRun(100, func() {
		snapshot, _ = fsnapshot.TakeFiles(files)
	})
	c.Assert(allocations, qt.Equals, float64(0))
	c.Assert(fstest.TestFS(snapshot, "migration.sql"), qt.IsNil)
}

type countingFS struct {
	fs.FS
	reads map[string]int
}

func (f *countingFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	return fs.ReadFile(f.FS, name)
}

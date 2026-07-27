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

type countingFS struct {
	fs.FS
	reads map[string]int
}

func (f *countingFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	return fs.ReadFile(f.FS, name)
}

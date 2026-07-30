package fsnapshot_test

import (
	"fmt"
	"io/fs"
	"strings"
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

func TestSnapshotWithFilesReturnsIndependentOverlay(t *testing.T) {
	c := qt.New(t)
	original, err := fsnapshot.FromFiles(map[string][]byte{
		"1_initial.sql": []byte("SELECT 1;"),
	})
	c.Assert(err, qt.IsNil)
	added := []byte("SELECT 2;")

	overlay, err := original.WithFiles(map[string][]byte{"2_next.sql": added})
	c.Assert(err, qt.IsNil)
	added[0] = 'X'

	originalEntries, err := fs.ReadDir(original, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(originalEntries, qt.HasLen, 1)
	contents, err := fs.ReadFile(overlay, "2_next.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 2;")
	c.Assert(fstest.TestFS(overlay, "1_initial.sql", "2_next.sql"), qt.IsNil)
}

func TestSnapshotDirectoryIndexPreservesFSSemantics(t *testing.T) {
	c := qt.New(t)
	files := map[string][]byte{
		"alpha/a.go":          []byte("package alpha"),
		"alpha/nested/b.go":   []byte("package nested"),
		"bravo/c.go":          []byte("package bravo"),
		"charlie/nested/d.go": []byte("package nested"),
		"root.go":             []byte("package root"),
	}
	paths := []string{
		"alpha/a.go",
		"alpha/nested/b.go",
		"bravo/c.go",
		"charlie/nested/d.go",
		"root.go",
	}

	snapshot, err := fsnapshot.FromFiles(files)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(snapshot, paths...), qt.IsNil)

	rootEntries, err := fs.ReadDir(snapshot, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(directoryEntryNames(rootEntries), qt.DeepEquals, []string{
		"alpha",
		"bravo",
		"charlie",
		"root.go",
	})
	rootEntries[0] = nil
	freshRootEntries, err := fs.ReadDir(snapshot, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(directoryEntryNames(freshRootEntries), qt.DeepEquals, []string{
		"alpha",
		"bravo",
		"charlie",
		"root.go",
	})

	cloned := snapshot.Clone()
	c.Assert(fstest.TestFS(cloned, paths...), qt.IsNil)
}

func TestCaptureMatchingBuildsIndependentDirectoryIndex(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"alpha/a.go":        {Data: []byte("package alpha")},
		"alpha/ignored.txt": {Data: []byte("ignored")},
		"bravo/b.go":        {Data: []byte("package bravo")},
	}

	captured, err := fsnapshot.CaptureMatching(
		source,
		func(name string, _ fs.DirEntry) bool {
			return strings.HasSuffix(name, ".go")
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(captured, "alpha/a.go", "bravo/b.go"), qt.IsNil)

	filtered, err := fsnapshot.CaptureMatching(
		captured,
		func(name string, _ fs.DirEntry) bool {
			return strings.HasPrefix(name, "alpha/")
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(filtered, "alpha/a.go"), qt.IsNil)

	rootEntries, err := fs.ReadDir(filtered, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(directoryEntryNames(rootEntries), qt.DeepEquals, []string{"alpha"})
}

func TestSnapshotZeroValueAndCloneImplementEmptyFS(t *testing.T) {
	c := qt.New(t)
	snapshot := fsnapshot.Snapshot{}
	tests := []struct {
		name string
		fsys fs.FS
	}{
		{name: "zero value", fsys: snapshot},
		{name: "cloned zero value", fsys: snapshot.Clone()},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(fstest.TestFS(test.fsys), qt.IsNil)
			entries, err := fs.ReadDir(test.fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
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

func TestTakeFiles_ProducesValidSnapshotFromOwnedContents(t *testing.T) {
	c := qt.New(t)
	files := map[string][]byte{
		"migration.sql": []byte("SELECT 1;\n"),
	}

	snapshot, err := fsnapshot.TakeFiles(files)
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(snapshot, "migration.sql"), qt.IsNil)
}

func BenchmarkSnapshotWalkDirectories(b *testing.B) {
	tests := []struct {
		name         string
		packageCount int
		filesPerDir  int
	}{
		{name: "50_packages", packageCount: 50, filesPerDir: 8},
		{name: "500_packages", packageCount: 500, filesPerDir: 8},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			c := qt.New(b)
			snapshot, err := fsnapshot.TakeFiles(
				benchmarkPackageFiles(test.packageCount, test.filesPerDir),
			)
			c.Assert(err, qt.IsNil)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkWalkErr = fs.WalkDir(
					snapshot,
					".",
					func(_ string, _ fs.DirEntry, err error) error {
						return err
					},
				)
			}
			b.StopTimer()
			c.Assert(benchmarkWalkErr, qt.IsNil)
		})
	}
}

var benchmarkWalkErr error

func directoryEntryNames(entries []fs.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}

func benchmarkPackageFiles(packageCount, filesPerDir int) map[string][]byte {
	files := make(map[string][]byte, packageCount*filesPerDir)
	for packageIndex := range packageCount {
		for fileIndex := range filesPerDir {
			name := fmt.Sprintf(
				"package%04d/file%04d.go",
				packageIndex,
				fileIndex,
			)
			files[name] = []byte("package benchmark\n")
		}
	}
	return files
}

type countingFS struct {
	fs.FS
	reads map[string]int
}

func (f *countingFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	return fs.ReadFile(f.FS, name)
}

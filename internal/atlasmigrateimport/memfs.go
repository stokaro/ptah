package atlasmigrateimport

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"maps"
	"slices"
	"time"
)

// memFS is a read-only, flat, in-memory filesystem holding migration files that
// have already been converted to Atlas single-file layout. It implements fs.FS,
// fs.ReadDirFS, and fs.ReadFileFS so migrator.DiscoverMigrationFiles (which uses
// fs.WalkDir) and the Atlas migration loader (which uses fs.ReadFile) can read a
// converted directory without writing it to disk. All files live directly under
// the root; there are no subdirectories.
type memFS struct {
	files map[string][]byte
}

func newMemFS(entries []Entry) memFS {
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		files[entry.Name] = entry.Data
	}
	return memFS{files: files}
}

func (m memFS) Open(name string) (fs.File, error) {
	if name == "." {
		return &memDir{entries: m.dirEntries()}, nil
	}
	data, ok := m.files[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &memFile{
		info:   memFileInfo{name: name, size: int64(len(data))},
		reader: bytes.NewReader(data),
	}, nil
}

func (m memFS) ReadFile(name string) ([]byte, error) {
	data, ok := m.files[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return slices.Clone(data), nil
}

func (m memFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	return m.dirEntries(), nil
}

func (m memFS) dirEntries() []fs.DirEntry {
	names := slices.Sorted(maps.Keys(m.files))
	entries := make([]fs.DirEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, memFileInfo{name: name, size: int64(len(m.files[name]))})
	}
	return entries
}

// memFileInfo implements both fs.FileInfo and fs.DirEntry for a converted
// migration file.
type memFileInfo struct {
	name string
	size int64
}

func (i memFileInfo) Name() string               { return i.name }
func (i memFileInfo) Size() int64                { return i.size }
func (i memFileInfo) Mode() fs.FileMode          { return 0o444 }
func (i memFileInfo) ModTime() time.Time         { return time.Time{} }
func (i memFileInfo) IsDir() bool                { return false }
func (i memFileInfo) Sys() any                   { return nil }
func (i memFileInfo) Type() fs.FileMode          { return i.Mode().Type() }
func (i memFileInfo) Info() (fs.FileInfo, error) { return i, nil }

// memFile is an open handle to one converted migration file.
type memFile struct {
	info   memFileInfo
	reader *bytes.Reader
}

func (f *memFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *memFile) Read(p []byte) (int, error) { return f.reader.Read(p) }
func (f *memFile) Close() error               { return nil }

// memDirInfo implements fs.FileInfo for the in-memory root directory.
type memDirInfo struct{}

func (memDirInfo) Name() string       { return "." }
func (memDirInfo) Size() int64        { return 0 }
func (memDirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o555 }
func (memDirInfo) ModTime() time.Time { return time.Time{} }
func (memDirInfo) IsDir() bool        { return true }
func (memDirInfo) Sys() any           { return nil }

// memDir is an open handle to the in-memory root directory. It implements
// fs.ReadDirFile so directory reads work through Open in addition to the
// fs.ReadDirFS fast path.
type memDir struct {
	entries []fs.DirEntry
	offset  int
}

func (d *memDir) Stat() (fs.FileInfo, error) { return memDirInfo{}, nil }
func (d *memDir) Close() error               { return nil }

func (d *memDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: ".", Err: errors.New("is a directory")}
}

func (d *memDir) ReadDir(n int) ([]fs.DirEntry, error) {
	remaining := d.entries[d.offset:]
	if n <= 0 {
		d.offset = len(d.entries)
		return remaining, nil
	}
	if len(remaining) == 0 {
		return nil, io.EOF
	}
	if n > len(remaining) {
		n = len(remaining)
	}
	d.offset += n
	return remaining[:n], nil
}

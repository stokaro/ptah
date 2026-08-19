// Package fsnapshot captures a read-only, in-memory view of an fs.FS.
package fsnapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"
	"time"
)

// errIsDirectory is the reason a read of a directory fails. The text matches
// what a real filesystem reports through syscall.EISDIR, so a snapshot-backed
// read and an os.DirFS-backed read of the same path render identically.
var errIsDirectory = errors.New("is a directory")

// Snapshot is an immutable in-memory filesystem. Its read methods return
// independent values, and Clone returns an independent filesystem view.
//
// capturedDirs records directory paths observed in their own right, as opposed
// to the ones buildDirectoryIndex synthesizes from the paths of captured files.
// Without it a snapshot could only ever answer "which files are here", and a
// directory holding no captured file simply vanished — which is how a directory
// named 2_evil.sql, created after Atlas CE hashed a clean directory, stayed
// invisible to every verb that verifies a snapshot rather than the live
// directory (stokaro/ptah#991). The community binary refuses such a directory;
// Ptah applied, and reported status, set a version and linted it clean.
type Snapshot struct {
	files        map[string][]byte
	capturedDirs map[string]struct{}
	directories  map[string][]fs.DirEntry
}

// FromFiles returns an immutable snapshot containing files. Every path must be
// valid for io/fs, and both the map and file contents are cloned.
func FromFiles(files map[string][]byte) (Snapshot, error) {
	if err := validatePaths(files, nil); err != nil {
		return Snapshot{}, err
	}
	cloned := make(map[string][]byte, len(files))
	for name, contents := range files {
		cloned[name] = slices.Clone(contents)
	}
	return snapshotFromValidatedFiles(cloned, nil), nil
}

// TakeFiles returns an immutable snapshot by taking exclusive ownership of
// files and their byte slices. The caller must not retain or mutate them after
// this call. Use FromFiles when ownership cannot be transferred.
func TakeFiles(files map[string][]byte) (Snapshot, error) {
	if err := validatePaths(files, nil); err != nil {
		return Snapshot{}, err
	}
	return snapshotFromValidatedFiles(files, nil), nil
}

// validatePaths rejects paths io/fs cannot address and any pair where one entry
// would have to live inside a file. A recorded directory that also names a file
// is the same contradiction seen from the other side, so it is rejected too.
func validatePaths(files map[string][]byte, dirs map[string]struct{}) error {
	containedByFile := func(name string) error {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, exists := files[parent]; exists {
				return fmt.Errorf(
					"invalid snapshot file paths %q and %q: a file cannot contain another file",
					parent,
					name,
				)
			}
		}
		return nil
	}
	for name := range files {
		if !fs.ValidPath(name) || name == "." {
			return fmt.Errorf("invalid snapshot file path %q", name)
		}
		if err := containedByFile(name); err != nil {
			return err
		}
	}
	for name := range dirs {
		if !fs.ValidPath(name) || name == "." {
			return fmt.Errorf("invalid snapshot directory path %q", name)
		}
		if _, exists := files[name]; exists {
			return fmt.Errorf("invalid snapshot path %q: recorded as both a file and a directory", name)
		}
		if err := containedByFile(name); err != nil {
			return err
		}
	}
	return nil
}

// Capture reads every file in fsys exactly once and returns an immutable
// snapshot. Capturing an existing Snapshot only clones its in-memory contents.
func Capture(fsys fs.FS) (Snapshot, error) {
	if snapshot, ok := fsys.(Snapshot); ok {
		return snapshot.Clone(), nil
	}
	return CaptureMatching(fsys, func(string, fs.DirEntry) bool {
		return true
	})
}

// CaptureMatching reads each file accepted by include exactly once, and records
// each DIRECTORY accepted by include as a directory in its own right.
//
// include therefore sees every entry, not only the files, and its fs.DirEntry
// argument is what distinguishes them. A directory is still descended into
// whether or not include accepts it, so accepting one costs a map key and never
// changes which files are captured.
//
// Recording directories is what lets a snapshot answer "this name exists and is
// not a file". A caller that selects entries by name — an Atlas integrity file
// covers whatever its per-format glob matches — must be able to reach that
// answer, or a directory whose name matches disappears between the capture and
// the check and the verb accepts a directory the community binary refuses
// (stokaro/ptah#991).
func CaptureMatching(
	fsys fs.FS,
	include func(name string, entry fs.DirEntry) bool,
) (Snapshot, error) {
	if snapshot, ok := fsys.(Snapshot); ok {
		return snapshot.matching(include), nil
	}
	files := make(map[string][]byte)
	dirs := make(map[string]struct{})
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// The root is the snapshot itself and has no entry to record.
		if name == "." || !include(name, entry) {
			return nil
		}
		if entry.IsDir() {
			dirs[name] = struct{}{}
			return nil
		}
		contents, err := fs.ReadFile(fsys, name)
		if err != nil {
			return fmt.Errorf("read snapshot file %s: %w", name, err)
		}
		files[name] = slices.Clone(contents)
		return nil
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("capture filesystem snapshot: %w", err)
	}
	if err := validatePaths(files, dirs); err != nil {
		return Snapshot{}, fmt.Errorf("capture filesystem snapshot: %w", err)
	}
	return snapshotFromValidatedFiles(files, dirs), nil
}

// Clone returns an independent view of the same captured files.
func (s Snapshot) Clone() Snapshot {
	cloned := Snapshot{
		files:        make(map[string][]byte, len(s.files)),
		capturedDirs: s.capturedDirs,
		directories:  s.directories,
	}
	maps.Copy(cloned.files, s.files)
	return cloned
}

// Equal reports whether both snapshots contain exactly the same paths and
// bytes, recorded directories included.
//
// Directories count because CaptureStable compares two captures to detect a
// directory changing underneath it. A directory appearing or disappearing
// between them is exactly such a change, and ignoring it would let the very
// entry #991 is about slip through the window the comparison exists to close.
func (s Snapshot) Equal(other Snapshot) bool {
	return maps.EqualFunc(s.files, other.files, bytes.Equal) &&
		maps.Equal(s.capturedDirs, other.capturedDirs)
}

// WithFiles returns a new immutable snapshot with files added or replaced.
// Paths and contents are validated and cloned; the receiver is unchanged.
func (s Snapshot) WithFiles(files map[string][]byte) (Snapshot, error) {
	combined := make(map[string][]byte, len(s.files)+len(files))
	maps.Copy(combined, s.files)
	for name, contents := range files {
		combined[name] = slices.Clone(contents)
	}
	if err := validatePaths(combined, s.capturedDirs); err != nil {
		return Snapshot{}, err
	}
	return snapshotFromValidatedFiles(combined, s.capturedDirs), nil
}

func (s Snapshot) matching(include func(name string, entry fs.DirEntry) bool) Snapshot {
	filtered := make(map[string][]byte, len(s.files))
	for _, name := range slices.Sorted(maps.Keys(s.files)) {
		contents := s.files[name]
		entry := snapshotFileInfo{name: path.Base(name), size: int64(len(contents))}
		if include(name, entry) {
			filtered[name] = contents
		}
	}
	filteredDirs := make(map[string]struct{}, len(s.capturedDirs))
	for _, name := range slices.Sorted(maps.Keys(s.capturedDirs)) {
		if include(name, snapshotFileInfo{name: path.Base(name), directory: true}) {
			filteredDirs[name] = struct{}{}
		}
	}
	return snapshotFromValidatedFiles(filtered, filteredDirs)
}

// Open implements fs.FS.
func (s Snapshot) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if contents, ok := s.files[name]; ok {
		return &snapshotFile{
			Reader: bytes.NewReader(contents),
			info:   snapshotFileInfo{name: path.Base(name), size: int64(len(contents))},
		}, nil
	}
	entries, ok := s.lookupDirectory(name)
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &snapshotDirectory{
		info:    snapshotFileInfo{name: path.Base(name), directory: true},
		entries: entries,
	}, nil
}

// ReadFile implements fs.ReadFileFS.
//
// Reading a directory reports "is a directory" rather than "file does not
// exist", matching what os.DirFS answers for the same path. The distinction is
// load-bearing: a caller that selects entries by name and then reads them uses
// the read failure to decide whether the entry was a migration, and the wrong
// diagnostic sent Ptah's #991 refusal out as `read 2_evil.sql: file does not
// exist` on a path the filesystem could see perfectly well.
func (s Snapshot) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrInvalid}
	}
	contents, ok := s.files[name]
	if !ok {
		if s.hasDirectory(name) {
			return nil, &fs.PathError{Op: "read", Path: name, Err: errIsDirectory}
		}
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	return slices.Clone(contents), nil
}

// ReadDir implements fs.ReadDirFS.
func (s Snapshot) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	entries, ok := s.lookupDirectory(name)
	if !ok {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	return slices.Clone(entries), nil
}

// Stat implements fs.StatFS.
func (s Snapshot) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	if contents, ok := s.files[name]; ok {
		return snapshotFileInfo{name: path.Base(name), size: int64(len(contents))}, nil
	}
	if s.hasDirectory(name) {
		return snapshotFileInfo{name: path.Base(name), directory: true}, nil
	}
	return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
}

func snapshotFromValidatedFiles(files map[string][]byte, dirs map[string]struct{}) Snapshot {
	return Snapshot{
		files:        files,
		capturedDirs: dirs,
		directories:  buildDirectoryIndex(files, dirs),
	}
}

func buildDirectoryIndex(files map[string][]byte, dirs map[string]struct{}) map[string][]fs.DirEntry {
	infos := map[string]map[string]snapshotFileInfo{
		".": {},
	}
	for filename, contents := range files {
		parent := path.Dir(filename)
		addDirectoryInfo(infos, parent, snapshotFileInfo{
			name: path.Base(filename),
			size: int64(len(contents)),
		})
		for directory := parent; directory != "."; directory = path.Dir(directory) {
			addDirectoryInfo(infos, path.Dir(directory), snapshotFileInfo{
				name:      path.Base(directory),
				directory: true,
			})
		}
	}
	// A recorded directory needs an (possibly empty) entry list of its own, or
	// it would only exist as long as something below it does.
	for directory := range dirs {
		ensureDirectory(infos, directory)
		for current := directory; current != "."; current = path.Dir(current) {
			addDirectoryInfo(infos, path.Dir(current), snapshotFileInfo{
				name:      path.Base(current),
				directory: true,
			})
		}
	}

	index := make(map[string][]fs.DirEntry, len(infos))
	for directory, directoryInfos := range infos {
		entries := make([]fs.DirEntry, 0, len(directoryInfos))
		for _, info := range directoryInfos {
			entries = append(entries, info)
		}
		slices.SortFunc(entries, func(a, b fs.DirEntry) int {
			return strings.Compare(a.Name(), b.Name())
		})
		index[directory] = entries
	}
	return index
}

func addDirectoryInfo(
	directories map[string]map[string]snapshotFileInfo,
	directory string,
	info snapshotFileInfo,
) {
	ensureDirectory(directories, directory)[info.name] = info
}

// ensureDirectory returns directory's entry map, creating an empty one when it
// has no entries yet. It never replaces an existing map, so recording a
// directory that already holds captured files keeps them.
func ensureDirectory(
	directories map[string]map[string]snapshotFileInfo,
	directory string,
) map[string]snapshotFileInfo {
	entries, ok := directories[directory]
	if !ok {
		entries = make(map[string]snapshotFileInfo)
		directories[directory] = entries
	}
	return entries
}

func (s Snapshot) lookupDirectory(name string) ([]fs.DirEntry, bool) {
	entries, ok := s.directories[name]
	if name == "." {
		return entries, true
	}
	return entries, ok
}

func (s Snapshot) hasDirectory(name string) bool {
	if name == "." {
		return true
	}
	_, ok := s.directories[name]
	return ok
}

type snapshotFile struct {
	*bytes.Reader
	info snapshotFileInfo
}

func (f *snapshotFile) Stat() (fs.FileInfo, error) {
	return f.info, nil
}

func (f *snapshotFile) Close() error {
	return nil
}

type snapshotDirectory struct {
	info    snapshotFileInfo
	entries []fs.DirEntry
	offset  int
}

func (d *snapshotDirectory) Stat() (fs.FileInfo, error) {
	return d.info, nil
}

func (d *snapshotDirectory) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.name, Err: errIsDirectory}
}

func (d *snapshotDirectory) Close() error {
	return nil
}

func (d *snapshotDirectory) ReadDir(count int) ([]fs.DirEntry, error) {
	if d.offset >= len(d.entries) && count > 0 {
		return nil, io.EOF
	}
	end := len(d.entries)
	if count > 0 {
		end = min(d.offset+count, end)
	}
	entries := slices.Clone(d.entries[d.offset:end])
	d.offset = end
	return entries, nil
}

type snapshotFileInfo struct {
	name      string
	size      int64
	directory bool
}

func (i snapshotFileInfo) Name() string {
	return i.name
}

func (i snapshotFileInfo) Size() int64 {
	return i.size
}

func (i snapshotFileInfo) Mode() fs.FileMode {
	if i.directory {
		return fs.ModeDir | 0o555
	}
	return 0o444
}

func (i snapshotFileInfo) ModTime() time.Time {
	return time.Time{}
}

func (i snapshotFileInfo) IsDir() bool {
	return i.directory
}

func (i snapshotFileInfo) Sys() any {
	return nil
}

func (i snapshotFileInfo) Type() fs.FileMode {
	return i.Mode().Type()
}

func (i snapshotFileInfo) Info() (fs.FileInfo, error) {
	return i, nil
}

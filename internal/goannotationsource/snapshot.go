// Package goannotationsource captures the Go files that define one annotation
// parsing and cleanup operation.
package goannotationsource

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/pathguard"
)

var (
	// ErrChanged reports that the selected Go source set, file identity, mode,
	// or contents no longer match a captured snapshot.
	ErrChanged = errors.New("Go annotation sources changed after snapshot")
)

// File is one regular, non-test Go source selected for annotation processing.
// Contents returns from Snapshot.Files are independent copies.
type File struct {
	Path         string
	RelativePath string
	Contents     []byte
	info         fs.FileInfo
}

// Mode returns the captured source mode.
func (f File) Mode() fs.FileMode {
	return f.info.Mode()
}

// SameFile reports whether info identifies the captured filesystem object.
func (f File) SameFile(info fs.FileInfo) bool {
	return os.SameFile(f.info, info)
}

// Snapshot is an immutable source-set view backed by a read-only in-memory
// filesystem and the host metadata needed for safe publication and cleanup.
type Snapshot struct {
	root       string
	rootInfo   fs.FileInfo
	files      []File
	filesystem fsnapshot.Snapshot
}

// Capture selects and reads every Go file eligible for annotation processing
// under root, including files that currently contain no annotations.
func Capture(root string) (*Snapshot, error) {
	if root == "" {
		root = "."
	}
	openedRoot, err := pathguard.OpenDirectory(root)
	if err != nil {
		return nil, fmt.Errorf("open Go annotation source root: %w", err)
	}
	snapshot, captureErr := captureOpenedRoot(openedRoot)
	closeErr := openedRoot.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close Go annotation source root: %w", closeErr)
	}
	if err := errors.Join(captureErr, closeErr); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// IsSource reports whether path belongs to the Go annotation source set.
func IsSource(path string) bool {
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

// SkipDirectory reports whether a directory is outside the Go annotation
// source set.
func SkipDirectory(name string) bool {
	return name == "vendor" || strings.HasPrefix(name, ".")
}

// Root returns the absolute source root captured by the snapshot.
func (s *Snapshot) Root() string {
	return s.root
}

// FS returns an independent immutable filesystem view.
func (s *Snapshot) FS() fs.FS {
	return s.filesystem.Clone()
}

// Files returns the selected files in stable relative-path order.
func (s *Snapshot) Files() []File {
	files := make([]File, len(s.files))
	for i, file := range s.files {
		files[i] = file
		files[i].Contents = slices.Clone(file.Contents)
	}
	return files
}

// SourceAlias returns the captured source aliased by path, or an empty string
// when path does not refer to a selected source.
func (s *Snapshot) SourceAlias(path string) (string, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve potential Go source alias %s: %w", path, err)
	}
	absolutePath = filepath.Clean(absolutePath)
	for _, file := range s.files {
		if absolutePath == file.Path {
			return file.Path, nil
		}
	}

	info, err := os.Stat(absolutePath)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("stat potential Go source alias %s: %w", absolutePath, err)
	}
	for _, file := range s.files {
		if file.SameFile(info) {
			return file.Path, nil
		}
	}
	return "", nil
}

// Revalidate verifies that source membership, identity, mode, and contents
// still match the captured snapshot.
func (s *Snapshot) Revalidate() error {
	current, err := Capture(s.root)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrChanged, err)
	}
	if !os.SameFile(s.rootInfo, current.rootInfo) {
		return fmt.Errorf("%w: source root identity changed: %s", ErrChanged, s.root)
	}
	if s.rootInfo.Mode() != current.rootInfo.Mode() {
		return fmt.Errorf("%w: source root mode changed: %s", ErrChanged, s.root)
	}
	if len(current.files) != len(s.files) {
		return fmt.Errorf(
			"%w: source count changed from %d to %d",
			ErrChanged,
			len(s.files),
			len(current.files),
		)
	}
	for i, expected := range s.files {
		actual := current.files[i]
		if actual.RelativePath != expected.RelativePath {
			return fmt.Errorf(
				"%w: source path %q changed to %q",
				ErrChanged,
				expected.RelativePath,
				actual.RelativePath,
			)
		}
		if !expected.SameFile(actual.info) {
			return fmt.Errorf(
				"%w: source identity changed: %s",
				ErrChanged,
				expected.Path,
			)
		}
		if actual.Mode() != expected.Mode() {
			return fmt.Errorf(
				"%w: source mode changed: %s",
				ErrChanged,
				expected.Path,
			)
		}
		if !bytes.Equal(actual.Contents, expected.Contents) {
			return fmt.Errorf(
				"%w: source contents changed: %s",
				ErrChanged,
				expected.Path,
			)
		}
	}
	return nil
}

func captureOpenedRoot(root *pathguard.OpenedDirectory) (*Snapshot, error) {
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("stat Go annotation source root: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("Go annotation source root is not a directory: %s", root.Path())
	}

	files := make([]File, 0)
	contents := make(map[string][]byte)
	fsys := root.FS()
	err = fs.WalkDir(fsys, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != "." && SkipDirectory(entry.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !IsSource(path) {
			return nil
		}

		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("refuse to capture symlinked Go source %s", path)
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat Go source %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refuse to capture non-regular Go source %s", path)
		}
		info, data, err := captureFile(fsys, path, info)
		if err != nil {
			return err
		}
		relativePath := filepath.ToSlash(path)
		files = append(files, File{
			Path:         filepath.Join(root.Path(), filepath.FromSlash(relativePath)),
			RelativePath: relativePath,
			Contents:     data,
			info:         info,
		})
		contents[relativePath] = data
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("capture Go annotation sources: %w", err)
	}
	slices.SortFunc(files, func(a, b File) int {
		return strings.Compare(a.RelativePath, b.RelativePath)
	})
	filesystem, err := fsnapshot.TakeFiles(contents)
	if err != nil {
		return nil, fmt.Errorf("build Go annotation source filesystem: %w", err)
	}
	return &Snapshot{
		root:       root.Path(),
		rootInfo:   rootInfo,
		files:      files,
		filesystem: filesystem,
	}, nil
}

func captureFile(fsys fs.FS, path string, info fs.FileInfo) (fs.FileInfo, []byte, error) {
	file, err := fsys.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open Go source %s: %w", path, err)
	}
	defer file.Close()

	openedInfo, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat opened Go source %s: %w", path, err)
	}
	if !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("%w: source identity changed: %s", ErrChanged, path)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("read Go source %s: %w", path, err)
	}
	finalInfo, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("restat opened Go source %s: %w", path, err)
	}
	if !os.SameFile(openedInfo, finalInfo) ||
		openedInfo.Mode() != finalInfo.Mode() ||
		finalInfo.Size() != int64(len(data)) {
		return nil, nil, fmt.Errorf("%w: source changed while reading: %s", ErrChanged, path)
	}
	return finalInfo, data, nil
}

package atlasmigrate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/pathguard"
)

// migrationWriterDir is the rooted migration-directory capability one
// publication transaction runs through, from the snapshot it verifies to the
// atlas.sum it commits.
//
// It holds two handles rather than one because the transaction touches two
// directories. The migration directory owns the staged files, the published
// migrations and atlas.sum; its parent owns the publication journal and the
// commit marker, which deliberately sit beside the directory so an interrupted
// run stays recoverable even when the directory itself was left half-built.
//
// Both are opened once, before anything is staged, and every later step names
// a direct child of one of them. That is the whole point: a pathname resolved
// again after validation can select a different filesystem object, so once
// these handles exist no write or verification step may reopen the directory by
// pathname (stokaro/ptah#1118).
//
// When root is supplied the two opens go through it, so a project-config
// relative migration directory cannot leave the opened project root even if the
// directory or one of its ancestors is replaced by a symlink pointing out of it
// after the path was resolved. A nil root keeps the direct-CLI behavior, where
// an explicit absolute --dir is the operator's own choice of destination.
type migrationWriterDir struct {
	parent *pathguard.OpenedDirectory
	// dir is nil when the migration directory does not exist. Recovery runs on
	// that shape, because the journal lives beside the directory and outlives it.
	dir *pathguard.OpenedDirectory
	// name is the migration directory's entry name inside parent, resolved the
	// same way the cross-process lock resolves it so both name the same object.
	name string
	// path is the pathname the caller selected, kept verbatim for the paths this
	// run reports back. Nothing resolves it again.
	path string
}

// Path returns the stable lexical path selected for the migration directory.
// It is display and result data only; no write step resolves it again.
func (w *migrationWriterDir) Path() string {
	return w.path
}

// FS returns the escape-resistant filesystem rooted at the migration directory.
func (w *migrationWriterDir) FS() (fs.FS, error) {
	if w.dir == nil {
		return nil, w.missingDirError()
	}
	return w.dir.FS(), nil
}

// Exists reports whether the migration directory was present when the handles
// were bound.
func (w *migrationWriterDir) Exists() bool {
	return w.dir != nil
}

func (w *migrationWriterDir) missingDirError() error {
	return fmt.Errorf("migration directory %s: %w", w.path, fs.ErrNotExist)
}

// Close releases both handles.
func (w *migrationWriterDir) Close() error {
	if w == nil {
		return nil
	}
	var dirErr error
	if w.dir != nil {
		dirErr = w.dir.Close()
	}
	return errors.Join(dirErr, w.parent.Close())
}

// writerDirBinding selects whether binding the migration directory may create
// it. It is a named type rather than a bool so the two call sites read as the
// operations they are at the call site instead of as a naked true/false.
type writerDirBinding int

const (
	// bindExistingWriterDir opens what is already there and reports a missing
	// migration directory through migrationWriterDir.Exists.
	bindExistingWriterDir writerDirBinding = iota
	// createWriterDir materializes a missing migration directory and its parents
	// through the selected root.
	createWriterDir
)

func (b writerDirBinding) creates() bool {
	return b == createWriterDir
}

// openMigrationWriterDir binds the migration directory and its parent without
// creating either. A migration directory that does not exist yields a handle
// whose Exists reports false rather than an error, because recovery has to run
// on a directory an interrupted run never finished creating.
func openMigrationWriterDir(
	root *pathguard.OpenedDirectory,
	dir string,
) (*migrationWriterDir, error) {
	return bindMigrationWriterDir(root, dir, bindExistingWriterDir)
}

// createMigrationWriterDir binds the migration directory and its parent,
// creating the directory through the parent handle when it is missing. The
// creation goes through the same rooted boundary as every later write, so a
// missing directory is materialized inside the opened root rather than wherever
// the pathname happens to point.
func createMigrationWriterDir(
	root *pathguard.OpenedDirectory,
	dir string,
) (*migrationWriterDir, error) {
	return bindMigrationWriterDir(root, dir, createWriterDir)
}

func bindMigrationWriterDir(
	root *pathguard.OpenedDirectory,
	dir string,
	binding writerDirBinding,
) (*migrationWriterDir, error) {
	display, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve migration directory: %w", err)
	}
	canonical, err := canonicalMigrationDir(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve migration directory: %w", err)
	}
	parent, err := openMigrationWriterParent(root, filepath.Dir(canonical), binding)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(canonical)
	writer := &migrationWriterDir{parent: parent, name: name, path: filepath.Clean(display)}
	if binding.creates() {
		if err := parent.Mkdir(name, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, errors.Join(fmt.Errorf("create migration directory: %w", err), parent.Close())
		}
	}
	opened, err := parent.OpenDirectory(name)
	if errors.Is(err, fs.ErrNotExist) {
		return writer, nil
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open migration directory: %w", err), parent.Close())
	}
	writer.dir = opened
	return writer, nil
}

func openMigrationWriterParent(
	root *pathguard.OpenedDirectory,
	parentPath string,
	binding writerDirBinding,
) (*pathguard.OpenedDirectory, error) {
	if root == nil {
		if binding.creates() {
			if err := os.MkdirAll(parentPath, 0o755); err != nil {
				return nil, fmt.Errorf("create migration directory parent: %w", err)
			}
		}
		opened, err := pathguard.OpenDirectory(parentPath)
		if err != nil {
			return nil, fmt.Errorf("open migration directory parent: %w", err)
		}
		return opened, nil
	}
	if binding.creates() {
		if err := rootMkdirAll(root, parentPath); err != nil {
			return nil, err
		}
	}
	opened, err := root.OpenDirectory(parentPath)
	if err != nil {
		return nil, fmt.Errorf("open migration directory parent: %w", err)
	}
	return opened, nil
}

// ensureMigrationDirParent creates the migration directory's parent through the
// selected root before the cross-process lock file is created beside it.
func ensureMigrationDirParent(root *pathguard.OpenedDirectory, dir string) error {
	parentPath := filepath.Dir(filepath.Clean(dir))
	if root == nil {
		if err := os.MkdirAll(parentPath, 0o755); err != nil {
			return fmt.Errorf("create migration directory parent: %w", err)
		}
		return nil
	}
	return rootMkdirAll(root, parentPath)
}

// rootMkdirAll creates path inside root, treating an entry that already resolves
// to a directory as success. os.Root.MkdirAll reports fs.ErrExist for a path
// component that is a symlink to a directory, where the unrooted os.MkdirAll
// this replaced followed it and returned nil; the containment guarantee comes
// from the rooted open that follows, not from the create.
func rootMkdirAll(root *pathguard.OpenedDirectory, path string) error {
	err := root.MkdirAll(path, 0o755)
	if err == nil || errors.Is(err, fs.ErrExist) {
		return nil
	}
	return fmt.Errorf("create migration directory parent: %w", err)
}

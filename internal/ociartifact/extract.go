package ociartifact

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"go.5x5.cz/ptah/internal/fsdurable"
)

// WriteToDir reconstructs the artifact under dir. The destination must not
// exist. Files are staged beside the destination and renamed into place so a
// failed pull never leaves a partial directory.
func (a Artifact) WriteToDir(dir string) error {
	if a.FileSystem == nil {
		return fmt.Errorf("artifact filesystem is required")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve artifact output directory: %w", err)
	}
	parent := filepath.Dir(absolute)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create artifact output parent: %w", err)
	}
	if err := validateAbsentDestination(absolute); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(parent, "."+filepath.Base(absolute)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create artifact staging directory: %w", err)
	}
	defer os.RemoveAll(staging)

	root, err := os.OpenRoot(staging)
	if err != nil {
		return fmt.Errorf("open artifact staging directory: %w", err)
	}
	if err := writeArtifactFiles(root, a.FileSystem); err != nil {
		_ = root.Close()
		return err
	}
	if err := root.Close(); err != nil {
		return fmt.Errorf("close artifact staging directory: %w", err)
	}
	// Not os.Rename: the check above is a look, and what makes it a guarantee
	// is that the install itself refuses a destination that appeared since.
	// os.Rename supplied it on Unix, where it refuses an existing destination
	// of either kind, and not on Windows, where it asks for replacement -- so
	// the guarantee held on two of the three platforms Ptah releases
	// (stokaro/ptah#1547). This refuses on all three, and does it in the move
	// rather than in a check preceding one.
	if err := fsdurable.MoveDirNoReplace(staging, absolute); err != nil {
		return fmt.Errorf("install artifact output directory: %w", err)
	}
	return nil
}

func validateAbsentDestination(dir string) error {
	_, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect artifact output path: %w", err)
	}
	return fmt.Errorf("artifact output path already exists: %s", dir)
}

func writeArtifactFiles(root *os.Root, fsys fs.FS) error {
	return fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if err := validateArtifactPath(name, Limits{}.normalized()); err != nil {
			return err
		}
		contents, err := fs.ReadFile(fsys, name)
		if err != nil {
			return fmt.Errorf("read artifact file %q: %w", name, err)
		}
		if err := root.MkdirAll(path.Dir(name), 0o755); err != nil {
			return fmt.Errorf("create artifact directory for %q: %w", name, err)
		}
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("write artifact file %q: %w", name, err)
		}
		if _, err := file.Write(contents); err != nil {
			_ = file.Close()
			return fmt.Errorf("write artifact file %q: %w", name, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close artifact file %q: %w", name, err)
		}
		return nil
	})
}

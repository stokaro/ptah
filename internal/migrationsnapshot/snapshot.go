// Package migrationsnapshot captures the files that define one migration run.
package migrationsnapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/stokaro/ptah/internal/fsnapshot"
)

var ErrChangedDuringCapture = errors.New("migration directory changed during snapshot capture")

var metadataFiles = map[string]struct{}{
	".ptah-lint.yaml": {},
	"atlas.sum":       {},
	"ptah.sum":        {},
}

// Capture reads SQL migrations and their lint or integrity metadata exactly
// once, excluding unrelated files from the immutable snapshot.
func Capture(fsys fs.FS) (fsnapshot.Snapshot, error) {
	snapshot, err := fsnapshot.CaptureMatching(fsys, func(name string, _ fs.DirEntry) bool {
		if strings.EqualFold(path.Ext(name), ".sql") {
			return true
		}
		_, ok := canonicalMetadataName(path.Base(name))
		return ok
	})
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	if err := validateMetadataNames(snapshot); err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return snapshot, nil
}

// CaptureStable requires two consecutive snapshots to match. This rejects a
// directory that changed while its files were being read.
func CaptureStable(fsys fs.FS) (fsnapshot.Snapshot, error) {
	first, err := Capture(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	second, err := Capture(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	if !first.Equal(second) {
		return fsnapshot.Snapshot{}, ErrChangedDuringCapture
	}
	return second, nil
}

func canonicalMetadataName(name string) (string, bool) {
	for canonical := range metadataFiles {
		if strings.EqualFold(name, canonical) {
			return canonical, true
		}
	}
	return "", false
}

func validateMetadataNames(fsys fs.FS) error {
	return fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		canonical, reserved := canonicalMetadataName(path.Base(name))
		if reserved && path.Base(name) != canonical {
			return fmt.Errorf("migration metadata file %q must use canonical name %q", name, canonical)
		}
		return nil
	})
}

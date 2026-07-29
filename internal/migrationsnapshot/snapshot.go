// Package migrationsnapshot captures the files that define one migration run.
package migrationsnapshot

import (
	"errors"
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
	return fsnapshot.CaptureMatching(fsys, func(name string, _ fs.DirEntry) bool {
		if strings.EqualFold(path.Ext(name), ".sql") {
			return true
		}
		_, ok := metadataFiles[path.Base(name)]
		return ok
	})
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

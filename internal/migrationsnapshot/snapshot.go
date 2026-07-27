// Package migrationsnapshot captures the files that define one migration run.
package migrationsnapshot

import (
	"io/fs"
	"path"
	"strings"

	"github.com/stokaro/ptah/internal/fsnapshot"
)

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

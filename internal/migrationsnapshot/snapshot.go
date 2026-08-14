// Package migrationsnapshot captures the files that define one migration run.
package migrationsnapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"go.5x5.cz/ptah/internal/fsnapshot"
)

var ErrChangedDuringCapture = errors.New("migration directory changed during snapshot capture")

var metadataFiles = map[string]struct{}{
	".ptah-lint.yaml": {},
	"atlas.sum":       {},
	"ptah.sum":        {},
}

// Capture reads SQL migrations and their lint or integrity metadata exactly
// once, excluding unrelated entries from the immutable snapshot.
//
// The predicate deliberately looks at the NAME and ignores the fs.DirEntry, so
// a DIRECTORY called 2_evil.sql is captured as a directory in its own right.
// That is not incidental tidiness: Atlas's integrity file covers whatever its
// glob matches, glob matching is by name, and a snapshot that recorded only
// files let such a directory vanish between the capture and the verification —
// so `migrate apply`, `status`, `set` and `lint` accepted a directory the
// community binary refuses (stokaro/ptah#991). Narrowing this to entry.Type()
// would reopen it.
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

// CaptureDirectory captures dir, treating a missing directory as an empty
// migration history. Generators accept an absent output directory and create
// it on publication, so their pre-replay snapshot needs the same zero-history
// meaning without weakening errors for an existing unreadable path.
//
// Use it ONLY where an absent directory is a legitimate starting state. A verb
// that READS a directory it never creates wants [CaptureExistingDirectory]
// instead: for those, "not there" is a typo in the path, and answering it with
// an empty history turns a run that executed nothing into a passing one.
func CaptureDirectory(dir string) (fsnapshot.Snapshot, error) {
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		return fsnapshot.Snapshot{}, nil
	} else if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return Capture(os.DirFS(dir))
}

// CaptureExistingDirectory captures dir and refuses a missing one, naming the
// path it could not read.
//
// It is the capture for every verb that consumes an existing migration history
// — `migrations test`, `baseline` and `checkpoint` — where the directory is an
// input rather than an output. Before this split those verbs opened
// os.DirFS(dir) directly, so a path that was not there failed when the provider
// scanned it; routing them through the generator's tolerant [CaptureDirectory]
// silently converted that failure into a valid empty history, and a
// `migrate_to: latest` step then reported success having run no migrations at
// all.
func CaptureExistingDirectory(dir string) (fsnapshot.Snapshot, error) {
	if err := RequireExistingDirectory(dir); err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return Capture(os.DirFS(dir))
}

// RequireExistingDirectory refuses a migration directory that is not there,
// naming the path it could not read.
//
// It is the same refusal [CaptureExistingDirectory] makes, separated for the
// caller whose directory is only conditionally required: `migrations test`
// gates the directory on every run but reads it only for a case carrying a
// migrate_to step, so it captures tolerantly and asks this once it knows the
// cases it is about to run.
func RequireExistingDirectory(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("read migration directory %s: %w", dir, err)
	}
	return nil
}

// CaptureStable returns the second of two matching snapshots. This is
// best-effort change detection: it rejects only differences observed between
// captures and cannot detect coordinated or ABA changes that make them match.
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

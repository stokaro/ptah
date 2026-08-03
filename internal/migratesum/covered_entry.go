package migratesum

import (
	"errors"
	"fmt"
	"io/fs"
)

// ErrCoveredEntryUnreadable marks a covered entry that exists but cannot be
// read as a migration file. It is distinct from drift and from a missing
// integrity file because the directory cannot be hashed AT ALL: no sum can be
// written, and no recorded sum can be checked against it.
//
// Callers that render Atlas-compatible checksum output match on this to emit
// the checksum guidance preamble, exactly as they do for drift.
var ErrCoveredEntryUnreadable = errors.New("covered migration entry cannot be read")

// coveredDirectoryError reports that an entry the integrity file covers is a
// directory rather than a migration file.
//
// This is a real and reachable shape, not a curiosity. Atlas CE selects the
// covered entries of every non-Flyway layout with a per-format glob, and a glob
// matches names without regard to type, so `mkdir 2_evil.sql` inside an
// already-hashed migration directory puts an unreadable member into the covered
// set. The community binary refuses the whole directory over it; so does Ptah
// (stokaro/ptah#991).
type coveredDirectoryError struct {
	name string
}

// Error names the offending entry, says what it actually is, and says what to
// do about it.
//
// The exit code, the stream layout and the checksum preamble around this
// message match the pinned community binary v1.3.0 exactly; the wording
// deliberately does not. That binary reports
// `sql/migrate: read file "2_evil.sql": read /abs/2_evil.sql: is a directory`,
// which states the fault without naming the fix — and a checksum tool cannot
// tell a directory the author named .sql by accident from a migration file that
// was replaced by a directory, so telling the reader which situation to look
// for is the whole remedy. Nothing in the conformance corpus asserts this
// string byte for byte (checked before choosing), and the text carries strictly
// more than the community binary's does.
func (e coveredDirectoryError) Error() string {
	return fmt.Sprintf(
		"read file %q: is a directory, not a migration file; "+
			"rename it or move it out of the migration directory",
		e.name,
	)
}

func (e coveredDirectoryError) Is(target error) bool {
	return target == ErrCoveredEntryUnreadable
}

// readCoveredEntry reads one covered entry, reporting a directory as the
// dedicated [ErrCoveredEntryUnreadable] failure rather than as a generic read
// error.
//
// The kind is settled by fs.Stat rather than by matching on the read error's
// text, so the answer is the same whether fsys is an os.DirFS over the real
// directory or an [go.5x5.cz/ptah/internal/fsnapshot.Snapshot] captured from it.
// Those two disagreed before #991: the snapshot had no way to record a
// directory holding no captured file, so the same tree read as "file does not
// exist" through one and "is a directory" through the other.
func readCoveredEntry(fsys fs.FS, name string) ([]byte, error) {
	data, err := fs.ReadFile(fsys, name)
	if err == nil {
		return data, nil
	}
	if info, statErr := fs.Stat(fsys, name); statErr == nil && info.IsDir() {
		return nil, coveredDirectoryError{name: name}
	}
	return nil, fmt.Errorf("failed to read %s: %w", name, err)
}

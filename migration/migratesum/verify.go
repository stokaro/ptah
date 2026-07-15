package migratesum

import (
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
)

// ErrSumFileMissing is returned when the migrations directory has no ptah.sum.
// It is distinct so callers can tell "never hashed" apart from "tampered".
var ErrSumFileMissing = errors.New("ptah.sum not found; run `ptah migrate-hash` to create it")

// Result is the outcome of Verify: the lists are empty when the directory
// matches its recorded sum.
type Result struct {
	// Added are migration files present on disk but absent from ptah.sum.
	Added []string
	// Removed are files recorded in ptah.sum but missing on disk.
	Removed []string
	// Changed are files whose content hash no longer matches ptah.sum.
	Changed []string
	// DirHashMismatch is set when the directory hash differs even though the
	// per-file diff is empty (a corrupted or hand-edited sum file).
	DirHashMismatch bool
}

// OK reports whether the directory matches its recorded sum exactly.
func (r *Result) OK() bool {
	return len(r.Added) == 0 && len(r.Removed) == 0 && len(r.Changed) == 0 && !r.DirHashMismatch
}

// Verify recomputes the sum of fsys and compares it against the ptah.sum
// recorded in the same directory. A missing ptah.sum returns
// ErrSumFileMissing; a read/parse failure returns a wrapped error. A drift is
// reported in the Result (not as an error) so callers choose the exit code.
func Verify(fsys fs.FS) (*Result, error) {
	return VerifyWithFormat(fsys, migrator.MigrationDirFormatAuto)
}

// VerifyWithFormat recomputes the sum of fsys using the selected migration
// directory format and compares it against the ptah.sum recorded in the same
// directory.
func VerifyWithFormat(fsys fs.FS, format migrator.MigrationDirFormat) (*Result, error) {
	recordedRaw, err := fs.ReadFile(fsys, FileName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrSumFileMissing
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", FileName, err)
	}
	recorded, err := Parse(recordedRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", FileName, err)
	}

	current, err := ComputeWithFormat(fsys, format)
	if err != nil {
		return nil, err
	}

	return diff(recorded, current), nil
}

// diff compares the recorded sum against the freshly computed one.
func diff(recorded, current *SumFile) *Result {
	recordedByName := make(map[string]string, len(recorded.Entries))
	for _, e := range recorded.Entries {
		recordedByName[e.Name] = e.Hash
	}
	currentByName := make(map[string]string, len(current.Entries))
	for _, e := range current.Entries {
		currentByName[e.Name] = e.Hash
	}

	var res Result
	for _, e := range current.Entries {
		recordedHash, ok := recordedByName[e.Name]
		switch {
		case !ok:
			res.Added = append(res.Added, e.Name)
		case recordedHash != e.Hash:
			res.Changed = append(res.Changed, e.Name)
		}
	}
	for _, e := range recorded.Entries {
		if _, ok := currentByName[e.Name]; !ok {
			res.Removed = append(res.Removed, e.Name)
		}
	}
	sort.Strings(res.Added)
	sort.Strings(res.Removed)
	sort.Strings(res.Changed)

	// Per-file entries match, yet the recorded directory-hash line does not
	// equal the hash recomputed over those entries: the dir line was
	// hand-edited (or the sum file was assembled inconsistently). Reordering
	// entry lines is not flagged here and need not be — the diff is
	// name-keyed and Compute always re-sorts, so order carries no meaning.
	if res.OK() && recorded.DirHash != current.DirHash {
		res.DirHashMismatch = true
	}
	return &res
}

// Describe renders a drift Result as human-readable lines. It returns "" when
// the result is OK.
func (r *Result) Describe() string {
	if r.OK() {
		return ""
	}
	lines := []string{"migration directory does not match ptah.sum:"}
	for _, n := range r.Changed {
		lines = append(lines, "  changed: "+n)
	}
	for _, n := range r.Added {
		lines = append(lines, "  added (not in ptah.sum): "+n)
	}
	for _, n := range r.Removed {
		lines = append(lines, "  removed (still in ptah.sum): "+n)
	}
	if r.DirHashMismatch {
		lines = append(lines, "  directory hash mismatch (ptah.sum was hand-edited)")
	}
	return strings.Join(lines, "\n")
}

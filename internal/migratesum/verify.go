package migratesum

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
)

var (
	// ErrSumFileMissing is returned when the migrations directory has no
	// expected integrity file. It is distinct so callers can tell "never
	// hashed" apart from "tampered".
	ErrSumFileMissing = errors.New("ptah.sum not found; run `ptah migrations hash` to create it")

	// ErrSumFileMalformed identifies an integrity file that cannot be parsed.
	// The concrete error preserves the file name and parser detail.
	ErrSumFileMalformed = errors.New("migration sum file is malformed")
)

// MismatchReason describes why the recorded and computed entry sequences first
// diverge.
type MismatchReason string

const (
	// MismatchReasonAdded means a migration exists on disk but not at the
	// corresponding position in the integrity file.
	MismatchReasonAdded MismatchReason = "added"
	// MismatchReasonEdited means a migration is in the expected position but
	// its content hash differs.
	MismatchReasonEdited MismatchReason = "edited"
	// MismatchReasonRemoved means an integrity-file entry has no matching file.
	MismatchReasonRemoved MismatchReason = "removed"
)

// Mismatch identifies the first integrity-file entry divergence.
type Mismatch struct {
	Line   int
	File   string
	Reason MismatchReason
}

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
	// SumFileName is the integrity file this result was compared against.
	SumFileName string
	mismatch    *Mismatch
}

// OK reports whether the directory matches its recorded sum exactly.
func (r *Result) OK() bool {
	return len(r.Added) == 0 && len(r.Removed) == 0 && len(r.Changed) == 0 && !r.DirHashMismatch
}

// FirstMismatch returns a copy of the first entry-level mismatch, or nil when
// the entry sequence matches.
func (r *Result) FirstMismatch() *Mismatch {
	if r.mismatch == nil {
		return nil
	}
	mismatch := *r.mismatch
	return &mismatch
}

// Verify recomputes the sum of fsys and compares it against the ptah.sum
// recorded in the same directory. A missing ptah.sum returns
// ErrSumFileMissing; a read/parse failure returns a wrapped error. A drift is
// reported in the Result (not as an error) so callers choose the exit code.
func Verify(fsys fs.FS) (*Result, error) {
	return VerifyWithFormat(fsys, migrator.MigrationDirFormatAuto)
}

// VerifyHashed verifies fsys against its integrity file when one exists.
// hashed=false (with a nil Result and nil error) means the directory carries
// no integrity file for the requested format, so callers can enforce
// apply-time verification on hashed directories while leaving unhashed
// directories ungated. When a sum file exists, the result and error are
// exactly those of [VerifyWithFormat].
func VerifyHashed(fsys fs.FS, format migrator.MigrationDirFormat) (result *Result, hashed bool, err error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return nil, false, err
	}

	hashed, err = hasSumFile(fsys, normalized)
	if err != nil {
		return nil, false, err
	}
	if !hashed {
		return nil, false, nil
	}

	result, err = VerifyWithFormat(fsys, normalized)
	return result, true, err
}

func hasSumFile(fsys fs.FS, format migrator.MigrationDirFormat) (bool, error) {
	names := []string{FileName, AtlasFileName}
	if format != migrator.MigrationDirFormatAuto {
		name, err := FileNameForFormat(format)
		if err != nil {
			return false, err
		}
		names = []string{name}
	}
	for _, name := range names {
		present, err := hasFile(fsys, name)
		if err != nil || present {
			return present, err
		}
	}
	return false, nil
}

// VerifyWithFormat recomputes the sum of fsys using the selected migration
// directory format and compares it against the selected integrity file.
func VerifyWithFormat(fsys fs.FS, format migrator.MigrationDirFormat) (*Result, error) {
	name, err := fileNameForVerify(fsys, format)
	if err != nil {
		return nil, err
	}
	recordedRaw, err := fs.ReadFile(fsys, name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, missingSumFileError(name, format)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", name, err)
	}
	recorded, err := Parse(recordedRaw)
	if err != nil {
		return nil, malformedSumFileError{name: name, err: err}
	}

	computeFormat := formatForSumFile(format, name)
	current, err := ComputeWithFormat(fsys, computeFormat)
	if err != nil {
		return nil, err
	}

	result := diff(recorded, current)
	result.SumFileName = name
	return result, nil
}

func formatForSumFile(format migrator.MigrationDirFormat, name string) migrator.MigrationDirFormat {
	if format != migrator.MigrationDirFormatAuto && format != "" {
		return format
	}
	if name == AtlasFileName {
		return migrator.MigrationDirFormatAtlas
	}
	return migrator.MigrationDirFormatPtah
}

func fileNameForVerify(fsys fs.FS, format migrator.MigrationDirFormat) (string, error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return "", err
	}
	if normalized != migrator.MigrationDirFormatAuto {
		return FileNameForFormat(normalized)
	}

	hasPtahSum, err := hasFile(fsys, FileName)
	if err != nil {
		return "", err
	}
	hasAtlasSum, err := hasFile(fsys, AtlasFileName)
	if err != nil {
		return "", err
	}
	switch {
	case hasPtahSum && hasAtlasSum:
		return "", fmt.Errorf("both %s and %s exist; choose --dir-format ptah or --dir-format atlas", FileName, AtlasFileName)
	case hasAtlasSum:
		return AtlasFileName, nil
	default:
		return FileName, nil
	}
}

func missingSumFileError(name string, format migrator.MigrationDirFormat) error {
	if name == FileName {
		return ErrSumFileMissing
	}
	return sumFileMissingError{name: name, format: format}
}

type sumFileMissingError struct {
	name   string
	format migrator.MigrationDirFormat
}

func (e sumFileMissingError) Error() string {
	return fmt.Sprintf("%s not found; run `ptah migrations hash --dir-format %s` to create it", e.name, e.format)
}

func (e sumFileMissingError) Is(target error) bool {
	return target == ErrSumFileMissing
}

type malformedSumFileError struct {
	name string
	err  error
}

func (e malformedSumFileError) Error() string {
	return fmt.Sprintf("failed to parse %s: %v", e.name, e.err)
}

func (e malformedSumFileError) Unwrap() error {
	return e.err
}

func (e malformedSumFileError) Is(target error) bool {
	return target == ErrSumFileMalformed
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
	res.mismatch = firstMismatch(recorded, current)

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

func firstMismatch(recorded, current *SumFile) *Mismatch {
	for i, entry := range recorded.Entries {
		if len(current.Entries) > i && current.Entries[i] == entry {
			continue
		}

		mismatch := &Mismatch{
			Line: i + 2,
			File: entry.Name,
		}
		switch idx := slices.IndexFunc(current.Entries, func(candidate Entry) bool {
			return candidate.Name == entry.Name
		}); {
		case idx < 0 || i >= len(current.Entries):
			mismatch.Reason = MismatchReasonRemoved
		case idx == i:
			mismatch.Reason = MismatchReasonEdited
		default:
			mismatch.File = current.Entries[i].Name
			mismatch.Reason = MismatchReasonAdded
		}
		return mismatch
	}
	if len(current.Entries) > len(recorded.Entries) {
		return &Mismatch{
			Line:   len(recorded.Entries) + 2,
			File:   current.Entries[len(recorded.Entries)].Name,
			Reason: MismatchReasonAdded,
		}
	}
	return nil
}

// Describe renders a drift Result as human-readable lines. It returns "" when
// the result is OK.
func (r *Result) Describe() string {
	if r.OK() {
		return ""
	}
	name := r.SumFileName
	if name == "" {
		name = FileName
	}
	lines := []string{"migration directory does not match " + name + ":"}
	for _, n := range r.Changed {
		lines = append(lines, "  changed: "+n)
	}
	for _, n := range r.Added {
		lines = append(lines, "  added (not in "+name+"): "+n)
	}
	for _, n := range r.Removed {
		lines = append(lines, "  removed (still in "+name+"): "+n)
	}
	if r.DirHashMismatch {
		lines = append(lines, "  directory hash mismatch ("+name+" was hand-edited)")
	}
	return strings.Join(lines, "\n")
}

package migratesum

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/migration/migrator"
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
	return new(*r.mismatch)
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

// VerifyAtlasFiles recomputes the Atlas-format sum of fsys over exactly names,
// in the order given, and compares it against the atlas.sum recorded in the
// same directory. A missing atlas.sum returns an error matching
// ErrSumFileMissing; a malformed one matches ErrSumFileMalformed. Drift is
// reported in the Result rather than as an error, so callers choose the exit
// code.
//
// It is the verification counterpart of [ComputeAtlasFiles]: a directory
// written by another migration tool and read through Atlas's ?format= carries
// an atlas.sum over its own source files, and only the caller knows which of
// those files the selected format covers.
func VerifyAtlasFiles(fsys fs.FS, names []string) (*Result, error) {
	recordedRaw, err := fs.ReadFile(fsys, AtlasFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, sumFileMissingError{
			name:   AtlasFileName,
			format: migrator.MigrationDirFormatAtlas,
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", AtlasFileName, err)
	}
	recorded, err := Parse(recordedRaw)
	if err != nil {
		return nil, malformedSumFileError{name: AtlasFileName, err: err}
	}

	current, err := ComputeAtlasFiles(fsys, names)
	if err != nil {
		return nil, err
	}

	result := diff(recorded, current, atlasDirHash)
	result.SumFileName = AtlasFileName
	return result, nil
}

// VerifyAtlasFilesHashed verifies fsys against its atlas.sum when one exists.
// hashed=false (with a nil Result and nil error) means the directory carries no
// atlas.sum, so callers can enforce verification on hashed directories while
// deciding separately what an unhashed one means. When the file exists, the
// result and error are exactly those of [VerifyAtlasFiles].
func VerifyAtlasFilesHashed(fsys fs.FS, names []string) (result *Result, hashed bool, err error) {
	hashed, err = hasFile(fsys, AtlasFileName)
	if err != nil || !hashed {
		return nil, false, err
	}
	result, err = VerifyAtlasFiles(fsys, names)
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

	result := diff(recorded, current, dirHashForFormat(computeFormat))
	result.SumFileName = name
	return result, nil
}

// dirHashForFormat returns the directory-hash function the selected format's
// [ComputeWithFormat] path uses, so a recorded sum file is re-hashed with the
// same scheme it was written with.
func dirHashForFormat(format migrator.MigrationDirFormat) func([]Entry) string {
	if format == migrator.MigrationDirFormatAtlas {
		return atlasDirHash
	}
	return dirHash
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

// diff compares the recorded sum against the freshly computed one. dirHashOf
// re-derives a directory hash from a list of entries using the scheme the
// caller computed `current` with; it is what lets diff ask whether the recorded
// file agrees with itself as well as with the directory.
func diff(recorded, current *SumFile, dirHashOf func([]Entry) string) *Result {
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

	// The recorded file must first agree with ITSELF: its directory-hash line
	// has to be the hash of the entry lines below it, in the order they are
	// written. Both hash schemes bind order -- the Atlas one chains each entry
	// into a running hash -- so a sum file whose entry lines were reordered no
	// longer hashes to the line it still carries, even though every (name,
	// hash) pair survived the move. Checking only the directory below missed
	// exactly that: the name-keyed diff sees no drift and Compute re-sorts, so
	// the recomputed directory hash matches the stale recorded one and a
	// tampered ordering verified clean (stokaro/ptah#1231 case 4).
	//
	// Measured against the pinned community binary v1.3.0, both shapes exit 1
	// and they differ in what they can say. A reordered sum file whose
	// directory line was NOT updated prints the bare refusal, because no entry
	// can be blamed while the file contradicts itself; one whose directory line
	// WAS recomputed for the new order prints `L2: <file> was added`, the
	// entry-level detail below. Clearing the mismatch here is what reproduces
	// the first shape.
	selfConsistent := recorded.DirHash == dirHashOf(recorded.Entries)
	if !selfConsistent {
		res.DirHashMismatch = true
		res.mismatch = nil
	}
	// Per-file entries match, yet the recorded directory-hash line does not
	// equal the hash recomputed over the directory: the sum file describes a
	// different directory than the one on disk.
	if selfConsistent && res.OK() && recorded.DirHash != current.DirHash {
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

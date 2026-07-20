package migratesum

import (
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
)

// ErrSumFileMissing is returned when the migrations directory has no expected
// integrity file. It is distinct so callers can tell "never hashed" apart from
// "tampered".
var ErrSumFileMissing = errors.New("ptah.sum not found; run `ptah migrations hash` to create it")

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
		return nil, fmt.Errorf("failed to parse %s: %w", name, err)
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

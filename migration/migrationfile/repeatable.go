package migrationfile

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

// RepeatableFilePrefix starts the name of a repeatable migration file in a
// directory read in the Ptah format: R__<identity>.sql.
//
// No versioned file of either format starts with it, because a Ptah version
// starts with ten digits. An Atlas directory reads the same name as the
// one-time Atlas version "R", so a repeatable migration exists only in a
// directory read in the Ptah format.
const RepeatableFilePrefix = "R__"

// RepeatableFile is one repeatable migration file: SQL that runs again
// whenever its content changes, rather than once at a version.
type RepeatableFile struct {
	// Path is the slash-separated path of the file relative to the walked
	// filesystem root. [DiscoverRepeatables] fills it; [ParseRepeatableFileName]
	// leaves it empty.
	Path string
	// Identity is the part of the name between [RepeatableFilePrefix] and the
	// .sql extension. The database records the repeatable under it, so a
	// renamed file is a different repeatable migration.
	Identity string
}

// ErrNotRepeatableFileName reports a file name that does not start with
// [RepeatableFilePrefix] or does not end in .sql, so it does not claim to be a
// repeatable migration at all.
var ErrNotRepeatableFileName = errors.New("not a repeatable migration file name")

// repeatableIdentityRe is the identity grammar: ASCII letters, digits,
// underscores and hyphens. A dot is left out so that a name such as
// R__view.up.sql, which reads as a direction, is refused rather than taken as
// the identity "view.up".
var repeatableIdentityRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// maxRepeatableIdentityLength is the longest identity a repeatable migration
// may carry. The column that records it holds 255 characters.
const maxRepeatableIdentityLength = 255

// ParseRepeatableFileName parses a repeatable migration file name,
// R__<identity>.sql. A name that does not claim the prefix and an extension
// spelled .sql in any case returns an error wrapping
// [ErrNotRepeatableFileName]. A name that claims both and breaks the identity
// grammar returns a different error, so a misspelled repeatable is refused
// rather than ignored. The identity takes ASCII letters, digits, underscores
// and hyphens, at most 255 of them.
func ParseRepeatableFileName(filename string) (*RepeatableFile, error) {
	rest, ok := strings.CutPrefix(filename, RepeatableFilePrefix)
	if !ok || !strings.EqualFold(path.Ext(filename), ".sql") {
		return nil, fmt.Errorf("%q: %w", filename, ErrNotRepeatableFileName)
	}
	identity, ok := strings.CutSuffix(rest, ".sql")
	if !ok {
		return nil, fmt.Errorf("repeatable migration file %q: spell the extension .sql", filename)
	}
	if !repeatableIdentityRe.MatchString(identity) {
		return nil, fmt.Errorf(
			"repeatable migration file %q: the name after %s takes ASCII letters, digits, underscores and hyphens, "+
				"and a repeatable migration has no direction suffix",
			filename, RepeatableFilePrefix)
	}
	if len(identity) > maxRepeatableIdentityLength {
		return nil, fmt.Errorf("repeatable migration file %q: the identity is longer than %d characters",
			filename, maxRepeatableIdentityLength)
	}
	return &RepeatableFile{Identity: identity}, nil
}

// RepeatableFileName returns the file name of the repeatable migration with
// identity: R__<identity>.sql. It does not check the identity; parse the
// result with [ParseRepeatableFileName] to do that.
func RepeatableFileName(identity string) string {
	return RepeatableFilePrefix + identity + ".sql"
}

// DiscoverRepeatables walks fsys and returns the repeatable migration files of
// a directory read in format, ordered by identity in byte order. That order is
// the order a run executes them in.
//
// A directory read in the Ptah format has repeatable migrations: format is
// [DirFormatPtah], or it is [DirFormatAuto] and the directory holds Ptah
// versioned files. Any other directory returns none, because the Atlas format
// reads R__<name>.sql as a one-time version. It reads the files [Discover]
// reads, under the same atlas.sum narrowing, and refuses a misspelled
// repeatable name and two identities that differ only in letter case, which a
// case-insensitive filesystem or collation could not tell apart.
func DiscoverRepeatables(fsys fs.FS, format DirFormat) ([]RepeatableFile, error) {
	discovered, err := discover(fsys, format)
	if err != nil {
		return nil, err
	}
	return discovered.repeatables, nil
}

// collectRepeatables parses the candidate files that claim the repeatable
// prefix. It is called only for a directory read in the Ptah format.
func collectRepeatables(sqlFiles []string) ([]RepeatableFile, error) {
	var repeatables []RepeatableFile
	folded := make(map[string]string)
	for _, p := range sqlFiles {
		parsed, err := ParseRepeatableFileName(path.Base(p))
		if errors.Is(err, ErrNotRepeatableFileName) {
			continue
		}
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(parsed.Identity)
		if previous, ok := folded[key]; ok {
			return nil, fmt.Errorf(
				"repeatable migrations %s and %s carry identities that differ only in letter case", previous, p)
		}
		folded[key] = p
		parsed.Path = p
		repeatables = append(repeatables, *parsed)
	}
	sort.Slice(repeatables, func(i, j int) bool {
		return repeatables[i].Identity < repeatables[j].Identity
	})
	return repeatables, nil
}

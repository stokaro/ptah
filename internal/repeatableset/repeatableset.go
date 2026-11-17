// Package repeatableset names the repeatable migrations of a migration
// directory for the code that selects migration files by name rather than
// running them: migration lint, its file selectors, and the apply gate built
// on it. It answers with [migrationfile.DiscoverRepeatables], the call the
// migrator loads repeatable migrations with, so a file is a repeatable
// migration to lint exactly when `ptah migrations up` runs it as one.
package repeatableset

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"ptah.run/migration/migrationfile"
)

// Paths returns the slash-separated paths, relative to the root of fsys, of
// the repeatable migrations of a directory read in format. It returns none for
// a directory that holds no repeatable migration, which includes every
// directory read in the Atlas format.
//
// A directory in which no SQL file claims the R__ prefix returns none without
// asking the migrator's discovery, so a directory refused for another reason,
// such as one whose files match no format, is left to the caller to report.
// Where a file claims the prefix, the discovery's refusals apply: a misspelled
// repeatable name, and two identities that differ only in letter case.
func Paths(fsys fs.FS, format migrationfile.DirFormat) (map[string]struct{}, error) {
	claimed, err := claimsRepeatable(fsys)
	if err != nil || !claimed {
		return nil, err
	}
	found, err := migrationfile.DiscoverRepeatables(fsys, format)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{}, len(found))
	for _, file := range found {
		paths[file.Path] = struct{}{}
	}
	return paths, nil
}

// errClaimed stops the walk at the first file that claims the prefix.
var errClaimed = errors.New("a file claims the repeatable prefix")

// claimsRepeatable reports whether a SQL file of fsys claims the repeatable
// prefix, whether or not the identity after it is well formed.
func claimsRepeatable(fsys fs.FS) (bool, error) {
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}
		if _, err := migrationfile.ParseRepeatableFileName(path.Base(p)); !errors.Is(err, migrationfile.ErrNotRepeatableFileName) {
			return errClaimed
		}
		return nil
	})
	switch {
	case errors.Is(err, errClaimed):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("scan migration files for repeatable migrations: %w", err)
	default:
		return false, nil
	}
}

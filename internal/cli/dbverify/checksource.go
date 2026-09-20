// Package dbverify implements "ptah db verify", which evaluates release-state
// assertions against an existing database and changes nothing in it.
//
// Ptah could already say that a migration ran and that a schema matches a
// declaration. Neither answers the question a release rests on: did the change
// produce the result it was for. A migration that adds a column and backfills
// it can succeed statement by statement, match the declaration, and report no
// drift, while the backfill predicate was wrong and a subset of rows kept a
// null. Drift compares structure; this verb evaluates requirements about the
// data (stokaro/ptah#3404).
//
// The requirements are written as the `-- +ptah check` directives a migration
// already uses, so versioned migrations and direct schema changes express a
// requirement the same way rather than growing two dialects for one idea.
package dbverify

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"ptah.run/migration/migrator"
)

// checksFileExtension is the only extension a checks directory contributes.
// The directory holds SQL files carrying directives, and a README or a stray
// editor backup beside them must not be parsed as one.
const checksFileExtension = ".sql"

// sourcedCheck is a check together with the file it was read from, so a report
// can name where a requirement is written rather than only what it asserts.
type sourcedCheck struct {
	Check  migrator.Check
	Source string
}

// loadChecks reads every check from path, which is a file or a directory of
// `.sql` files, in a deterministic order: a directory contributes its files
// sorted by name, and each file contributes its checks in the order written.
//
// The order is fixed rather than incidental because the report is read as a
// list, and a list whose order changes between runs cannot be diffed. dialect
// selects the lexer rules the directive scanner needs; it comes from the
// connection, so a checks file is parsed by the rules of the engine it will be
// evaluated against.
func loadChecks(path, dialect string) ([]sourcedCheck, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read checks from %s: %w", path, err)
	}
	if !info.IsDir() {
		return loadChecksFile(path, dialect)
	}
	files, err := checksDirectoryFiles(path)
	if err != nil {
		return nil, err
	}
	var checks []sourcedCheck
	for _, file := range files {
		parsed, err := loadChecksFile(file, dialect)
		if err != nil {
			return nil, err
		}
		checks = append(checks, parsed...)
	}
	return checks, nil
}

func checksDirectoryFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read checks directory %s: %w", directory, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), checksFileExtension) {
			continue
		}
		files = append(files, filepath.Join(directory, entry.Name()))
	}
	slices.Sort(files)
	return files, nil
}

func loadChecksFile(path, dialect string) ([]sourcedCheck, error) {
	source, err := os.ReadFile(path) // #nosec G304 -- the operator named this file
	if err != nil {
		return nil, fmt.Errorf("read checks file %s: %w", path, err)
	}
	parsed, err := migrator.ParseChecks(string(source), dialect)
	if err != nil {
		return nil, fmt.Errorf("checks file %s: %w", path, err)
	}
	checks := make([]sourcedCheck, 0, len(parsed))
	for _, check := range parsed {
		checks = append(checks, sourcedCheck{Check: check, Source: path})
	}
	return checks, nil
}

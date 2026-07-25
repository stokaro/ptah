package importer

import (
	"cmp"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Flyway migration file names, using the default `__` separator between the
// version and the description:
//
//   - V<version>__<description>.sql — a versioned migration (the "up").
//   - U<version>__<description>.sql — an undo migration (the "down" for the
//     versioned migration with the same version).
//   - R__<description>.sql          — a repeatable migration (no version).
//
// The version is one or more numeric parts. Flyway treats `.` and `_` as
// interchangeable version-part separators (V1.1 and V1_1 are both version 1.1),
// so both are accepted; the `__` before the description is never ambiguous
// because every separator must be followed by a digit.
var (
	flywayVersionedRE  = regexp.MustCompile(`^V(\d+(?:[._]\d+)*)__(.+)\.sql$`)
	flywayUndoRE       = regexp.MustCompile(`^U(\d+(?:[._]\d+)*)__(.+)\.sql$`)
	flywayRepeatableRE = regexp.MustCompile(`^R__(.+)\.sql$`)
)

// flywayParser imports Flyway SQL migrations. Flyway encodes direction in the
// file-name prefix (V = up, U = undo/down) rather than in-file markers, matches
// an undo to its versioned migration by version, and supports dotted versions
// (V2.1) and repeatable (R__) migrations that Ptah has no native counterpart for.
type flywayParser struct{}

func (flywayParser) Name() string { return "flyway" }

func (flywayParser) Detect(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if flywayVersionedRE.MatchString(entry.Name()) || flywayRepeatableRE.MatchString(entry.Name()) {
			return true
		}
	}
	return false
}

// flywayVersioned is a parsed V migration awaiting version assignment. comps is
// the version split into numeric parts for ordering; raw is the original version
// string (e.g. "2.1") preserved for traceability.
type flywayVersioned struct {
	comps    []int64
	raw      string
	name     string
	upSQL    string
	fileName string
}

func (flywayParser) Parse(fsys fs.FS) ([]SourceMigration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}

	var versioned []flywayVersioned
	var repeatables []SourceMigration
	undoByVersion := make(map[string]string) // canonical version -> down SQL
	seenVersion := make(map[string]string)   // canonical version -> V file name
	seenUndo := make(map[string]string)      // canonical version -> U file name

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		switch {
		case flywayVersionedRE.MatchString(fileName):
			match := flywayVersionedRE.FindStringSubmatch(fileName)
			comps, err := parseFlywayVersion(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid flyway version in %q: %w", fileName, err)
			}
			canonical := canonicalFlywayVersion(comps)
			if first, ok := seenVersion[canonical]; ok {
				return nil, fmt.Errorf("duplicate flyway version %s (%q and %q)", canonical, first, fileName)
			}
			seenVersion[canonical] = fileName
			up, err := readFlywaySQL(fsys, fileName)
			if err != nil {
				return nil, err
			}
			versioned = append(versioned, flywayVersioned{
				comps:    comps,
				raw:      match[1],
				name:     match[2],
				upSQL:    up,
				fileName: fileName,
			})
		case flywayUndoRE.MatchString(fileName):
			match := flywayUndoRE.FindStringSubmatch(fileName)
			comps, err := parseFlywayVersion(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid flyway version in %q: %w", fileName, err)
			}
			canonical := canonicalFlywayVersion(comps)
			if first, ok := seenUndo[canonical]; ok {
				return nil, fmt.Errorf("duplicate flyway undo version %s (%q and %q)", canonical, first, fileName)
			}
			seenUndo[canonical] = fileName
			down, err := readFlywaySQL(fsys, fileName)
			if err != nil {
				return nil, err
			}
			undoByVersion[canonical] = down
		case flywayRepeatableRE.MatchString(fileName):
			match := flywayRepeatableRE.FindStringSubmatch(fileName)
			up, err := readFlywaySQL(fsys, fileName)
			if err != nil {
				return nil, err
			}
			repeatables = append(repeatables, SourceMigration{
				Name:       match[1],
				UpSQL:      up,
				Repeatable: true,
			})
		default:
			continue // ignore non-migration files (README, .gitkeep, ...)
		}
	}

	if len(versioned) == 0 && len(repeatables) == 0 && len(undoByVersion) == 0 {
		return nil, fmt.Errorf("no flyway migration files (V<version>__<name>.sql or R__<name>.sql) found")
	}

	// Order versioned migrations by their dotted version so sequential
	// reassignment (when needed) preserves Flyway's apply order.
	slices.SortStableFunc(versioned, func(a, b flywayVersioned) int {
		return compareFlywayVersion(a.comps, b.comps)
	})

	migrations := buildFlywayVersioned(versioned, undoByVersion)

	// Every undo must pair with a versioned migration; a leftover undo is a
	// dangling rollback the import cannot place.
	for canonical := range undoByVersion {
		return nil, fmt.Errorf("flyway undo migration for version %s (%q) has no matching versioned migration", canonical, seenUndo[canonical])
	}

	// Flyway applies repeatable migrations after all versioned ones, ordered by
	// description; mirror that so the imported order is stable and faithful.
	slices.SortStableFunc(repeatables, func(a, b SourceMigration) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return append(migrations, repeatables...), nil
}

// buildFlywayVersioned converts sorted Flyway versioned migrations into
// SourceMigrations, pairing each with its undo (down) and consuming matched
// entries from undoByVersion.
//
// Flyway versions are dotted and can exceed Ptah's 10-digit format, so they
// cannot always be used as Ptah versions directly. When every version is a
// single integer that fits, it is preserved; otherwise all versions are
// reassigned to sequential Ptah versions in sorted order, with the original
// version folded into the name so history stays traceable.
func buildFlywayVersioned(versioned []flywayVersioned, undoByVersion map[string]string) []SourceMigration {
	preserve := true
	for _, v := range versioned {
		if len(v.comps) != 1 || v.comps[0] < 1 || v.comps[0] > maxPtahVersion {
			preserve = false
			break
		}
	}

	migrations := make([]SourceMigration, 0, len(versioned))
	for index, v := range versioned {
		version := int64(index + 1)
		name := fmt.Sprintf("v%s_%s", strings.ReplaceAll(v.raw, ".", "_"), v.name)
		if preserve {
			version = v.comps[0]
			name = v.name
		}
		canonical := canonicalFlywayVersion(v.comps)
		down := undoByVersion[canonical]
		delete(undoByVersion, canonical) // mark the undo consumed
		migrations = append(migrations, SourceMigration{
			Version: version,
			Name:    name,
			UpSQL:   v.upSQL,
			DownSQL: down,
		})
	}
	return migrations
}

// readFlywaySQL reads a Flyway migration file and fails on empty content: a
// Flyway migration is plain SQL (no in-file section markers), so an empty file
// is never a valid migration.
func readFlywaySQL(fsys fs.FS, name string) (string, error) {
	content, err := fs.ReadFile(fsys, name)
	if err != nil {
		return "", fmt.Errorf("read %q: %w", name, err)
	}
	sql := strings.TrimSpace(string(content))
	if sql == "" {
		return "", fmt.Errorf("flyway migration %q is empty", name)
	}
	return sql, nil
}

// parseFlywayVersion splits a Flyway version ("2.1.3" or "2_1_3") into numeric
// parts, treating `.` and `_` as equivalent separators.
func parseFlywayVersion(raw string) ([]int64, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == '.' || r == '_' })
	comps := make([]int64, len(parts))
	for i, part := range parts {
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("version part %q: %w", part, err)
		}
		comps[i] = n
	}
	return comps, nil
}

// canonicalFlywayVersion renders parsed version parts back to a canonical dotted
// string (leading zeros stripped), so "1.1" and "01.1" collide as duplicates.
func canonicalFlywayVersion(comps []int64) string {
	parts := make([]string, len(comps))
	for i, c := range comps {
		parts[i] = strconv.FormatInt(c, 10)
	}
	return strings.Join(parts, ".")
}

// compareFlywayVersion orders two dotted versions component-wise, treating a
// shorter version as smaller when it is a prefix of the longer (1.1 < 1.1.1).
func compareFlywayVersion(a, b []int64) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := cmp.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

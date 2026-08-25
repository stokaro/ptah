package importer

import (
	"cmp"
	"fmt"
	"io/fs"
	"path"
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
	// B<version>__<description>.sql is a Flyway baseline, and the callbacks are
	// a fixed set of event names. Both are recognized in order to be declined
	// by name rather than falling through to "unrecognized".
	flywayBaselineRE = regexp.MustCompile(`^B(\d+(?:[._]\d+)*)__(.+)\.sql$`)
	flywayCallbackRE = regexp.MustCompile(
		`^(?:before|after)(?:Migrate|EachMigrate|Repeatables|EachRepeatable|Clean|Info|Validate|Baseline|Undo|EachUndo)` +
			`(?:Error|Applied)?__(.+)\.sql$`)
)

// flywayParser imports Flyway SQL migrations. Flyway encodes direction in the
// file-name prefix (V = up, U = undo/down) rather than in-file markers, matches
// an undo to its versioned migration by version, and supports dotted versions
// (V2.1) and repeatable (R__) migrations that Ptah has no native counterpart for.
type flywayParser struct{}

func (flywayParser) Name() string { return "flyway" }

func (flywayParser) NamePattern() string {
	return "V<version>__<name>.sql, U<version>__<name>.sql or R__<name>.sql"
}

// Detect walks the tree rather than listing the top level, for the same reason
// Parse does: a Flyway project laid out per module or per release keeps its
// migrations below the location root, and that is a Flyway directory.
func (flywayParser) Detect(fsys fs.FS) bool {
	files, err := sourceFiles(fsys)
	if err != nil {
		return false
	}
	for _, file := range files {
		base := path.Base(file)
		if flywayVersionedRE.MatchString(base) || flywayRepeatableRE.MatchString(base) {
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

// Parse walks the source tree rather than reading only its top level.
//
// Flyway's documented contract is that a location is scanned recursively -- all
// migrations in non-hidden directories below the configured locations are picked
// up. Reading only the top level meant a real Flyway project imported missing
// whatever sat below it, and `ptah-compat migrate import` converted the same
// directory differently, with neither verb saying anything about the difference
// (stokaro/ptah#2231).
func (p flywayParser) Parse(fsys fs.FS) (*ParseResult, error) {
	result := &ParseResult{}
	files, err := sourceFiles(fsys)
	if err != nil {
		return nil, err
	}

	var versioned []flywayVersioned
	var repeatables []SourceMigration
	undoByVersion := make(map[string]string) // canonical version -> down SQL
	seenVersion := make(map[string]string)   // canonical version -> V file name
	seenUndo := make(map[string]string)      // canonical version -> U file name

	for _, filePath := range files {
		fileName := path.Base(filePath)
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
			seenVersion[canonical] = filePath
			up, err := readFlywaySQL(fsys, filePath)
			if err != nil {
				return nil, err
			}
			result.consume(filePath)
			versioned = append(versioned, flywayVersioned{
				comps:    comps,
				raw:      match[1],
				name:     match[2],
				upSQL:    up,
				fileName: filePath,
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
			seenUndo[canonical] = filePath
			down, err := readFlywaySQL(fsys, filePath)
			if err != nil {
				return nil, err
			}
			result.consume(filePath)
			undoByVersion[canonical] = down
		case flywayRepeatableRE.MatchString(fileName):
			match := flywayRepeatableRE.FindStringSubmatch(fileName)
			up, err := readFlywaySQL(fsys, filePath)
			if err != nil {
				return nil, err
			}
			result.consume(filePath)
			repeatables = append(repeatables, SourceMigration{
				Name:       match[1],
				UpSQL:      up,
				Repeatable: true,
			})
		case flywayBaselineRE.MatchString(fileName):
			// Ptah models a Flyway baseline on the compat path (decided in
			// stokaro/ptah#1003) and has no native counterpart: a baseline
			// asserts a schema already exists rather than describing a change
			// to make. Naming it is the difference between a decision and a
			// dropped file.
			result.decline(filePath,
				"it is a Flyway baseline, which describes a schema already in place rather than a "+
					"migration to apply; Ptah has no native baseline, so import the schema instead")
		case flywayCallbackRE.MatchString(fileName):
			result.decline(filePath,
				"it is a Flyway callback, which runs around migrations rather than being one")
		default:
			// AccountForSource reports it by name.
			continue
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
	migrations = append(migrations, repeatables...)
	result.Migrations = migrations
	return result, nil
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

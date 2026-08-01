package atlasmigrateimport

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
)

// SumFileNames returns the source file names an Atlas integrity file covers for
// a directory read as format, in the order Atlas hashes them.
//
// The Atlas integrity hash chains file names and contents together, so both the
// membership of this set and its order are part of the resulting atlas.sum. A
// caller that hashes a different set, or the same set in a different order,
// produces a sum Atlas CE never would — which on a verification path means
// refusing a directory Atlas CE hashed and applies.
//
// The rules below were measured against Atlas CE v1.2.0; the corpus in
// testdata/ce-sums holds the oracle's own atlas.sum for 61 directory shapes and
// TestSumFileNamesMatchesAtlasCE reproduces every one of them byte for byte.
//
// Formats differ in which files count and how deep Atlas looks:
//
//   - atlas, goose, dbmate, liquibase: every top-level *.sql file, ordered by
//     name. File names carry no meaning to the hasher, so a non-versioned
//     foo.sql counts while a sibling .go or .xml file does not.
//   - golang-migrate: every top-level *.up.sql file, ordered by name. The down
//     file of a pair is not covered, so editing it is invisible to the
//     integrity check — matching Atlas CE, which never reads it.
//   - flyway: the whole tree, and not in name order. See flywaySumFileNames.
//
// Flyway is the sole format Atlas CE recurses into. Every other format sees
// only the top level, so a migration one directory down is not covered by the
// integrity file at all.
//
// The .sql suffix match is case-sensitive: Atlas CE covers no file named
// 1_init.SQL, and neither does this.
func SumFileNames(fsys fs.FS, format Format) ([]string, error) {
	if fsys == nil {
		return nil, fmt.Errorf("source migration filesystem is required")
	}
	if err := validateExternalFormat(format); err != nil {
		return nil, err
	}

	switch format {
	case FormatFlyway:
		names, err := treeNames(fsys)
		if err != nil {
			return nil, err
		}
		return flywaySumFileNames(names)
	case FormatGolangMigrate:
		names, err := topLevelNames(fsys)
		if err != nil {
			return nil, err
		}
		return filterSuffix(names, ".up.sql"), nil
	case FormatAtlas, FormatGoose, FormatDBMate, FormatLiquibase:
		names, err := topLevelNames(fsys)
		if err != nil {
			return nil, err
		}
		return filterSuffix(names, ".sql"), nil
	default:
		// validateExternalFormat already rejected everything else; this keeps
		// a newly added Format from silently hashing as if it were Atlas.
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
}

func topLevelNames(fsys fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	// fs.ReadDir is documented to sort, but the order decides the sum, so it is
	// established here rather than assumed of every filesystem implementation.
	slices.Sort(names)
	return names, nil
}

func treeNames(fsys fs.FS) ([]string, error) {
	var names []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk migration directory: %w", err)
	}
	slices.Sort(names)
	return names, nil
}

func filterSuffix(names []string, suffix string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, suffix) {
			out = append(out, name)
		}
	}
	return out
}

// flywaySumFileKind is the role Flyway's file-name prefix assigns to a file.
type flywaySumFileKind int

const (
	flywaySumVersioned flywaySumFileKind = iota
	flywaySumBaseline
	flywaySumRepeatable
	flywaySumUndo
)

// flywaySumFile is one parsed Flyway file name. version is the raw token
// between the prefix and the "__" separator; components is that token parsed
// into numbers, or nil when it does not parse.
type flywaySumFile struct {
	name       string
	kind       flywaySumFileKind
	version    string
	components []int
}

// flywaySumFileNames selects and orders the Flyway files an atlas.sum covers.
//
// Measured against Atlas CE v1.2.0:
//
//   - Undo (U) files are never covered. Repeatable (R) files are, and come
//     after every versioned file, ordered by name.
//   - A baseline (B) file squashes history. The baseline is the B file with the
//     highest version, and it is emitted FIRST — not in version order. That is
//     only observable when an unparseable version is present, because otherwise
//     everything that outranks the baseline also sorts after it.
//   - Every other file at or below the baseline version is dropped, including
//     lower baselines.
//   - Surviving files are ordered by version compared NUMERICALLY, component by
//     component, so V1 < V1.5 < V2 < V10.
//
// The two comparisons genuinely differ, which is the trap this function exists
// to encode: the baseline cut compares version tokens as STRINGS, so a baseline
// at V2 drops V10 ("10" < "2") even though V10 sorts after V2 in the output.
// Implementing the cut numerically would keep a file Atlas CE dropped and
// produce a sum that never verifies against the oracle's.
//
// A version token that does not parse as numbers (V__x.sql, Vx__y.sql) is
// covered, sorts before every parseable version, and is exempt from the
// baseline cut whenever it compares greater as a string.
//
// Flyway is also the one format Atlas CE recurses into: a nested file is
// covered under its slash-separated path and ordered by version alongside the
// top-level ones, so zzz/V1__nested.sql precedes V9__top.sql. Combining a
// baseline with subdirectories is refused — see
// ErrFlywayBaselineWithSubdirectories.
func flywaySumFileNames(names []string) ([]string, error) {
	var versioned, repeatable []flywaySumFile
	for _, name := range names {
		file, ok := parseFlywaySumFile(name)
		if !ok {
			continue
		}
		switch file.kind {
		case flywaySumUndo:
			continue
		case flywaySumRepeatable:
			repeatable = append(repeatable, file)
		case flywaySumVersioned, flywaySumBaseline:
			versioned = append(versioned, file)
		}
	}

	// The highest baseline wins; a tie goes to the last name, so two baselines
	// recorded at the same version resolve the way Atlas CE resolves them.
	baseline := -1
	for i, file := range versioned {
		if file.kind == flywaySumBaseline &&
			(baseline < 0 || file.version >= versioned[baseline].version) {
			baseline = i
		}
	}

	if err := checkFlywayBaselineDepth(versioned, repeatable, baseline); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(versioned)+len(repeatable))
	kept := make([]flywaySumFile, 0, len(versioned))
	for i, file := range versioned {
		switch {
		case i == baseline:
			out = append(out, file.name)
		case baseline >= 0 && file.version <= versioned[baseline].version:
			// Squashed by the baseline. Compared as strings, deliberately.
		default:
			kept = append(kept, file)
		}
	}

	// kept is still in name order, so a stable sort leaves files whose versions
	// compare equal — including every unparseable one — ordered by name.
	slices.SortStableFunc(kept, func(a, b flywaySumFile) int {
		return compareFlywaySumVersions(a.components, b.components)
	})
	for _, file := range kept {
		out = append(out, file.name)
	}
	for _, file := range repeatable {
		out = append(out, file.name)
	}
	return out, nil
}

// ErrFlywayBaselineWithSubdirectories reports a Flyway layout whose checksum
// this package declines to compute.
//
// Measured against Atlas CE v1.2.0, the baseline cut behaves differently
// depending on where the baseline and the files it would squash sit in the
// tree, and four probes produced no model that explains all of them: a
// top-level B2 squashes a nested V1, a nested B2 squashes a sibling V1, a
// nested B9 squashes neither of two top-level files, and a nested B5 alongside
// a top-level B2 squashes both a top-level B2 and a top-level V1.
//
// Guessing between those would mean fitting a rule to four points and emitting
// a checksum that may silently disagree with the oracle's — the failure this
// package exists to prevent. Refusing is loud, cannot mis-verify, and affects
// only directories that combine a baseline with subdirectories; every other
// Flyway layout, nested or not, is computed normally.
var ErrFlywayBaselineWithSubdirectories = errors.New(
	"flyway baseline combined with migrations in subdirectories is not supported",
)

func checkFlywayBaselineDepth(versioned, repeatable []flywaySumFile, baseline int) error {
	if baseline < 0 {
		return nil
	}
	for _, file := range slices.Concat(versioned, repeatable) {
		if strings.Contains(file.name, "/") {
			return fmt.Errorf(
				"%w: baseline %s with %s",
				ErrFlywayBaselineWithSubdirectories,
				versioned[baseline].name,
				file.name,
			)
		}
	}
	return nil
}

// parseFlywaySumFile splits a Flyway file name into its prefix and version
// token. A nested file keeps its full slash path as the name while the prefix
// and version come from the base name, matching how Atlas CE covers
// sub/V2__nested.sql.
//
// The only requirements Atlas CE imposes are a .sql suffix and a leading V, B,
// R or U. Neither the "__" separator nor a description nor a parseable version
// is needed, so V1.sql, V1__.sql and V.sql are all covered — and so is any
// ordinary word with that initial: Video.sql is a versioned migration, and
// Backup.sql is a BASELINE, which squashes every versioned migration beneath
// it. Requiring a separator here would drop files the oracle covers.
//
// The match is case-sensitive, unlike the importer's flywayFileRe: Atlas CE
// covers V1__a.sql and ignores v1__a.sql, so reusing that (?i) pattern would
// cover a file the oracle does not.
func parseFlywaySumFile(name string) (flywaySumFile, bool) {
	base, ok := strings.CutSuffix(path.Base(name), ".sql")
	if !ok || base == "" {
		return flywaySumFile{}, false
	}

	var kind flywaySumFileKind
	switch base[0] {
	case 'V':
		kind = flywaySumVersioned
	case 'B':
		kind = flywaySumBaseline
	case 'R':
		kind = flywaySumRepeatable
	case 'U':
		kind = flywaySumUndo
	default:
		return flywaySumFile{}, false
	}

	// The version token runs to the FIRST "__" when there is one, so
	// V1__a__b.sql is version 1 described as "a__b", and covers the whole
	// remainder when there is not.
	version := base[1:]
	if before, _, found := strings.Cut(version, "__"); found {
		version = before
	}
	return flywaySumFile{
		name:       name,
		kind:       kind,
		version:    version,
		components: parseFlywaySumVersion(version),
	}, true
}

// parseFlywaySumVersion parses a Flyway version token into numeric components,
// splitting on Flyway's interchangeable '.' and '_' separators. It returns nil
// for a token that is empty or holds any non-numeric component, which orders
// that file before every parseable version.
func parseFlywaySumVersion(version string) []int {
	parts := strings.FieldsFunc(version, func(r rune) bool { return r == '.' || r == '_' })
	if len(parts) == 0 {
		return nil
	}
	components := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil
		}
		components = append(components, value)
	}
	return components
}

// compareFlywaySumVersions orders version components numerically, treating a
// missing trailing component as zero so V1 sorts before V1.5.
func compareFlywaySumVersions(a, b []int) int {
	for i := range max(len(a), len(b)) {
		var x, y int
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

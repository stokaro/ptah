package atlasmigrateimport

import (
	"cmp"
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
// Every rule here was derived from measurement against the pinned Atlas CE
// v1.2.0 binary, never from reading its source. The corpus in testdata/ce-sums
// holds the oracle's own atlas.sum for each captured shape, and
// TestSumFileNamesDifferentialFuzz compares against the live oracle over
// randomly generated directories.
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
		// Flyway selection is a walk-order state machine, so names must arrive
		// in visit order rather than sorted by path. Hidden directories are
		// pruned: Atlas CE does not descend into them, so covering
		// .archive/V1__old.sql would make Ptah refuse a directory the oracle
		// still considers clean.
		names, err := treeNames(fsys, skipHiddenDir)
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

func skipHiddenDir(base string) bool {
	return strings.HasPrefix(base, ".")
}

// treeNames returns every file in fsys in lexical walk order — the order
// fs.WalkDir visits them, which interleaves a subdirectory's contents at the
// position of the subdirectory itself. The result is deliberately NOT sorted by
// path: sorting would place "sub.sql" before "sub/V1__x.sql" ('.' sorts below
// '/') while a walk visits the directory first, and Flyway selection depends on
// visit order. skipDir, when non-nil, prunes a directory by its base name.
func treeNames(fsys fs.FS, skipDir func(base string) bool) ([]string, error) {
	var names []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name != "." && skipDir != nil && skipDir(path.Base(name)) {
				return fs.SkipDir
			}
			return nil
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk migration directory: %w", err)
	}
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
// between the prefix and the "__" separator; components is that token scored
// into numbers for ordering.
type flywaySumFile struct {
	name       string
	kind       flywaySumFileKind
	version    string
	components []int
}

// flywaySumFileNames selects and orders the Flyway files an atlas.sum covers.
//
// Selection is a state machine over the walk, not a filter over a set — that
// distinction is the whole difficulty, and it is invisible unless
// subdirectories are involved. At the top level Atlas visits names in order, so
// baselines (B) always precede versioned files (V); only a subdirectory can put
// a baseline after a file it would otherwise have squashed.
//
// Walking in visit order, carrying the baseline in force at each step:
//
//   - Undo (U) files are never covered. Repeatable (R) files are, and come
//     after every versioned file.
//   - A baseline (B) supersedes the one in force when its version is greater or
//     equal, which is why the LAST of two equal-versioned baselines wins and a
//     superseded baseline vanishes from the sum entirely.
//   - A versioned file is squashed when a baseline is already in force and its
//     version is not greater. A file visited BEFORE any baseline survives and
//     is never reconsidered when a later baseline appears — so B2, V3, sub/B5
//     keeps V3 even though "3" <= "5".
//   - The surviving baseline is emitted FIRST, ahead of version order.
//
// The two comparisons genuinely differ, which is the trap to remember: the
// squash test compares version tokens as STRINGS, so a baseline at V2 squashes
// V10 ("10" < "2") even though V10 sorts after V2 in the output, which is
// ordered numerically.
func flywaySumFileNames(names []string) ([]string, error) {
	var survivors, repeatable []flywaySumFile
	var baseline *flywaySumFile
	visited := 0

	for _, name := range names {
		file, ok := parseFlywaySumFile(name)
		if !ok {
			continue
		}
		if file.kind == flywaySumBaseline && visited > 0 &&
			flywayVersionIsUnbounded(file.version) {
			return nil, fmt.Errorf("%w: %s", ErrFlywayBaselineVersionNotNumeric, file.name)
		}
		visited++
		switch file.kind {
		case flywaySumUndo:
			continue
		case flywaySumRepeatable:
			repeatable = append(repeatable, file)
		case flywaySumBaseline:
			// A baseline is never squashed by the one in force: it either
			// replaces it or is dropped as superseded history.
			if baseline == nil || file.version >= baseline.version {
				baseline = &file
			}
		case flywaySumVersioned:
			if baseline != nil && file.version <= baseline.version {
				continue
			}
			survivors = append(survivors, file)
		}
	}

	out := make([]string, 0, len(survivors)+len(repeatable)+1)
	if baseline != nil {
		out = append(out, baseline.name)
	}

	// survivors is still in walk order, so a stable sort leaves files whose
	// versions compare equal ordered the way Atlas visited them.
	slices.SortStableFunc(survivors, func(a, b flywaySumFile) int {
		return compareFlywaySumVersions(a.components, b.components)
	})
	for _, file := range survivors {
		out = append(out, file.name)
	}
	for _, file := range repeatable {
		out = append(out, file.name)
	}
	return out, nil
}

// ErrFlywayBaselineVersionNotNumeric reports the one Flyway layout whose
// checksum this package declines to compute: a baseline whose version token has
// a non-numeric component (Bx__base.sql, Bx.5__base.sql) reached after some
// other covered file was already visited.
//
// Everywhere else the selection is a visit-order state machine, validated
// against the oracle over thousands of randomized shapes. That model holds for
// every baseline with a numeric version, at any depth. A non-numeric baseline
// reached mid-walk instead squashes files the walk had already accepted, and no
// model tried so far reproduces it: treating such a baseline as unbounded fixes
// those shapes and breaks others.
//
// Refusing is scoped to a token no ordinary project writes — a Flyway baseline
// is a version number — and it cannot mis-verify. An ordinary baseline layout,
// including one with subdirectories, is computed normally.
var ErrFlywayBaselineVersionNotNumeric = errors.New(
	"flyway baseline version is not numeric and follows another migration",
)

// parseFlywaySumFile splits a Flyway file name into its prefix and version
// token. A nested file keeps its full slash path as the name while the prefix
// and version come from the base name, matching how Atlas CE covers
// sub/V2__nested.sql.
//
// The only requirements Atlas CE imposes are a .sql suffix and a leading V, B,
// R or U. Neither the "__" separator nor a description nor a numeric version is
// needed, so V1.sql, V1__.sql and V.sql are all covered — and so is any
// ordinary word with that initial: Video.sql is a versioned migration, and
// Backup.sql is a BASELINE, which squashes every versioned migration visited
// after it. Requiring a separator here would drop files the oracle covers.
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

// parseFlywaySumVersion scores a Flyway version token into ordering components,
// splitting on Flyway's interchangeable '.' and '_' separators.
//
// Empty components are kept rather than dropped, so "1." scores as {1, 0} and
// ranks after "1" — the component COUNT is significant, which is why a token is
// never zero-extended when compared (see compareFlywaySumVersions).
//
// Every component is scored by what strconv.Atoi returns, and its error is
// deliberately ignored because the returned value is already the right answer
// in each case the oracle exhibits: the exact value when the part is numeric,
// zero when it is not (so "x" ranks as 0 without poisoning its siblings), and
// the clamped MaxInt/MinInt on ErrRange (so a 20-digit timestamp ranks above
// every ordinary version instead of being rejected). Negative parts are kept
// and ordered, so V-5 precedes V-1.
func parseFlywaySumVersion(version string) []int {
	parts := strings.Split(strings.ReplaceAll(version, "_", "."), ".")
	components := make([]int, len(parts))
	for i, part := range parts {
		value, _ := strconv.Atoi(part)
		components[i] = value
	}
	return components
}

// flywayVersionIsUnbounded reports whether any component of a version token
// fails to parse as a number outright, as in Bx__base.sql or Bx.5__base.sql.
//
// A baseline with such a token squashes every versioned file in the directory,
// including files the walk had already accepted, which is the single behavior
// visit order alone does not explain. A numeric baseline — including one that
// overflows, which clamps rather than failing — squashes only what is visited
// after it. Both halves are measured; a value that merely overflows is
// deliberately NOT unbounded, since ErrRange still yields an ordering.
func flywayVersionIsUnbounded(version string) bool {
	for part := range strings.SplitSeq(strings.ReplaceAll(version, "_", "."), ".") {
		if _, err := strconv.Atoi(part); errors.Is(err, strconv.ErrSyntax) {
			return true
		}
	}
	return false
}

// compareFlywaySumVersions orders version components numerically. A shorter
// component list that is a prefix of a longer one ranks FIRST: V1 precedes
// V1.0, and V2 precedes V2.0.0. Zero-extending the shorter list instead would
// make those pairs compare equal and fall through to a name tiebreak, which
// reverses the oracle's order on entirely ordinary file names.
func compareFlywaySumVersions(a, b []int) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return cmp.Compare(a[i], b[i])
		}
	}
	return cmp.Compare(len(a), len(b))
}

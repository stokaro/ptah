package fromschema_test

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// normalizeDialectSource is the file that decides which dialect spellings ptah
// accepts. The spelling list below is read out of it rather than copied here, so
// a spelling added to the switch is covered by this test without anyone editing
// this file.
const normalizeDialectSource = "../../../core/platform/constants.go"

// quotedLiteral deliberately requires a non-empty literal: the switch's default
// arm returns "", which is the one string in the body that is not a spelling.
var quotedLiteral = regexp.MustCompile(`"([^"]+)"`)

// acceptedSpellings returns every dialect spelling that appears as a case in
// platform.NormalizeDialect's switch, read from the switch body itself.
//
// The body holds no string literal other than the case spellings: the argument
// is lowercased through strings helpers and every return is a named constant.
func acceptedSpellings(tb testing.TB) []string {
	c := qt.New(tb)
	source, err := os.ReadFile(normalizeDialectSource)
	c.Assert(err, qt.IsNil)

	_, afterSignature, foundSignature := strings.Cut(string(source), "func NormalizeDialect(dialect string) string {")
	c.Assert(foundSignature, qt.IsTrue, qt.Commentf("NormalizeDialect signature moved in %s", normalizeDialectSource))

	body, _, foundEnd := strings.Cut(afterSignature, "\n}")
	c.Assert(foundEnd, qt.IsTrue, qt.Commentf("NormalizeDialect body is unterminated in %s", normalizeDialectSource))

	matches := quotedLiteral.FindAllStringSubmatch(body, -1)
	spellings := make([]string, 0, len(matches))
	for _, match := range matches {
		spellings = append(spellings, match[1])
	}
	slices.Sort(spellings)
	return slices.Compact(spellings)
}

// convertedStatements is the conversion this test compares: the AST that
// fromschema builds for a dialect spelling, rendered to SQL. A render error is
// folded into the compared string instead of failing the test, so a dialect that
// refuses part of the fixture still contributes a value both spellings of that
// engine must agree on.
func convertedStatements(database goschema.Database, dialect string) []string {
	nodes := fromschema.FromDatabase(database, dialect)
	rendered := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		sql, err := renderer.RenderSQL(dialect, node)
		rendered = append(rendered, fmt.Sprintf("%s | err=%v", sql, err))
	}
	return rendered
}

func spellingFixture(tb testing.TB) goschema.Database {
	c := qt.New(tb)
	database, err := goschema.ParseDir("testdata/dialectspellings")
	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.IsNotNil)
	return *database
}

// TestAcceptedSpellings_ExtractionControls proves the spelling list the parity
// test iterates is really the switch's own list.
//
// Reverting the extraction (a renamed signature, a regexp that stops matching)
// leaves acceptedSpellings empty, and an empty list makes the parity test below
// pass while comparing nothing. This test prints the missing spelling name.
func TestAcceptedSpellings_ExtractionControls(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c.TB)

	// Positive control: aliases that exist only inside the switch, one per
	// engine family that has one.
	for _, alias := range []string{"pgx", "ch", "sqlite3", "tsql", "sql-server", "crdb", "ysql", "google_spanner"} {
		c.Assert(spellings, qt.Contains, alias)
	}
	// Positive control: every canonical name is a case of its own switch.
	for _, canonical := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		c.Assert(spellings, qt.Contains, canonical)
	}
	// Negative control: the extractor must not reach past the switch body. Every
	// literal it collected has to be a spelling NormalizeDialect actually
	// accepts, so a comment word or a neighboring function's literal fails here.
	for _, spelling := range spellings {
		c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "", qt.Commentf("collected %q, which is not an accepted spelling", spelling))
	}
}

// TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName is the
// completion criterion for stokaro/ptah#929 workstream A: one dialect spelling,
// one answer.
//
// If the normalization is reverted — isPostgreSQLPlatform back to its two-string
// form, handleEnumTypes back to comparing the raw name, applyPlatformOverrides
// back to indexing Overrides by the raw name — this fails with the list of
// spellings that disagree with their own canonical name, so the failure is a
// count of engines-times-predicates rather than a single boolean. On the
// measured baseline that list held all 15 non-canonical spellings.
func TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c.TB)
	spellings := acceptedSpellings(c.TB)

	divergent := slices.DeleteFunc(slices.Clone(spellings), func(spelling string) bool {
		return slices.Equal(
			convertedStatements(database, spelling),
			convertedStatements(database, platform.NormalizeDialect(spelling)),
		)
	})

	c.Assert(divergent, qt.HasLen, 0, qt.Commentf("these spellings convert differently from their own canonical name"))
}

// nodeKinds is the sequence of AST node types a conversion produces. It is the
// converter's whole output as far as this comparison cares: which object kinds
// were emitted, in which order. Rendering is deliberately not involved -- see
// the test below for why.
func nodeKinds(database goschema.Database, dialect string) []string {
	nodes := fromschema.FromDatabase(database, dialect)
	kinds := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		kinds = append(kinds, fmt.Sprintf("%T", node))
	}
	return kinds
}

// TestFromDatabase_PostgresFamilyEmitsTheSameObjectKinds is the completion
// criterion for stokaro/ptah#929 items 1 and 4: offline render and live plan
// must answer the same question the same way.
//
// registerBuiltInPlanners routes cockroachdb, yugabytedb and spanner through
// the PostgreSQL planner, so `schema apply --dry-run` planned sequences,
// domains, composites, ranges, roles, grants, RLS, functions, views, matviews
// and triggers for those targets. The offline converter gated all of them on a
// predicate that matched the literal "postgres", so `schema render` dropped
// every one -- with no comment, no warning and exit 0. Measured on the fixture
// used here before the fix: postgres rendered 6 statements, each of the other
// three rendered 1.
//
// The comparison is over node kinds rather than rendered SQL on purpose. Which
// objects the converter emits is its decision; whether an engine accepts one is
// the renderer's, and it already refuses what a preset cannot do -- rendering
// this fixture for cockroachdb now reports `cockroachdb does not support role
// management` instead of silently dropping the role AND everything near it.
// Comparing SQL would fold those two separate answers into one string and make
// this test fail for the renderer's reasons.
func TestFromDatabase_PostgresFamilyEmitsTheSameObjectKinds(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c.TB)
	want := nodeKinds(database, platform.Postgres)

	// The fixture has to carry PostgreSQL object kinds for this to compare
	// anything: a table-only fixture would agree across the family no matter
	// what the predicate did.
	c.Assert(len(want) > 1, qt.IsTrue, qt.Commentf("fixture emits %d nodes for postgres", len(want)))

	for _, dialect := range []string{platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(nodeKinds(database, dialect), qt.DeepEquals, want)
		})
	}
}

// TestFromDatabase_FixtureDiscriminatesEngines is the negative control for the
// parity test above.
//
// The parity comparison is only meaningful if the fixture can tell engines
// apart at all. A fixture that rendered to the same statements everywhere would
// make TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName pass
// no matter what the predicates did.
//
// Emptying the fixture reddens this test. Measured: it then reports one
// survivor per fingerprint rather than the colliding pair, because the map is
// keyed by fingerprint and the last engine written wins — `map[string]string
// {"": "spanner"}` for an empty fixture.
//
// Deleting the per-engine `platform.<name>.<attr>` overrides does NOT redden
// it, and an earlier version of this comment claimed it did. The remaining
// column types and constraints still fingerprint the nine engines apart, so
// the control holds for the reason above rather than because of the overrides.
// What the overrides are load-bearing for is the parity test itself: under a
// raw-index mutant, dropping `platform.sqlite.default` removes sqlite3 from
// its divergent list and swapping `platform.clickhouse.type` removes ch.
func TestFromDatabase_FixtureDiscriminatesEngines(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c.TB)
	canonicals := []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	}

	fingerprints := make(map[string]string, len(canonicals))
	for _, canonical := range canonicals {
		fingerprints[strings.Join(convertedStatements(database, canonical), "\n")] = canonical
	}

	c.Assert(fingerprints, qt.HasLen, len(canonicals))
}

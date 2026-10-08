package modelast_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/modelast"
)

// acceptedSpellings reads the declaration used by built-in normalization and
// registration, so adding an alias automatically extends these sweeps.
func acceptedSpellings(c *qt.C) []string {
	spellings := platform.DialectSpellings()
	slices.Sort(spellings)
	c.Assert(len(spellings) > 9, qt.IsTrue,
		qt.Commentf("only %d spellings, so the sweep is incomplete", len(spellings)))
	return spellings
}

// convertedStatements is the conversion this test compares: the AST that
// modelast lowers a schema for a dialect spelling, rendered to SQL. A render error is
// folded into the compared string instead of failing the test, so a dialect that
// refuses part of the fixture still contributes a value both spellings of that
// engine must agree on.
func convertedStatements(database schemamodel.Database, dialect string) []string {
	nodes := must.Must(modelast.CollectDatabase(database, dialect))
	rendered := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		sql, err := builtin.RenderSQL(dialect, node)
		rendered = append(rendered, fmt.Sprintf("%s | err=%v", sql, err))
	}
	return rendered
}

func spellingFixture(c *qt.C) schemamodel.Database {
	database, err := goschema.ParseDir("testdata/dialectspellings")
	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.IsNotNil)
	return *database
}

// TestAcceptedSpellings_DeclarationControls pins representative aliases and
// canonical names so an incomplete declaration cannot vacate the sweeps.
func TestAcceptedSpellings_DeclarationControls(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c)

	// Positive control: representative aliases from each engine family.
	for _, alias := range []string{"pgx", "ch", "sqlite3", "tsql", "sql-server", "crdb", "ysql", "google_spanner"} {
		c.Assert(spellings, qt.Contains, alias)
	}
	// Positive control: canonical names are accepted spellings too.
	for _, canonical := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		c.Assert(spellings, qt.Contains, canonical)
	}
	// Every enumerated spelling must resolve to a target.
	for _, spelling := range spellings {
		c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "", qt.Commentf("collected %q, which is not an accepted spelling", spelling))
	}
}

// TestCollectDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName is the
// completion criterion for stokaro/ptah#929 workstream A: one dialect spelling,
// one answer.
//
// If the normalization is reverted — isPostgreSQLPlatform back to its two-string
// form, handleEnumTypes back to comparing the raw name, applyPlatformOverrides
// back to indexing Overrides by the raw name — this fails with the list of
// spellings that disagree with their own canonical name, so the failure is a
// count of engines-times-predicates rather than a single boolean. On the
// measured baseline that list held all 15 non-canonical spellings.
func TestCollectDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
	spellings := acceptedSpellings(c)

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
func nodeKinds(database schemamodel.Database, dialect string) []string {
	nodes := must.Must(modelast.CollectDatabase(database, dialect))
	kinds := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		kinds = append(kinds, fmt.Sprintf("%T", node))
	}
	return kinds
}

// TestCollectDatabase_PostgresFamilyEmitsTheSameObjectKinds is the completion
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
func TestCollectDatabase_PostgresFamilyEmitsTheSameObjectKinds(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
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

// TestCollectDatabase_FixtureDiscriminatesEngines is the negative control for the
// parity test above.
//
// The parity comparison is only meaningful if the fixture can tell engines
// apart at all. A fixture that rendered to the same statements everywhere would
// make TestCollectDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName pass
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
func TestCollectDatabase_FixtureDiscriminatesEngines(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
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

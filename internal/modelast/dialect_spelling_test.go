package modelast_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtinlowering"
	"ptah.run/internal/builtintest"
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
// modelast lowers a schema for a dialect spelling, projected onto its engine
// by [scopedTo], rendered to SQL. A render error is
// folded into the compared string instead of failing the test, so a dialect that
// refuses part of the fixture still contributes a value both spellings of that
// engine must agree on.
func convertedStatements(database schemamodel.Database, dialect string) []string {
	nodes := must.Must(modelast.CollectDatabase(scopedTo(database, dialect), dialect, builtinlowering.ForTarget(context.Background(), must.Must(builtin.New()), dialect, capability.ForDialect(dialect))))
	rendered := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		sql, err := builtin.RenderSQL(dialect, node)
		rendered = append(rendered, fmt.Sprintf("%s | err=%v", sql, err))
	}
	return rendered
}

func spellingFixture(c *qt.C) schemamodel.Database {
	database, err := goschema.ParseDir(builtintest.Annotations(), "testdata/dialectspellings")
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

// scopedTo projects database onto dialect's engine, as every render entry
// point does, so an object scoped to another target is left out.
func scopedTo(database schemamodel.Database, dialect string) schemamodel.Database {
	selection := must.Must(schemaext.NewTargetSelection(platform.NormalizeDialect(dialect)))
	return *must.Must(schemamodel.ScopeToTarget(&database, selection))
}

// nodeKinds is the sequence of AST node types a conversion produces. It is the
// converter's whole output as far as this comparison cares: which object kinds
// were emitted, in which order. Rendering is deliberately not involved -- see
// the test below for why.
func nodeKinds(database schemamodel.Database, dialect string) []string {
	nodes := must.Must(modelast.CollectDatabase(scopedTo(database, dialect), dialect,
		builtinlowering.ForTarget(context.Background(), must.Must(builtin.New()), dialect, capability.ForDialect(dialect))))
	kinds := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		kind := fmt.Sprintf("%T", node)
		for _, operation := range ast.OwnerOperations(node) {
			kind += fmt.Sprintf("/%T", operation)
		}
		kinds = append(kinds, kind)
	}
	return kinds
}

// TestCollectDatabase_PostgresFamilyEmitsUnscopedObjectKinds checks that the
// host preserves common declarations across PostgreSQL-family targets. Owner
// declarations honor their explicit target scope; the domain, composite and
// range types target PostgreSQL.
func TestCollectDatabase_PostgresFamilyEmitsUnscopedObjectKinds(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
	want := nodeKinds(database, platform.Postgres)

	// The fixture has to carry PostgreSQL object kinds for this to compare
	// anything: a table-only fixture would agree across the family no matter
	// what the predicate did.
	c.Assert(len(want) > 1, qt.IsTrue, qt.Commentf("fixture emits %d nodes for postgres", len(want)))

	// The owner's type declarations target PostgreSQL. Spanner also
	// represents the enum on its column, as text with a CHECK.
	ownerTypes := []string{"*ast.ExtensionStatement/*pgtype.CreateDomain", "*ast.ExtensionStatement/*pgtype.CreateComposite",
		"*ast.ExtensionStatement/*pgtype.CreateRange"}
	withoutOwnerTypes := slices.DeleteFunc(slices.Clone(want), func(kind string) bool { return slices.Contains(ownerTypes, kind) })
	c.Assert(withoutOwnerTypes, qt.HasLen, len(want)-len(ownerTypes), qt.Commentf("the fixture declares one type of each owner kind"))
	withoutEnumTypes := slices.DeleteFunc(slices.Clone(withoutOwnerTypes), func(kind string) bool { return kind == "*ast.EnumNode" })
	c.Assert(withoutEnumTypes, qt.HasLen, len(withoutOwnerTypes)-1, qt.Commentf("the fixture declares one enum type"))
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: platform.CockroachDB, want: withoutOwnerTypes},
		{dialect: platform.YugabyteDB, want: withoutOwnerTypes},
		{dialect: platform.Spanner, want: withoutEnumTypes},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(nodeKinds(database, test.dialect), qt.DeepEquals, test.want)
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

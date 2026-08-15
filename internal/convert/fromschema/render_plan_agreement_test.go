package fromschema_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// objectSkippedMarker is the tail of the diagnostic the PostgreSQL-family
// renderer writes in place of an object its capability set refuses. Both
// surfaces reach that renderer, so the marker is how a census tells "the target
// was told it cannot host this" apart from "nobody mentioned it".
const objectSkippedMarker = "is not supported by this target; skipped."

// TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget is stokaro/ptah#929 item 4:
// offline `schema render` and the live `schema apply` plan must give the same
// answer for the same desired schema on the same PostgreSQL-family target.
//
// The reported defect was one-sided. `schema render --dialect yugabytedb`
// emitted a single CREATE TABLE while `schema apply --dry-run` against a live
// YugabyteDB planned six statements, because the offline converter gated every
// PostgreSQL object kind on a predicate matching only the literal strings
// "postgres" and "postgresql" while the planner routed cockroachdb, yugabytedb
// and spanner through the PostgreSQL planner with no such gate. The two
// commands disagreed about the same file, and the omitting side said nothing.
//
// The comparison is a census rather than a text diff on purpose. The two
// surfaces legitimately order their statements differently, and the planner
// writes an idempotent `DROP POLICY IF EXISTS` ahead of a policy the offline
// renderer creates outright. Neither is an object appearing on one surface and
// vanishing on the other, which is the only difference this test is about. So
// each surface is reduced to, per AST node kind, how many nodes it produced and
// how many of those the renderer answered with a named skip. That reduction
// catches an object dropped by either side, and it also catches the residue
// shape #1447 recorded: one surface naming a skip while the other stays silent.
//
// Two other tests already stand on this ground, and this one is placed where it
// is because of what neither can reach:
//
//   - migration/planner.TestObjectKinds_NeitherPathLosesAnObject classifies each
//     (dialect, object) cell as ddl / skipped / silent. Its rows are
//     objectKindGates, which is one row per capability KEY, so it is complete
//     over the things a preset can refuse: views, matviews, functions, triggers,
//     sequences, roles, grants, RLS and policies. Extensions, domains, composite
//     types and range types have no capability key, so they have no row there.
//     Those are exactly the kinds #929's own item-5 measurement called the widest
//     hole. This test needs no key, because it asks the two surfaces about each
//     other rather than about a preset.
//   - TestFromDatabase_EveryDialectGetsEveryDeclaredObject in this package pins
//     the render surface against the fixture, on every dialect. It says nothing
//     about the plan surface, which is the other half of a disagreement.
//
// The fixture is shared with that second test on purpose:
// TestFromDatabase_TheRoutingFixtureCoversEveryDeclaredCollection holds it
// complete over goschema.Database by reflection, so an object kind added to the
// schema model cannot quietly fall outside this comparison.
//
// The live half of this measurement is
// TestSchemaRenderAndPlanCatalogAgreementE2E in ./integration, which applies
// both surfaces and reads the catalog back. This half exists because it needs
// no server, so it covers spanner, the family member issue stokaro/ptah#942
// records as having no live coverage at all.
func TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget(t *testing.T) {
	c := qt.New(t)

	dialects := postgresFamilyPlannerDialects()

	// Control on the enumeration: a filter that matched nothing, or a registry
	// that had not initialized, would make every loop below vacuous.
	c.Assert(len(dialects) > 1, qt.IsTrue,
		qt.Commentf("planner registry reported %d PostgreSQL-family dialects: %v",
			len(dialects), dialects))
	c.Assert(dialects, qt.Contains, platform.Spanner,
		qt.Commentf("spanner has no live coverage, so this offline half is the only place it is measured"))

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := routingFixture()

			renderCensus := surfaceCensus(c.TB, dialect,
				fromschema.FromDatabase(desired, dialect).Statements)

			planNodes, err := planner.GenerateSchemaDiffAST(
				schemadiff.CompareWithDialect(&desired, &dbschematypes.DBSchema{}, dialect),
				&desired,
				dialect,
			)
			c.Assert(err, qt.IsNil)
			planCensus := surfaceCensus(c.TB, dialect, planNodes)

			// Non-vacuity: two empty censuses are equal. The fixture declares one
			// object of every kind in routedKinds, and each of those kinds is one
			// AST node kind, so a surface that carried them all reports exactly
			// that many rows.
			//
			// Check rather than Assert so a surface that lost a kind still reaches
			// the comparison below, which is the assertion that names which kind
			// went missing on which side.
			c.Check(renderCensus, qt.HasLen, len(routedKinds),
				qt.Commentf("render surface census:\n%s", strings.Join(renderCensus, "\n")))

			c.Assert(planCensus, qt.DeepEquals, renderCensus,
				qt.Commentf("render and plan disagree for %s\nrender:\n%s\nplan:\n%s",
					dialect, strings.Join(renderCensus, "\n"), strings.Join(planCensus, "\n")))
		})
	}
}

// postgresFamilyPlannerDialects lists the registered planner dialects that
// platform calls PostgreSQL-family.
//
// It is derived from the registry and the predicate rather than written out,
// because item 4 is about the family and not about three names somebody
// remembered. A fifth member registered tomorrow is measured the same day.
func postgresFamilyPlannerDialects() []string {
	return slices.DeleteFunc(planner.RegisteredDialects(), func(dialect string) bool {
		return !platform.IsPostgresFamily(dialect)
	})
}

// surfaceCensus reduces one surface's AST nodes to one sorted line per node
// kind: how many were produced, and how many of those the renderer refused with
// a named skip.
//
// Each node is rendered on its own so the count is per object rather than per
// document; rendering the whole slice at once would let one node's text hide
// another's absence.
func surfaceCensus(tb testing.TB, dialect string, nodes []ast.Node) []string {
	c := qt.New(tb)
	c.Helper()

	produced := map[string]int{}
	skipped := map[string]int{}
	for _, node := range nodes {
		sql, err := renderer.RenderSQL(dialect, node)
		c.Assert(err, qt.IsNil, qt.Commentf("rendering %T for %s", node, dialect))

		kind := strings.TrimPrefix(fmt.Sprintf("%T", node), "*ast.")
		produced[kind]++
		skipped[kind] += strings.Count(sql, objectSkippedMarker)
	}

	lines := make([]string, 0, len(produced))
	for kind, count := range produced {
		lines = append(lines, fmt.Sprintf("%-32s produced=%d skipped=%d", kind, count, skipped[kind]))
	}
	slices.Sort(lines)
	return lines
}

package modelast_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/modelast"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
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
//   - TestCollectDatabase_EveryDialectGetsEveryDeclaredObject in this package pins
//     the render surface against the fixture, on every dialect. It says nothing
//     about the plan surface, which is the other half of a disagreement.
//
// The fixture is shared with that second test on purpose:
// TestCollectDatabase_TheRoutingFixtureCoversEveryDeclaredCollection holds it
// complete over schemamodel.Database by reflection, so an object kind added to the
// schema model cannot quietly fall outside this comparison.
//
// The live half of this measurement is
// TestSchemaRenderAndPlanCatalogAgreementE2E in ./integration, which applies
// both surfaces and reads the catalog back. This half exists because it needs
// no server, so it covers spanner, the family member issue stokaro/ptah#942
// records as having no live coverage at all.
func TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget(t *testing.T) {
	c := qt.New(t)

	dialects := postgresFamilyPlannerDialects(c)

	// Control on the enumeration: a filter that matched nothing, or a registry
	// that had not initialized, would make every loop below vacuous.
	c.Assert(len(dialects) > 1, qt.IsTrue,
		qt.Commentf("planner registry reported %d PostgreSQL-family dialects: %v",
			len(dialects), dialects))
	c.Assert(dialects, qt.Contains, platform.Spanner,
		qt.Commentf("spanner has no live coverage, so this offline half is the only place it is measured"))

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			assertRenderAndPlanAgree(qt.New(t), dialect)
		})
	}
}

// derivedNodeKinds are the AST node kinds the routing fixture causes without
// declaring an object of that kind.
//
// A default privilege names the schema it applies in, and the fixture declares
// no schema of its own. Both surfaces turn that name into a namespace: the
// render appends it through schemasForRender, and the plan emits it as a
// precondition, because `ALTER DEFAULT PRIVILEGES ... IN SCHEMA` against a
// schema the server does not hold fails with SQLSTATE 3F000.
//
// They are counted here rather than added to routedKinds, which is the census
// of DECLARED object kinds and excludes namespaces on purpose.
var derivedNodeKinds = []string{"CreateSchemaNode"}

// assertRenderAndPlanAgree is the per-dialect body.
//
// It lives here rather than inline because a target that cannot create a
// domain takes the other of two paths, and the choice belongs beside the two
// rather than as a branch inside the test.
func assertRenderAndPlanAgree(c *qt.C, dialect string) {
	c.Helper()
	desired := routingFixture()

	// A target that cannot create a domain refuses the whole schema before
	// SQL, on BOTH surfaces, because they share one validation -- the render
	// call makes it directly and the plan pipeline makes it in the comparison
	// that feeds the planner. That is agreement too, and asserting it is
	// stronger than adapting the fixture until the question goes away: the
	// census below could not tell a shared refusal from a shared emission
	// (stokaro/ptah#1717).
	if assertBothSurfacesRefuseTheDomain(c, dialect, &desired) {
		return
	}
	// Remove refused YDB families in the order shared validation checks them,
	// then run the census over the rest of the fixture.
	refused := assertBothSurfacesRefuseTheCoordinationNode(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheResourcePools(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheSecret(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheStreamingQueries(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheExternalObjects(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheTopic(c, dialect, &desired)
	refused += assertBothSurfacesRefuseTheReplications(c, dialect, &desired)

	renderCensus := surfaceCensus(c, dialect,
		modelast.CollectDatabase(desired, dialect).Statements)

	planNodes, err := planner.GenerateSchemaDiffAST(
		schemadiff.CompareWithDialect(&desired, &catalog.Database{}, dialect),

		dialect,
	)

	c.Assert(err, qt.IsNil)
	planCensus := surfaceCensus(c, dialect, planNodes)

	// Non-vacuity: two empty censuses are equal. The fixture declares one
	// object of every kind in routedKinds, and each of those kinds is one
	// AST node kind, so a surface that carried them all reports exactly
	// that many rows, plus the kinds below that the fixture causes without
	// declaring, less the kinds both surfaces refused above.
	//
	// Check rather than Assert so a surface that lost a kind still reaches
	// the comparison below, which is the assertion that names which kind
	// went missing on which side.
	c.Check(renderCensus, qt.HasLen, len(routedKinds)+len(derivedNodeKinds)-refused,
		qt.Commentf("render surface census:\n%s", strings.Join(renderCensus, "\n")))

	c.Assert(planCensus, qt.DeepEquals, renderCensus,
		qt.Commentf("render and plan disagree for %s\nrender:\n%s\nplan:\n%s",
			dialect, strings.Join(renderCensus, "\n"), strings.Join(planCensus, "\n")))
}

// postgresFamilyPlannerDialects lists the registered planner dialects that
// platform calls PostgreSQL-family.
//
// It is derived from the registry and the predicate rather than written out,
// because item 4 is about the family and not about three names somebody
// remembered. A fifth member registered tomorrow is measured the same day.
func postgresFamilyPlannerDialects(c *qt.C) []string {
	registered, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(registered, func(dialect string) bool {
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
func surfaceCensus(c *qt.C, dialect string, nodes []ast.Node) []string {
	c.Helper()

	produced := make(map[string]int)
	skipped := make(map[string]int)
	for _, node := range nodes {
		sql, err := builtin.RenderSQL(dialect, node)
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

// assertBothSurfacesRefuseTheDomain checks the shared refusal and reports
// whether it applied, so the caller has one branch rather than a nest of them.
func assertBothSurfacesRefuseTheDomain(c *qt.C, dialect string, desired *schemamodel.Database) bool {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.DomainTypes) {
		return false
	}
	// The plan surface refuses at the comparison that feeds it. A plan reads
	// only the diff, so the validation that has to see the whole target runs
	// where the whole target is supplied, and the pipeline still refuses
	// before it plans (stokaro/ptah#2315).
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)

	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "CREATE DOMAIN")
	c.Assert(renderErr.Error(), qt.Contains, "CREATE DOMAIN")
	return true
}

// assertBothSurfacesRefuseTheCoordinationNode checks that a target without
// the coordination_nodes key refuses the fixture's coordination node on both
// surfaces, through the one validation they share, and takes the node out of
// desired so the census can run over the rest. It returns how many routed
// kinds it took out.
func assertBothSurfacesRefuseTheCoordinationNode(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.CoordinationNodes) {
		return 0
	}
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability coordination_nodes")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability coordination_nodes")
	desired.CoordinationNodes = nil
	return 1
}

// assertBothSurfacesRefuseTheTopic checks that a target without the topics key
// refuses the fixture's topic on both surfaces, through the one validation they
// share, and takes the topic out of desired so the census can run over the
// rest. It returns how many routed kinds it took out.
func assertBothSurfacesRefuseTheTopic(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.Topics) {
		return 0
	}
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability topics")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability topics")
	desired.Topics = nil
	return 1
}

// assertBothSurfacesRefuseTheReplications checks that a target without the
// async_replication and transfers keys refuses the fixture's replication and
// transfer on both surfaces, through the one validation they share, and takes
// each out of desired so the census can run over the rest. It returns how many
// routed kinds it took out.
func assertBothSurfacesRefuseTheReplications(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	caps := capability.ForDialect(dialect)
	if caps.Has(capability.AsyncReplication) || caps.Has(capability.Transfers) {
		return 0
	}
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability async_replication")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability async_replication")
	desired.AsyncReplications = nil

	_, planErr = schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr = builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability transfers")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability transfers")
	desired.Transfers = nil
	return 2
}

// assertBothSurfacesRefuseTheResourcePools checks that a target without the
// resource_pools key refuses the fixture's pool on both surfaces, through the
// one validation they share, and takes the pool and its classifier out of
// desired so the census can run over the rest. It returns how many routed
// kinds it took out.
func assertBothSurfacesRefuseTheResourcePools(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.ResourcePools) {
		return 0
	}
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability resource_pools")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability resource_pools")
	desired.ResourcePools, desired.ResourcePoolClassifiers = nil, nil
	return 2
}

// assertBothSurfacesRefuseTheSecret checks that a target without the secrets
// key refuses the fixture's secret on both surfaces, through the one
// validation they share, and takes the secret out of desired so the census can
// run over the rest. It returns how many routed kinds it took out.
//
// The fixture holds a topic and external objects too, which the same targets
// refuse, so the probe leaves them out: the refusal measured here is the
// secret's whichever family the validation reaches first.
func assertBothSurfacesRefuseTheSecret(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.Secrets) {
		return 0
	}
	probe := *desired
	probe.Topics, probe.ExternalDataSources, probe.ExternalTables = nil, nil, nil
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		&probe, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(&probe, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability secrets")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability secrets")
	desired.Secrets = nil
	return 1
}

// assertBothSurfacesRefuseTheExternalObjects checks that a target without the
// external_data_sources key refuses the fixture's data source on both
// surfaces, and takes the data source and the external table out of desired.
// It returns how many routed kinds it took out. The probe leaves the topic
// out, for the reason [assertBothSurfacesRefuseTheSecret] gives.
func assertBothSurfacesRefuseTheExternalObjects(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	if capability.ForDialect(dialect).Has(capability.ExternalDataSources) {
		return 0
	}
	probe := *desired
	probe.Topics = nil
	_, planErr := schemadiff.CompareWithDatabaseInfo(
		&probe, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil,
	)
	renderErr := builtin.ValidateSchema(&probe, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability external_data_sources")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability external_data_sources")
	desired.ExternalDataSources, desired.ExternalTables = nil, nil
	return 2
}

// assertBothSurfacesRefuseTheStreamingQueries measures the refusal before
// removing this family from the shared routing census.
func assertBothSurfacesRefuseTheStreamingQueries(c *qt.C, dialect string, desired *schemamodel.Database) int {
	c.Helper()
	_, planErr := schemadiff.CompareWithDatabaseInfo(desired, &catalog.Database{}, catalog.ServerInfo{Dialect: dialect}, nil)
	renderErr := builtin.ValidateSchema(desired, dialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(planErr.Error(), qt.Contains, "requires target capability streaming_queries")
	c.Assert(renderErr.Error(), qt.Contains, "requires target capability streaming_queries")
	desired.StreamingQueries = nil
	return 1
}

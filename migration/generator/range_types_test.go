package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtinlowering"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/modelast"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// Rollback takes the subtype, options, and comment from the catalog snapshot.
// Explicit and implicit public schemas identify the same range on both paths.
func TestRangeBidirectionalPlanRestoresCapturedDefinition(t *testing.T) {
	for _, test := range []struct{ name, desiredSchema, currentSchema string }{
		{name: "qualified declaration", desiredSchema: "public"},
		{name: "qualified observation", currentSchema: "public"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			caps := capability.Postgres18()
			before := &pgtype.ObservedRange{Subtype: "numeric", SubtypeOpClass: "numeric_ops", SubtypeDiff: "numrange_subdiff", Comment: "captured"}
			current := &catalog.Database{
				FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.RangeKind, test.currentSchema, "span"), before)))),
				FeatureCoverage: must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
			}
			desired := &schemamodel.Database{
				FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, test.desiredSchema, "span"), &pgtype.DesiredRange{Subtype: "int8", Comment: "declared"})))),
				FeatureCoverage: must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
			}
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FeatureChanges, qt.HasLen, 1)
			captured, err := runtime.Codecs().SnapshotChanges(c.Context(), diff.FeatureChanges)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres", Capabilities: caps,
			})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities("postgres", caps, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.Contains, "SUBTYPE = int8")
			reverse, err := builtin.RenderSQLWithCapabilities("postgres", caps, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(reverse, qt.Contains, "SUBTYPE = numeric, SUBTYPE_OPCLASS = numeric_ops, SUBTYPE_DIFF = numrange_subdiff")
			c.Assert(reverse, qt.Contains, "IS 'captured'")
			c.Assert(plan.Reverse.Diff.FeatureChanges, qt.HasLen, 1)
			restored := plan.Reverse.Diff.FeatureChanges[0].Value.(*pgtype.RangeChange)
			c.Assert(restored.After, qt.DeepEquals, before.Desired())
			c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
			restored.After.Subtype = "text"
			c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
		})
	}
}

func TestRangePlanOrdersDependenciesAcrossTypeOwners(t *testing.T) {
	for _, schema := range []string{"", "app", "public"} {
		t.Run("schema="+schema, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			caps := capability.Postgres18()
			qualify := func(name string) string {
				return strings.TrimPrefix(schema+"."+name, ".")
			}
			desired := &schemamodel.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(
					must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, schema, "wrapper"), &pgtype.DesiredDomain{BaseType: qualify("z_inner")})),
					must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, schema, "base"), &pgtype.DesiredDomain{BaseType: "numeric"})),
					must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, schema, "box"),
						&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: qualify("a_outer")}}})),
					must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, schema, "a_outer"), &pgtype.DesiredRange{Subtype: qualify("wrapper")})),
					must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, schema, "z_inner"), &pgtype.DesiredRange{Subtype: qualify("base")})),
				)),
				FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired)),
			}
			current := &catalog.Database{FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed))}
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres", Capabilities: caps})
			c.Assert(err, qt.IsNil)
			declaration, err := modelast.CollectDatabase(*desired, "postgres", builtinlowering.ForTarget(c.Context(), runtime, "postgres", caps))
			c.Assert(err, qt.IsNil)
			for _, nodes := range []struct {
				name  string
				sql   string
				order []string
			}{
				{"forward", must.Must(builtin.RenderSQLWithCapabilities("postgres", caps, plan.Forward.Nodes...)), []string{"base", "z_inner", "wrapper", "a_outer", "box"}},
				{"declaration", must.Must(builtin.RenderSQLWithCapabilities("postgres", caps, declaration.Statements...)), []string{"base", "z_inner", "wrapper", "a_outer", "box"}},
				{"reverse", must.Must(builtin.RenderSQLWithCapabilities("postgres", caps, plan.Reverse.Nodes...)), []string{"box", "a_outer", "wrapper", "z_inner", "base"}},
			} {
				t.Run(nodes.name, func(t *testing.T) {
					c := qt.New(t)
					previous := -1
					for _, name := range nodes.order {
						position := strings.Index(nodes.sql, `"`+name+`"`)
						c.Assert(position > previous, qt.IsTrue, qt.Commentf("%s must follow its dependency in:\n%s", name, nodes.sql))
						previous = position
					}
				})
			}
		})
	}
}

func TestRangeReplacementOrdersChangedDependents(t *testing.T) {
	for _, test := range []struct {
		name, before, after string
		order               []string
	}{
		{name: "range replacement", before: `CREATE TYPE app.span AS RANGE (SUBTYPE = numeric); CREATE DOMAIN app.wrapped AS app.span; CREATE TYPE app.box AS (value app.wrapped);`, after: `CREATE TYPE app.span AS RANGE (SUBTYPE = int8); CREATE DOMAIN app.wrapped AS app.span NOT NULL; CREATE TYPE app.box AS (value app.wrapped, label text);`, order: []string{`DROP TYPE IF EXISTS "app"."box"`, `DROP DOMAIN IF EXISTS "app"."wrapped"`, `DROP TYPE IF EXISTS "app"."span"`, `CREATE TYPE "app"."span"`, `CREATE DOMAIN "app"."wrapped"`, `CREATE TYPE "app"."box"`}},
		{name: "domain replacement", before: `CREATE DOMAIN app.base AS numeric; CREATE TYPE app.span AS RANGE (SUBTYPE = app.base); CREATE DOMAIN app.wrapped AS app.span;`, after: `CREATE DOMAIN app.base AS bigint; CREATE TYPE app.span AS RANGE (SUBTYPE = app.base); COMMENT ON TYPE app.span IS 'declared'; CREATE DOMAIN app.wrapped AS app.span NOT NULL;`, order: []string{`DROP DOMAIN IF EXISTS "app"."wrapped"`, `DROP TYPE IF EXISTS "app"."span"`, `DROP DOMAIN IF EXISTS "app"."base"`, `CREATE DOMAIN "app"."base"`, `CREATE TYPE "app"."span"`, `CREATE DOMAIN "app"."wrapped"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			caps := capability.Postgres18()
			options := sqlschema.Options{Owners: runtime.SQL()}
			before, _, err := sqlschema.ReadWith(c.Context(), []byte(test.before), "postgres", nil, options)
			c.Assert(err, qt.IsNil)
			after, _, err := sqlschema.ReadWith(c.Context(), []byte(test.after), "postgres", nil, options)
			c.Assert(err, qt.IsNil)
			current, err := goschematodb.ToDBSchema(c.Context(), &before, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &after, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: &after, CurrentSchema: current, Dialect: "postgres", Capabilities: caps})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities("postgres", caps, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			previous := -1
			for _, statement := range test.order {
				position := strings.Index(forward, statement)
				c.Assert(position > previous, qt.IsTrue, qt.Commentf("%s must follow its prerequisite in:\n%s", statement, forward))
				previous = position
			}
		})
	}
}

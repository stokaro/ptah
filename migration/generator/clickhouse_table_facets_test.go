package generator_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A live connection supplies a default database after SQL is parsed. Reverse
// CREATE projection must retain table coverage under that same identity.
func TestClickHouseSQLCreationPlansBothDirections(t *testing.T) {
	for _, test := range []struct {
		name, defaultSchema, table, explicitSchema string
	}{
		{"offline", "", "asn", ""},
		{"connection database", "tenant_database", "asn", ""},
		{"database containing a dot", "tenant.database", "asn", ""},
		{"explicit other database", "tenant_database", "archive.asn", "archive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			sql := fmt.Sprintf("CREATE TABLE %s (id Int32, n Int32) ENGINE = MergeTree ORDER BY id;", test.table)
			source, _, err := sqlschema.Read([]byte(sql), "clickhouse")
			c.Assert(err, qt.IsNil)
			current := &catalog.Database{}
			semantics := identifier.ForDialect("clickhouse")
			semantics.DefaultSchema = test.defaultSchema
			info := catalog.ServerInfo{Dialect: "clickhouse", Schema: test.defaultSchema, IdentifierSemantics: semantics}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &source, current, info, nil, runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: &source, CurrentSchema: current, Dialect: "clickhouse",
			})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.Contains, "CREATE TABLE "+test.table)
			c.Assert(forward, qt.Contains, "ORDER BY (id)")
			reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(reverse, qt.Contains, "DROP TABLE IF EXISTS "+test.table)
			c.Assert(plan.Reverse.Diff.TablesRemoved, qt.HasLen, 1)
			subject := objectidentity.NewBuilder(semantics).TableParts(test.explicitSchema, "asn")
			coverage := plan.Reverse.Diff.TablesRemoved[0].Current.FeatureCoverage
			c.Assert(coverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
			c.Assert(source.Tables[0].Schema, qt.Equals, test.explicitSchema)
		})
	}
}

func TestClickHouseTypedCreationPlansBothDirections(t *testing.T) {
	for _, test := range []struct {
		name    string
		primary chschema.Setting
		want    string
	}{
		{"inherited", chschema.Setting{}, "PRIMARY KEY (tenant, id)"},
		{"default", chschema.Setting{State: chschema.Default}, "PRIMARY KEY (tenant, id)"},
		{"empty", chschema.Setting{State: chschema.Explicit}, "PRIMARY KEY (tuple())"},
		{"prefix", chschema.Setting{State: chschema.Explicit, Value: "tenant"}, "PRIMARY KEY (tenant)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			source := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{
					OrderBy: chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, PrimaryKey: test.primary,
				}))}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}, {Name: "tenant", StructName: "Event", Type: "UInt64"}},
			}
			current := &catalog.Database{}
			diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Forward.Nodes, qt.HasLen, 1)
			c.Assert(plan.Reverse.Nodes, qt.HasLen, 1)
			forward, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes[0])
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.Contains, "CREATE TABLE events")
			c.Assert(forward, qt.Contains, "ORDER BY (tenant, id)")
			c.Assert(forward, qt.Contains, test.want)
			reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes[0])
			c.Assert(err, qt.IsNil)
			c.Assert(reverse, qt.Contains, "DROP TABLE IF EXISTS events")
		})
	}
}

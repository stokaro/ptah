package chsource_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

func refreshYAML(c *qt.C, attribute string) *schemamodel.Database {
	c.Helper()
	db, err := yamlschema.Parse(c.Context(), builtintest.Runtime().YAML(), []byte("matviews:\n  mv:\n    body: SELECT 1\n"+attribute))
	c.Assert(err, qt.IsNil)
	return db
}

func refreshHCL(c *qt.C, attribute string) *schemamodel.Database {
	c.Helper()
	db, err := atlashcl.ParseWithOptions(c.Context(), []byte("materialized \"mv\" {\n  as = \"SELECT 1\"\n"+attribute+"\n}"), "schema.hcl",
		atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	return db
}

func TestRefreshSourcesHaveTheSameScheduleAndSQL(t *testing.T) {
	for _, clause := range []string{
		"EVERY 60 MINUTE", "AFTER 90 SECOND", "EVERY 1 DAY OFFSET 120 MINUTE RANDOMIZE FOR 1800 SECOND DEPENDS ON other APPEND",
	} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)
			goDB, err := parseMatViewRefreshSource(c, fmt.Sprintf(" refresh=%q", clause))
			c.Assert(err, qt.IsNil)
			sqlDB, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE MATERIALIZED VIEW mv REFRESH "+clause+" AS SELECT 1;"), "clickhouse", nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			yamlDB := refreshYAML(c, fmt.Sprintf("    refresh: %q\n", clause))
			hclDB := refreshHCL(c, fmt.Sprintf("  refresh = %q", clause))
			want := must.Must(chsource.RefreshFacets(clause))
			wantSQL := must.Must(builtin.GetOrderedCreateStatementsWithCapabilities(&goDB, "clickhouse", capability.ClickHouse2411()))
			for _, db := range []*schemamodel.Database{&goDB, &sqlDB, yamlDB, hclDB} {
				c.Assert(db.MaterializedViews, qt.HasLen, 1)
				c.Assert(db.MaterializedViews[0].Facets, qt.DeepEquals, want)
				c.Assert(must.Must(builtin.GetOrderedCreateStatementsWithCapabilities(db, "clickhouse", capability.ClickHouse2411())), qt.DeepEquals, wantSQL)
			}
			exported, err := atlashclrender.Render(yamlDB, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "clickhouse"})
			c.Assert(err, qt.IsNil)
			c.Assert(exported.Diagnostics, qt.HasLen, 0)
			again, err := atlashcl.ParseWithOptions(t.Context(), exported.Data, "export.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			c.Assert(again.MaterializedViews[0].Facets, qt.DeepEquals, want)
		})
	}
}

func TestRefreshSourcesOmittedAndEmptyDeclareNoSchedule(t *testing.T) {
	c := qt.New(t)
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "mv")
	for _, db := range []*schemamodel.Database{
		refreshYAML(c, ""), refreshYAML(c, "    refresh: ''\n"), refreshHCL(c, ""), refreshHCL(c, "  refresh = \"\""),
	} {
		c.Assert(db.MaterializedViews[0].Facets.IsZero(), qt.IsTrue)
		c.Assert(db.FeatureCoverage.Lookup(chschema.RefreshKind, view).State, qt.Equals, schemaext.Complete)
	}
	for _, owners := range []schemaext.Coverage{
		must.Must(yamlext.None().Coverage()), must.Must(hclext.None().Coverage(hclext.CoverageRequest{})),
	} {
		c.Assert(owners.Lookup(chschema.RefreshKind, view).State, qt.Equals, schemaext.Uninspected)
	}
}

func TestRefreshHCLExportPreservesQualifiedQuotedNames(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`schema "a.b" {}
materialized " v.name " {
 schema = schema["a.b"]
 as = "SELECT 1"
 refresh = "EVERY 1 HOUR"
}`), "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "clickhouse"})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	again, err := atlashcl.ParseWithOptions(t.Context(), result.Data, "copy.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(again.MaterializedViews, qt.DeepEquals, db.MaterializedViews)
}

package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// TestRenderInspected_WritesClickHouseRowPolicy preserves the owner model in HCL.
func TestRenderInspected_WritesClickHouseRowPolicy(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "UInt64", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"),
			chschema.DesiredRowPolicy{Filter: new("tenant_id = 1"), Composition: chschema.Restrictive})))),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	rendered, err := atlashclrender.RenderInspectedForAtlasCLI(db, "", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "clickhouse"})

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `row_policy "tenant"`)
	c.Assert(string(rendered.Data), qt.Contains, `as = "RESTRICTIVE"`)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
}

// TestRender_SharedRowSecurityWritesRestrictiveAndEnforced writes a shared
// declaration's composition and FORCE as the attributes the HCL parser reads,
// rather than leaving a restrictive policy permissive and a forced table
// unforced (stokaro/ptah#4343).
func TestRender_SharedRowSecurityWritesRestrictiveAndEnforced(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
		RLSPolicies: []schemamodel.RLSPolicy{{StructName: "Order", Name: "tenant", Table: "orders", UsingExpression: "true",
			Restrictive: true}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{StructName: "Order", Table: "orders", Forced: true}},
	}

	rendered, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "mysql"})

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `as = "RESTRICTIVE"`)
	c.Assert(string(rendered.Data), qt.Contains, "enforced = true")
}

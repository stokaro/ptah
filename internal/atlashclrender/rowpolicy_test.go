package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/atlashclrender"
)

// TestRenderInspected_ReportsAClickHouseRowPolicyItCannotWrite reports a
// ClickHouse row policy as left out of the document rather than writing it as
// a policy block: Atlas HCL declares PostgreSQL's row-level security only, so
// the block would read back as a permissive PostgreSQL policy, and a
// restrictive row policy would come back admitting rows it withholds
// (stokaro/ptah#4343).
func TestRenderInspected_ReportsAClickHouseRowPolicyItCannotWrite(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "UInt64", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"),
			chschema.DesiredRowPolicy{Filter: new("tenant_id = 1"), Composition: chschema.Restrictive})))),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	rendered, err := atlashclrender.RenderInspectedForAtlasCLI(db, "clickhouse", "")

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "policy")
	c.Assert(rendered.Diagnostics, qt.HasLen, 1)
	c.Assert(rendered.Diagnostics[0].Severity, qt.Equals, atlashclrender.SeverityWarning)
	c.Assert(rendered.Diagnostics[0].Message, qt.Contains, "ptah.run/clickhouse/row-policy orders.tenant")
	c.Assert(rendered.Diagnostics[0].Message, qt.Contains, "is not represented in HCL")
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

	rendered, err := atlashclrender.RenderForDialect(db, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `as = "RESTRICTIVE"`)
	c.Assert(string(rendered.Data), qt.Contains, "enforced = true")
}

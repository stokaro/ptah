package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/atlashclrender"
)

// rowSecurityDocument declares orders with the given switches and two
// policies: one with every value away from its default, one with every value
// left to PostgreSQL.
func rowSecurityDocument(state *pgpolicy.DesiredTableState, roles ...pgpolicy.RoleSelector) *schemamodel.Database {
	facets := schemaext.Facets{}
	if state != nil {
		facets = must.Must(schemaext.NewFacets(state))
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Facets: facets}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{
				Command: pgpolicy.CommandUpdate, Roles: roles, Using: new("tenant_id = 1"), WithCheck: new("tenant_id > 0"),
				Composition: pgpolicy.Restrictive, Comment: "tenant rows",
			})),
			must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "all_rows"), pgpolicy.DesiredPolicy{})),
		)),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
}

// TestRenderInspected_WritesRowSecurity pins the blocks a table's policies and
// switches are written as: each value a declaration states and nothing it
// leaves out, keywords as strings, and FORCE as `enforced`.
func TestRenderInspected_WritesRowSecurity(t *testing.T) {
	c := qt.New(t)
	db := rowSecurityDocument(&pgpolicy.DesiredTableState{Enabled: true, Forced: true, Comment: "isolated"},
		pgpolicy.RoleSelector{Name: "reader"}, pgpolicy.RoleSelector{Keyword: pgpolicy.CurrentUser})

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	document := string(result.Data)
	for _, block := range []string{
		"  row_security {\n    enabled = true\n    enforced = true\n    comment = \"isolated\"\n  }\n",
		"policy \"all_rows\" {\n  on = table.orders\n}\n",
		"policy \"tenant\" {\n  on = table.orders\n  for = \"UPDATE\"\n  to = [\"CURRENT_USER\", \"reader\"]\n  as = \"RESTRICTIVE\"\n" +
			"  using = \"tenant_id = 1\"\n  check = \"tenant_id > 0\"\n  comment = \"tenant rows\"\n}\n",
	} {
		c.Assert(document, qt.Contains, block)
	}
}

// TestRenderInspected_ReportsRowSecurityItCannotWrite pins the losses HCL
// reports rather than leaves silent: FORCE without ENABLE, and a role named
// like a role keyword, which a reader would take for the keyword.
func TestRenderInspected_ReportsRowSecurityItCannotWrite(t *testing.T) {
	tests := []struct {
		name     string
		database func() *schemamodel.Database
		path     string
		message  string
	}{
		{name: "force without enable", database: func() *schemamodel.Database {
			return rowSecurityDocument(&pgpolicy.DesiredTableState{Forced: true})
		}, path: "table.orders.row_security", message: "FORCE ROW LEVEL SECURITY without ENABLE cannot be represented in HCL"},
		{name: "a role named like a keyword", database: func() *schemamodel.Database {
			return rowSecurityDocument(nil, pgpolicy.RoleSelector{Name: "current_user"})
		}, path: "rls_policies.orders.tenant", message: "a role named like a role keyword cannot be represented in HCL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.database(), platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Path, qt.Equals, test.path)
			c.Assert(result.Diagnostics[0].Message, qt.Equals, test.message)
			c.Assert(strings.Count(string(result.Data), "row_security"), qt.Equals, 0)
		})
	}
}

// TestRenderInspected_RefusesRowSecurityCoverageItCannotWrite pins that a
// document claiming to describe every policy refuses a source that could not.
func TestRenderInspected_RefusesRowSecurityCoverageItCannotWrite(t *testing.T) {
	c := qt.New(t)
	db := rowSecurityDocument(nil)
	db.FeatureCoverage = must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "partial"}, nil))

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Data, qt.IsNil)
}

// TestRenderInspected_ReportsAScopedSwitch pins that switches bound to a
// target are written, and the binding HCL cannot carry is reported as a loss.
func TestRenderInspected_ReportsAScopedSwitch(t *testing.T) {
	c := qt.New(t)
	db := rowSecurityDocument(&pgpolicy.DesiredTableState{Enabled: true})
	db.Tables[0].Facets = must.Must(db.Tables[0].Facets.WithTargetScope(pgpolicy.TableStateKind, "postgres"))

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Path, qt.Equals, "table.orders.row_security")
	c.Assert(result.Diagnostics[0].Message, qt.Equals, `dialect scope "postgres" is not represented in HCL`)
	c.Assert(string(result.Data), qt.Contains, "  row_security {\n    enabled = true\n  }\n")
}

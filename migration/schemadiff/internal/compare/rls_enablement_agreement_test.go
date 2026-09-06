package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestRLSEnabledTables_ADeclaredPolicyIsNotARequestToDisable pins the rule that
// makes the comparator agree with the planner about what a policy implies.
//
// The planner enables row-level security for a NEW table the description
// declares policies on, because a policy on a table without it does nothing.
// The comparator read the same silence -- no `row_security` block -- as a
// request to DISABLE, so applying one file twice turned the control off.
// Measured on PostgreSQL 17.11: the first apply emitted
// `ALTER TABLE docs ENABLE ROW LEVEL SECURITY`, the second emitted `DISABLE`,
// and the three policies stayed in place, unenforced (stokaro/ptah#2048).
func TestRLSEnabledTables_ADeclaredPolicyIsNotARequestToDisable(t *testing.T) {
	tests := []struct {
		name        string
		policies    []schemamodel.RLSPolicy
		enabled     []schemamodel.RLSEnabledTable
		wantRemoved []string
	}{
		{
			// The row the issue is about.
			name: "policies declared, enablement not",
			policies: []schemamodel.RLSPolicy{{
				StructName: "D", Name: "p", Table: "docs",
				PolicyFor: "SELECT", ToRoles: "PUBLIC", UsingExpression: "true",
			}},
		},
		{
			name: "both declared",
			policies: []schemamodel.RLSPolicy{{
				StructName: "D", Name: "p", Table: "docs",
				PolicyFor: "SELECT", ToRoles: "PUBLIC", UsingExpression: "true",
			}},
			enabled: []schemamodel.RLSEnabledTable{{StructName: "D", Table: "docs"}},
		},
		{
			// The control, and the way a description asks for the control to
			// come off: it stops declaring policies for the table.
			name:        "neither declared",
			wantRemoved: []string{"public.docs"},
		},
		{
			// The other control: a qualified declaration and an unqualified
			// read are one table, which is what the semantics are for.
			name: "the declaration qualifies the table and the read does not",
			policies: []schemamodel.RLSPolicy{{
				StructName: "D", Name: "p", Table: "public.docs",
				PolicyFor: "SELECT", ToRoles: "PUBLIC", UsingExpression: "true",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			declared := &schemamodel.Database{
				Tables:           []schemamodel.Table{{StructName: "D", Name: "docs"}},
				RLSPolicies:      test.policies,
				RLSEnabledTables: test.enabled,
			}
			live := &catalog.Database{Tables: []catalog.Table{{
				Name: "docs", Schema: "public", RLSEnabled: true,
			}}}

			compare.RLSEnabledTablesWithSemantics(declared, live, diff, rlsSemantics())

			c.Assert(diff.RLSEnabledTablesRemoved.Names(), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// rlsSemantics is the identifier rule a live PostgreSQL connection supplies.
func rlsSemantics() identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	return semantics
}

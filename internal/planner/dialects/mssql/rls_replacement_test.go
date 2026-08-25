package mssql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/mssql"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// A replacement the renderer would refuse contributes no DROP -- stokaro/ptah#2211.
//
// A modified policy is planned as a drop followed by a create. When the create
// half is refused, the drop half is the whole plan, and applying it takes the
// table's row-level security away and puts nothing back. That is a worse answer
// than the difference the pair was planned to close, so the pair is not planned
// at all.
//
// The create node is still emitted, so the plan carries the renderer's refusal
// by name. A planner that simply skipped the policy would leave the user with a
// plan that silently does nothing about a declaration Ptah cannot express.
func TestPlanRLS_ARefusedReplacementDoesNotDropThePolicy(t *testing.T) {
	tests := []struct {
		name        string
		policyFor   string
		withCheck   string
		wantDrop    bool
		wantRefusal string
	}{
		{
			name:      "a filter-only policy naming an operation is refused whole",
			policyFor: "SELECT",
			wantDrop:  false, wantRefusal: "no form for",
		},
		{
			name:      "an operation with no block predicate to carry it",
			policyFor: "INSERT",
			wantDrop:  false, wantRefusal: "WITH CHECK",
		},
		{
			name:      "ALL is expressible, so the replacement is planned",
			policyFor: "ALL",
			wantDrop:  true,
		},
		{
			name:      "an operation riding a block predicate is expressible",
			policyFor: "UPDATE", withCheck: "dbo.fn_write(tenant)",
			wantDrop: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				RLSPoliciesModified: []types.RLSPolicyDiff{{
					PolicyName: "tenant_filter", TableName: "docs",
					Changes: map[string]string{"for": "ALL -> " + test.policyFor},
				}},
			}
			generated := &goschema.Database{
				RLSPolicies: []goschema.RLSPolicy{{
					Name: "tenant_filter", Table: "docs",
					PolicyFor:           test.policyFor,
					UsingExpression:     "dbo.fn_pred(tenant)",
					WithCheckExpression: test.withCheck,
				}},
			}

			nodes, err := mssql.New().GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)

			sql, err := renderer.RenderSQL("sqlserver", nodes...)
			c.Assert(err, qt.IsNil)

			dropped := strings.Contains(sql, "DROP SECURITY POLICY")
			c.Assert(dropped, qt.Equals, test.wantDrop,
				qt.Commentf("plan was:\n%s", sql))

			if test.wantRefusal == "" {
				c.Assert(sql, qt.Contains, "CREATE SECURITY POLICY")
				return
			}
			// The refusal reaches the plan, rather than the policy quietly
			// going unchanged with nothing said about it.
			c.Assert(sql, qt.Contains, test.wantRefusal)
			c.Assert(sql, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
		})
	}
}

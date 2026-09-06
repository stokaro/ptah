package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// An unspecified FOR clause and an explicit ALL are the same policy --
// stokaro/ptah#2211.
//
// Both directions are pinned because both occur. A declaration that omits FOR
// is the model's zero value and meets a catalog reporting ALL; a declaration
// that writes ALL meets a catalog that reports nothing, which is what a SQL
// Server filter predicate does. Either way the plan used to be a DROP and a
// CREATE of the policy on every apply, forever.
func TestRLSPolicyDefinitions_AnUnspecifiedForClauseEqualsALL(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		observed   string
		wantChange bool
	}{
		{name: "declared ALL against a catalog that reports nothing", declared: "ALL", observed: ""},
		{name: "declared nothing against a catalog that reports ALL", declared: "", observed: "ALL"},
		{name: "both unspecified", declared: "", observed: ""},
		{name: "both ALL", declared: "ALL", observed: "ALL"},
		{name: "a real difference is still reported", declared: "SELECT", observed: "ALL", wantChange: true},
		{name: "and in the other direction", declared: "", observed: "SELECT", wantChange: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.RLSPolicyDefinitions(
				schemamodel.RLSPolicy{
					Name: "p", Table: "t",
					PolicyFor:       test.declared,
					UsingExpression: "tenant = 1",
				},
				catalog.RLSPolicy{
					Name: "p", Table: "t",
					PolicyFor:       test.observed,
					UsingExpression: "tenant = 1",
				},
			)

			_, changed := diff.Changes["policy_for"]
			c.Assert(changed, qt.Equals, test.wantChange,
				qt.Commentf("declared %q, observed %q, changes: %v",
					test.declared, test.observed, diff.Changes))
		})
	}
}

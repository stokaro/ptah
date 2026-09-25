package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestRLSPolicyDefinitions_AnOmittedToClauseEqualsPUBLIC covers
// stokaro/ptah#3572. PostgreSQL applies a policy with no TO clause to PUBLIC
// and the reader reports it that way, so a declaration that omits TO meets a
// catalog saying PUBLIC. Comparing the two as different made the plan a DROP
// and a CREATE of the policy on every run.
//
// The observed values are the reader's spelling: roles joined with a comma and
// no space, PUBLIC in upper case.
func TestRLSPolicyDefinitions_AnOmittedToClauseEqualsPUBLIC(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		observed   string
		wantChange bool
	}{
		{name: "omitted against PUBLIC", declared: "", observed: "PUBLIC"},
		{name: "PUBLIC written out", declared: "PUBLIC", observed: "PUBLIC"},
		{name: "public in lower case", declared: "public", observed: "PUBLIC"},
		{name: "PUBLIC beside a role", declared: "PUBLIC, app_a", observed: "PUBLIC"},
		{name: "two roles", declared: "app_a, app_b", observed: "app_a,app_b"},
		{name: "two roles in the other order", declared: "app_b, app_a", observed: "app_a,app_b"},
		{name: "a role against PUBLIC is a change", declared: "wpmgr_app", observed: "PUBLIC", wantChange: true},
		{name: "omitted against a role is a change", declared: "", observed: "wpmgr_app", wantChange: true},
		{name: "a second role is a change", declared: "app_a, app_b", observed: "app_a", wantChange: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.RLSPolicyDefinitionsWithExpressions(
				schemamodel.RLSPolicy{Name: "p", Table: "t", ToRoles: test.declared, UsingExpression: "true"},
				catalog.RLSPolicy{Name: "p", Table: "t", ToRoles: test.observed, UsingExpression: "true"},
				"postgres",
				config.PolicyExpression{},
			)

			_, changed := diff.Changes["to_roles"]
			c.Assert(changed, qt.Equals, test.wantChange,
				qt.Commentf("declared %q, observed %q, changes: %v", test.declared, test.observed, diff.Changes))
		})
	}
}

// TestRLSPoliciesWithSemantics_TheDialectReachesTheRoleFold drives the fold
// through the comparison entry point schemadiff calls, so a comparison that
// stopped passing the dialect on is caught here and not only in the helper.
// The dialect-neutral comparison is the control: without a target the empty
// clause means nothing in particular and stays a difference.
func TestRLSPoliciesWithSemantics_TheDialectReachesTheRoleFold(t *testing.T) {
	tests := []struct {
		dialect      string
		wantModified int
	}{
		{dialect: "postgres", wantModified: 0},
		{dialect: "", wantModified: 1},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{RLSPolicies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "public.t", PolicyFor: "ALL", UsingExpression: "true"},
			}}
			database := &catalog.Database{RLSPolicies: []catalog.RLSPolicy{
				{Name: "p", Table: "public.t", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true"},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.RLSPoliciesWithSemantics(
				desired, database, diff, identifier.ForDialect("postgres"), test.dialect, compare.Coverage{}, nil)

			c.Assert(diff.RLSPoliciesModified, qt.HasLen, test.wantModified)
		})
	}
}

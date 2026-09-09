package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A policy's AS clause is compared, because the two spellings are different
// policies (stokaro/ptah#3121).
//
// Permissive policies are OR-ed with each other and restrictive ones AND-ed
// over the result. A comparison that skipped the flag would call a declared
// RESTRICTIVE equal to a permissive policy on the server, so an apply would
// report the schema in sync while the table granted access the declaration
// withholds.
func TestRLSPolicyDefinitions_TheAsClauseIsCompared(t *testing.T) {
	tests := []struct {
		name       string
		declared   bool
		observed   bool
		wantChange string
	}{
		{name: "declared restrictive against a permissive policy", declared: true, observed: false, wantChange: "PERMISSIVE -> RESTRICTIVE"},
		{name: "declared permissive against a restrictive policy", declared: false, observed: true, wantChange: "RESTRICTIVE -> PERMISSIVE"},
		{name: "both restrictive", declared: true, observed: true},
		{name: "both permissive", declared: false, observed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.RLSPolicyDefinitions(
				schemamodel.RLSPolicy{
					Name:        "p",
					Table:       "docs",
					PolicyFor:   "ALL",
					Restrictive: test.declared,
				},
				catalog.RLSPolicy{
					Name:        "p",
					Table:       "docs",
					PolicyFor:   "ALL",
					Restrictive: test.observed,
				},
			)

			c.Assert(diff.Changes["as"], qt.Equals, test.wantChange)
		})
	}
}

// TestRLSPolicyDefinitions_TheAsClauseIsTheOnlyChangeReported is the control
// for the comparison above.
//
// Two policies differing in nothing but the AS clause must produce exactly one
// entry. A comparison that also reported a second key would mean the flag
// leaked into a clause it does not belong to, and the plan would rewrite more
// than the policy's strength.
func TestRLSPolicyDefinitions_TheAsClauseIsTheOnlyChangeReported(t *testing.T) {
	c := qt.New(t)

	diff := compare.RLSPolicyDefinitions(
		schemamodel.RLSPolicy{
			Name:            "p",
			Table:           "docs",
			PolicyFor:       "ALL",
			ToRoles:         "app",
			UsingExpression: "true",
			Restrictive:     true,
		},
		catalog.RLSPolicy{
			Name:            "p",
			Table:           "docs",
			PolicyFor:       "ALL",
			ToRoles:         "app",
			UsingExpression: "true",
			Restrictive:     false,
		},
	)

	c.Assert(diff.Changes, qt.HasLen, 1)
	c.Assert(diff.Changes["as"], qt.Equals, "PERMISSIVE -> RESTRICTIVE")
}

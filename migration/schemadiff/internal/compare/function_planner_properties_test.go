package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A routine's LEAKPROOF is compared, because it decides whether the planner may
// push a filter using the routine past a security barrier (stokaro/ptah#3121).
//
// A comparison that skipped it would report a routine unchanged while the rows
// a row-level-security policy withholds became reachable through it.
func TestFunctionDefinitions_LeakproofIsCompared(t *testing.T) {
	tests := []struct {
		name       string
		declared   bool
		observed   bool
		wantChange string
	}{
		{name: "declared leakproof against one that is not", declared: true, observed: false, wantChange: "false -> true"},
		{name: "declared plain against a leakproof routine", declared: false, observed: true, wantChange: "true -> false"},
		{name: "both leakproof", declared: true, observed: true},
		{name: "neither", declared: false, observed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitions(
				schemamodel.Function{Name: "f", Leakproof: test.declared},
				catalog.Function{Name: "f", Leakproof: test.observed},
			)

			c.Assert(diff.Changes["leakproof"], qt.Equals, test.wantChange)
		})
	}
}

// An unstated PARALLEL level and an explicit UNSAFE are the same routine.
//
// Both directions occur: a declaration that omits the clause is the model's
// zero value and meets a catalog reporting UNSAFE, which is what pg_proc says
// for every routine that never named a level. Comparing the two as different
// makes the plan a CREATE OR REPLACE on every apply, forever.
func TestFunctionDefinitions_AnUnstatedParallelLevelEqualsUnsafe(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		observed   string
		wantChange string
	}{
		{name: "declared UNSAFE against a catalog reporting nothing", declared: "UNSAFE", observed: ""},
		{name: "declared nothing against a catalog reporting UNSAFE", declared: "", observed: "UNSAFE"},
		{name: "both unstated", declared: "", observed: ""},
		{name: "both SAFE", declared: "SAFE", observed: "SAFE"},
		{name: "a real difference is reported", declared: "SAFE", observed: "UNSAFE", wantChange: "UNSAFE -> SAFE"},
		{name: "and in the other direction", declared: "", observed: "SAFE", wantChange: "SAFE -> UNSAFE"},
		{name: "restricted is its own level", declared: "RESTRICTED", observed: "SAFE", wantChange: "SAFE -> RESTRICTED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitions(
				schemamodel.Function{Name: "f", Parallel: test.declared},
				catalog.Function{Name: "f", Parallel: test.observed},
			)

			c.Assert(diff.Changes["parallel"], qt.Equals, test.wantChange)
		})
	}
}

package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A routine's LEAKPROOF is compared, because it decides whether the planner may
// push a filter using the routine past a security barrier (stokaro/ptah#3121).
//
// A comparison that skipped it would report a routine unchanged while the rows
// a row-level-security policy withholds became reachable through it.
func TestFunctionDefinitions_LeakproofIsCompared(t *testing.T) {
	tests := []struct {
		name        string
		declared    bool
		observed    bool
		wantChanges int
	}{
		{name: "declared leakproof against one that is not", declared: true, observed: false, wantChanges: 1},
		{name: "declared plain against a leakproof routine", declared: false, observed: true, wantChanges: 1},
		{name: "both leakproof", declared: true, observed: true},
		{name: "neither", declared: false, observed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			count := comparePostgresProperties(c, pgroutine.Properties{Leakproof: test.declared}, pgroutine.Properties{Leakproof: test.observed})

			c.Assert(count, qt.Equals, test.wantChanges)
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
		name        string
		declared    string
		observed    string
		wantChanges int
	}{
		{name: "declared UNSAFE against a catalog reporting nothing", declared: "UNSAFE", observed: ""},
		{name: "declared nothing against a catalog reporting UNSAFE", declared: "", observed: "UNSAFE"},
		{name: "both unstated", declared: "", observed: ""},
		{name: "both SAFE", declared: "SAFE", observed: "SAFE"},
		{name: "a real difference is reported", declared: "SAFE", observed: "UNSAFE", wantChanges: 1},
		{name: "and in the other direction", declared: "", observed: "SAFE", wantChanges: 1},
		{name: "restricted is its own level", declared: "RESTRICTED", observed: "SAFE", wantChanges: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			count := comparePostgresProperties(c, pgroutine.Properties{Parallel: test.declared}, pgroutine.Properties{Parallel: test.observed})

			c.Assert(count, qt.Equals, test.wantChanges)
		})
	}
}

// A routine's STRICT is compared, because a strict function returns NULL for a
// NULL argument without running its body. Two routines that differ only in it
// answer the same call differently.
func TestFunctionDefinitions_StrictIsCompared(t *testing.T) {
	tests := []struct {
		name        string
		declared    bool
		observed    bool
		wantChanges int
	}{
		{name: "declared strict against one that is not", declared: true, observed: false, wantChanges: 1},
		{name: "declared plain against a strict routine", declared: false, observed: true, wantChanges: 1},
		{name: "both strict", declared: true, observed: true},
		{name: "neither", declared: false, observed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			count := comparePostgresProperties(c, pgroutine.Properties{Strict: test.declared}, pgroutine.Properties{Strict: test.observed})

			c.Assert(count, qt.Equals, test.wantChanges)
		})
	}
}

func comparePostgresProperties(c *qt.C, declared, observed pgroutine.Properties) int {
	c.Helper()
	desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT 1", Facets: must.Must(pgroutine.WithProperties(schemaext.Facets{}, declared))}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	current := &catalog.Database{Functions: []catalog.Function{{Name: "f", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT 1", Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, observed))}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return len(diff.FunctionsModified)
}

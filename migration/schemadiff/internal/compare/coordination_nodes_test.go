package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// compareNodes runs the coordination node comparison over one declaration and
// one read.
func compareNodes(declared *schemamodel.Database, live *catalog.Database) (*difftypes.SchemaDiff, []coverage.Object) {
	diff := &difftypes.SchemaDiff{}
	cov := compare.CoverageOf(declared, live)
	compare.CoordinationNodes(declared, live, diff, cov)
	return diff, cov.UndecidedAdditions()
}

// TestCoordinationNodes_ComparesWhatTheNodeRunsWith pins what makes two nodes
// the same: the directory and the name, case for case, and the configuration
// the node runs with, YDB's defaults filled in on both sides.
func TestCoordinationNodes_ComparesWhatTheNodeRunsWith(t *testing.T) {
	tests := []struct {
		name         string
		declared     []schemamodel.CoordinationNode
		live         []catalog.CoordinationNode
		wantAdded    []string
		wantRemoved  []string
		wantModified []difftypes.CoordinationNodeChange
	}{
		{
			name:      "declared and absent",
			declared:  []schemamodel.CoordinationNode{{Schema: "app", Name: "locks"}},
			wantAdded: []string{"app.locks"},
		},
		{
			name:        "present and not declared",
			live:        []catalog.CoordinationNode{{Schema: "app", Name: "locks"}},
			wantRemoved: []string{"app.locks"},
		},
		{
			name:        "a name that differs in case is another node",
			declared:    []schemamodel.CoordinationNode{{Name: "Locks"}},
			live:        []catalog.CoordinationNode{{Name: "locks"}},
			wantAdded:   []string{"Locks"},
			wantRemoved: []string{"locks"},
		},
		{
			name:        "a node in another directory is another node",
			declared:    []schemamodel.CoordinationNode{{Schema: "a", Name: "locks"}},
			live:        []catalog.CoordinationNode{{Schema: "b", Name: "locks"}},
			wantAdded:   []string{"a.locks"},
			wantRemoved: []string{"b.locks"},
		},
		{
			name:        "a dotted name is one name",
			declared:    []schemamodel.CoordinationNode{{Name: "app.locks"}},
			live:        []catalog.CoordinationNode{{Schema: "app", Name: "locks"}},
			wantAdded:   []string{`"app.locks"`},
			wantRemoved: []string{"app.locks"},
		},
		{
			name: "settings at their defaults against a node that never had them",
			declared: []schemamodel.CoordinationNode{{Name: "locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 1000, SessionGracePeriodMillis: 10000, ReadConsistencyMode: "relaxed",
				AttachConsistencyMode: "strict", RateLimiterCountersMode: "aggregated",
			}}},
			live: []catalog.CoordinationNode{{Name: "locks"}},
		},
		{
			name:     "a node set to the defaults against a declaration that names none",
			declared: []schemamodel.CoordinationNode{{Name: "locks"}},
			live: []catalog.CoordinationNode{{Name: "locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 1000, AttachConsistencyMode: "strict",
			}}},
		},
		{
			name: "a changed setting, and one the declaration leaves to the default",
			declared: []schemamodel.CoordinationNode{{Schema: "app", Name: "locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2000, ReadConsistencyMode: "strict",
			}}},
			live: []catalog.CoordinationNode{{Schema: "app", Name: "locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2000, ReadConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			}}},
			wantModified: []difftypes.CoordinationNodeChange{{
				Schema: "app", Name: "locks",
				Changes: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict", RateLimiterCountersMode: "aggregated"},
				Previous: ast.CoordinationNodeSpec{
					SelfCheckPeriodMillis: 2000, ReadConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
				},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, undecided := compareNodes(
				&schemamodel.Database{CoordinationNodes: test.declared},
				&catalog.Database{CoordinationNodes: test.live},
			)

			c.Assert(nodeNames(diff.CoordinationNodesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(nodeNames(diff.CoordinationNodesRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.CoordinationNodesModified, qt.DeepEquals, test.wantModified)
			c.Assert(undecided, qt.HasLen, 0)
		})
	}
}

// A removal carries the configuration the node held, which a rollback creates
// it with.
func TestCoordinationNodes_ARemovalCarriesTheConfiguration(t *testing.T) {
	c := qt.New(t)
	held := ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2500, AttachConsistencyMode: "relaxed"}

	diff, _ := compareNodes(&schemamodel.Database{},
		&catalog.Database{CoordinationNodes: []catalog.CoordinationNode{{Schema: "app", Name: "locks", Spec: held}}})

	c.Assert(diff.CoordinationNodesRemoved, qt.DeepEquals, []schemamodel.CoordinationNode{
		{Schema: "app", Name: "locks", Spec: held},
	})
}

// Coverage decides what silence means: a declaration that declines the family
// keeps every node it does not name, and a read that did not describe nodes
// withholds an addition rather than planning a creation that fails on a node
// that is there.
func TestCoordinationNodes_HonorsCoverage(t *testing.T) {
	declined := coverage.Set{}.With(coverage.Object{Kind: coverage.CoordinationNode})
	tests := []struct {
		name          string
		declared      *schemamodel.Database
		live          *catalog.Database
		wantAdded     []string
		wantRemoved   []string
		wantUndecided []coverage.Object
	}{
		{
			name:          "a declaration that declines the family",
			declared:      &schemamodel.Database{NotDescribed: declined},
			live:          &catalog.Database{CoordinationNodes: []catalog.CoordinationNode{{Name: "app_locks"}}},
			wantUndecided: make([]coverage.Object, 0),
		},
		{
			name: "a declaration that declines one node",
			declared: &schemamodel.Database{NotDescribed: coverage.Set{}.With(
				coverage.Object{Kind: coverage.CoordinationNode, Name: "app_locks"})},
			live: &catalog.Database{CoordinationNodes: []catalog.CoordinationNode{
				{Name: "app_locks"}, {Name: "stale"},
			}},
			wantRemoved:   []string{"stale"},
			wantUndecided: make([]coverage.Object, 0),
		},
		{
			name:          "a read that did not describe nodes",
			declared:      &schemamodel.Database{CoordinationNodes: []schemamodel.CoordinationNode{{Name: "locks"}}},
			live:          &catalog.Database{NotDescribed: declined},
			wantUndecided: []coverage.Object{{Kind: coverage.CoordinationNode, Name: "locks"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, undecided := compareNodes(test.declared, test.live)

			c.Assert(nodeNames(diff.CoordinationNodesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(nodeNames(diff.CoordinationNodesRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(undecided, qt.DeepEquals, test.wantUndecided)
		})
	}
}

// nodeNames is the qualified name of each node, in order.
func nodeNames(nodes []schemamodel.CoordinationNode) []string {
	var names []string
	for _, node := range nodes {
		names = append(names, node.QualifiedName())
	}
	return names
}

package providerhost

// White-box testing is required to validate the private selection adapter's
// footprint checks independently of a provider's declared native vocabulary.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
)

func TestNativeAccessWindowsPreservePrincipalsAndAccessOrder(t *testing.T) {
	c := qt.New(t)
	selections, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	request := featureplan.Request{Identifiers: selections.selected["example.org/queue"].rules}
	nodes := []ast.Node{ast.NewCreateRole("app"), ast.NewRevokePrivilege("app", "TOPIC", "main.jobs", []string{"SELECT"}),
		ast.NewAlterRole("app"), ast.NewGrantPrivilege("app", "TOPIC", "main.jobs", []string{"SELECT"}),
		ast.NewGrantPrivilege("app", "TOPIC", "main.other", []string{"SELECT"}), ast.NewDropRole("app"),
		ast.NewGrantPrivilege("app", "TABLE", "main.table", []string{"SELECT"})}
	metadata, err := commonPlanningMetadata(request, nodes, selections)
	c.Assert(err, qt.IsNil)
	movable, edges, err := nativeAccessWindows(request, nodes, metadata, selections)
	c.Assert(err, qt.IsNil)
	c.Assert(movable, qt.DeepEquals, []int{1, 3, 4})
	c.Assert(edges, qt.DeepEquals, []featurehost.CommonDependency{{Before: 0, After: 1}, {Before: 1, After: 2}, {Before: 1, After: 3}, {Before: 1, After: 5},
		{Before: 0, After: 3}, {Before: 2, After: 3}, {Before: 3, After: 5}, {Before: 0, After: 4}, {Before: 2, After: 4}, {Before: 4, After: 5}})
}

func TestNativeAccessWindowsRefuseForeignOrMissingFootprints(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]featureplan.CommonStep)
	}{
		{"missing", func(values []featureplan.CommonStep) { values[0].Effects = nil }},
		{"foreign object", func(values []featureplan.CommonStep) {
			values[0].Effects[0].Subject.Kind = objectidentity.Kind("example.org/foreign")
		}},
		{"wrong action", func(values []featureplan.CommonStep) { values[0].Effects[1].Action = plangraph.Drop }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selections, err := newScopeSelections(scopeDescription())
			c.Assert(err, qt.IsNil)
			request := featureplan.Request{Identifiers: selections.selected["example.org/queue"].rules}
			nodes := []ast.Node{ast.NewGrantPrivilege("app", "TOPIC", "main.jobs", []string{"SELECT"})}
			metadata, err := commonPlanningMetadata(request, nodes, selections)
			c.Assert(err, qt.IsNil)
			test.mutate(metadata)
			movable, edges, err := nativeAccessWindows(request, nodes, metadata, selections)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(movable, qt.IsNil)
			c.Assert(edges, qt.IsNil)
		})
	}
}

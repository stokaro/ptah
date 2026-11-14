package lint

// White-box testing required: a zero-operation ALTER TABLE has no observable
// effect through [AnalyzeFS], so a public-API fixture could not tell this rule
// from its opposite. The node is reachable: `ALTER TABLE t;` parses to exactly
// that node and [AnalyzeFS] hands it to [nodeSchemaChanges]. What it lacks is a
// consequence. Under `search_path=public`, both `ALTER TABLE public.t;` and
// `ALTER TABLE app.users;` report no diagnostic and no schema change, so
// whether the scope removed the statement changes nothing a caller can read:
// there is no finding on it to drop.
//
// The rule becomes load-bearing the moment a rule fires on a bare ALTER TABLE,
// when it decides whether an in-scope statement's findings survive, and a rule
// nothing pins is a rule that gets inverted by the change that makes it
// observable. Which schema an index is measured in is observable, and
// scope_test.go pins it through [AnalyzeFS].

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestNodeSchemaChanges_ScopedOutMeansRejectedNotSilent pins that producing no
// change is not the same answer as being scoped out. Only the second may drop a
// statement's findings, so conflating them would silence a hazard on a table
// that is under review.
func TestNodeSchemaChanges_ScopedOutMeansRejectedNotSilent(t *testing.T) {
	tests := []struct {
		name          string
		node          ast.Node
		wantChanges   int
		wantScopedOut bool
	}{
		{
			name:          "an in-scope table with no modeled operation is not scoped out",
			node:          &ast.AlterTableNode{Name: "public.t"},
			wantChanges:   0,
			wantScopedOut: false,
		},
		{
			name:          "an out-of-scope table is scoped out",
			node:          &ast.AlterTableNode{Name: "app.t"},
			wantChanges:   0,
			wantScopedOut: true,
		},
		{
			name:          "a node naming nothing under review is not scoped out",
			node:          &ast.CommentNode{},
			wantChanges:   1,
			wantScopedOut: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			changes, scopedOut := nodeSchemaChanges(&File{}, Statement{}, test.node, newSchemaScope("public"))

			c.Assert(changes, qt.HasLen, test.wantChanges)
			c.Assert(scopedOut, qt.Equals, test.wantScopedOut)
		})
	}
}

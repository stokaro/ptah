package lint

// White-box testing required: neither rule below has an observable effect
// through [AnalyzeFS] today, so a public-API fixture could not tell either
// answer from its opposite.
//
// The reasons differ, and only one of them is unreachability. Ptah's SQL parser
// does not model DROP INDEX in any spelling, so no [ast.DropIndexNode] ever
// reaches the change extractor. A zero-operation [ast.AlterTableNode] is the
// other case and it is reachable, not unreachable: `ALTER TABLE t;` parses to
// exactly that node and [AnalyzeFS] hands it to [nodeSchemaChanges]. What it
// lacks is a consequence — measured under `search_path=public`, both
// `ALTER TABLE public.t;` and `ALTER TABLE app.users;` report no diagnostic and
// no schema change, so whether the scope removed the statement changes nothing
// a caller can read: there is no finding on it to drop.
//
// Both rules become load-bearing the moment the parser learns DROP INDEX or a
// rule fires on a bare ALTER TABLE — the first decides which schema an index
// drop is measured in, the second decides whether an in-scope statement's
// findings survive — and a rule nothing pins is a rule that gets inverted by the
// change that makes it observable.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

func TestNodeScopeReference_IndexIsMeasuredByItsTable(t *testing.T) {
	tests := []struct {
		name   string
		node   ast.Node
		object string
		want   string
	}{
		{
			name:   "a created index is measured by the table it is on",
			node:   &ast.IndexNode{Name: "idx", Table: "app.users"},
			object: "idx",
			want:   "app.users",
		},
		{
			name:   "a created index with no table recorded keeps its own name",
			node:   &ast.IndexNode{Name: "idx"},
			object: "idx",
			want:   "idx",
		},
		{
			name:   "a dropped index is measured by the table it was on",
			node:   &ast.DropIndexNode{Name: "idx", Table: "app.users"},
			object: "idx",
			want:   "app.users",
		},
		{
			name:   "a dropped index with no table recorded keeps its own name",
			node:   &ast.DropIndexNode{Name: "idx"},
			object: "idx",
			want:   "idx",
		},
		{
			name:   "every other node is measured by the object it names",
			node:   &ast.CreateTableNode{Name: "app.users"},
			object: "app.users",
			want:   "app.users",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(nodeScopeReference(test.node, test.object), qt.Equals, test.want)
		})
	}
}

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

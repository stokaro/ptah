package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/oracle"
)

// The dispatcher answers three kinds of input the typed handlers never see: a
// node that lowers to another node before it renders, a node that is part of a
// statement rather than a statement, and a node the switch does not name. Each
// is asserted here, because nothing above this package drives them: the render
// path prepares and refuses before a renderer is reached, so a dispatcher that
// answered one of them with a silent nil would emit nothing at exit 0.

// loweredRoutineRow is one routine node and the SQL its body reaches the output
// as.
type loweredRoutineRow struct {
	kind string
	node ast.Node
	want string
}

// TestVisitNode_ARoutineBodyReachesTheOutputVerbatim pins the lowering.
//
// A routine node carries a complete statement this renderer does not parse, so
// the dispatcher hands the body to the raw-SQL handler. Five node kinds do
// this, and each one that stopped doing it would reach the default arm and
// abort a render that emits the routine today.
func TestVisitNode_ARoutineBodyReachesTheOutputVerbatim(t *testing.T) {
	tests := []loweredRoutineRow{
		{
			kind: "MySQLRoutineNode",
			node: &ast.MySQLRoutineNode{SQL: "CREATE FUNCTION f() RETURNS int DETERMINISTIC RETURN 1;"},
			want: "CREATE FUNCTION f() RETURNS int DETERMINISTIC RETURN 1;\n",
		},
		{
			kind: "OpaqueRoutineNode",
			node: &ast.OpaqueRoutineNode{SQL: "CREATE PROCEDURE p AS BEGIN NULL; END;"},
			want: "CREATE PROCEDURE p AS BEGIN NULL; END;\n",
		},
		{
			kind: "PostgresDoBlockNode",
			node: &ast.PostgresDoBlockNode{SQL: "DO $$ BEGIN END $$;"},
			want: "DO $$ BEGIN END $$;\n",
		},
		{
			kind: "PostgresRoutineNode",
			node: &ast.PostgresRoutineNode{SQL: "CREATE PROCEDURE p() LANGUAGE sql AS $$ $$;"},
			want: "CREATE PROCEDURE p() LANGUAGE sql AS $$ $$;\n",
		},
		{
			kind: "SQLServerRoutineNode",
			node: &ast.SQLServerRoutineNode{SQL: "CREATE PROCEDURE p AS SELECT 1;"},
			want: "CREATE PROCEDURE p AS SELECT 1;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			c := qt.New(t)

			r := oracle.New()
			err := r.VisitNode(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(r.Output(), qt.Equals, test.want)
		})
	}
}

// TestVisitNode_AStatementListEmitsItsStatementsInOrder pins the walk.
//
// The list hands the dispatcher itself rather than its statements, so the order
// they reach the buffer in is this renderer's decision. Two statements are the
// smallest input that can tell an ordered walk from a reversed one or from a
// walk that emits only the last statement.
func TestVisitNode_AStatementListEmitsItsStatementsInOrder(t *testing.T) {
	c := qt.New(t)

	r := oracle.New()
	list := &ast.StatementList{Statements: []ast.Node{
		ast.NewComment("first"),
		ast.NewComment("second"),
	}}

	err := r.VisitNode(list)

	c.Assert(err, qt.IsNil)
	c.Assert(r.Output(), qt.Equals, "-- first\n-- second\n")
}

// TestVisitNode_AStatementListStopsAtTheFirstFailure pins that the walk reports
// a statement's refusal rather than rendering past it.
func TestVisitNode_AStatementListStopsAtTheFirstFailure(t *testing.T) {
	c := qt.New(t)

	r := oracle.New()
	list := &ast.StatementList{Statements: []ast.Node{
		ast.NewComment("first"),
		&ast.UpsertNode{Table: "users", InsertColumns: []string{"id"}, Values: []string{"1"}},
		ast.NewComment("third"),
	}}

	err := r.VisitNode(list)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(r.Output(), qt.Equals, "-- first\n")
}

// refusedNodeRow is one node the dispatcher refuses, with the sentinel and the
// message it refuses with.
type refusedNodeRow struct {
	name   string
	node   ast.Node
	wantIs error
	want   string
}

// TestVisitNode_FailurePath pins what the dispatcher answers for an input no
// typed handler owns.
//
// The message names the concrete type, because the caller that produced it is
// the only one that can say which node it meant to render.
func TestVisitNode_FailurePath(t *testing.T) {
	tests := []refusedNodeRow{
		{
			name:   "a nil node",
			node:   nil,
			wantIs: ptaherr.ErrInvalidSchemaDiff,
			want:   `invalid schema diff: oracle: the AST node is nil`,
		},
		{
			name:   "an alter operation on its own",
			node:   &ast.AddColumnOperation{Column: ast.NewColumn("email", "VARCHAR2(255)")},
			wantIs: ptaherr.ErrInvalidSchemaDiff,
			want: `invalid schema diff: oracle: \*ast\.AddColumnOperation renders as part of the ` +
				`statement that carries it, not on its own`,
		},
		{
			name:   "a constraint drop on its own",
			node:   &ast.DropConstraintOperation{ConstraintName: "uq_users_email"},
			wantIs: ptaherr.ErrInvalidSchemaDiff,
			want: `invalid schema diff: oracle: \*ast\.DropConstraintOperation renders as part of the ` +
				`statement that carries it, not on its own`,
		},
		{
			name:   "a type definition on its own",
			node:   ast.NewEnumTypeDef("happy", "sad"),
			wantIs: ptaherr.ErrInvalidSchemaDiff,
			want: `invalid schema diff: oracle: \*ast\.EnumTypeDef renders as part of the ` +
				`statement that carries it, not on its own`,
		},
		{
			name:   "a type operation on its own",
			node:   &ast.AddEnumValueOperation{Value: "neutral"},
			wantIs: ptaherr.ErrInvalidSchemaDiff,
			want: `invalid schema diff: oracle: \*ast\.AddEnumValueOperation renders as part of the ` +
				`statement that carries it, not on its own`,
		},
		{
			name:   "a node kind the switch does not name",
			node:   &unroutedNode{},
			wantIs: ptaherr.ErrUnsupportedFeature,
			want:   `unsupported feature: oracle: \*oracle_test\.unroutedNode has no handler in this renderer`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r := oracle.New()
			err := r.VisitNode(test.node)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(r.Output(), qt.Equals, "")
		})
	}
}

// unroutedNode is an ast.Node the dispatcher cannot name, which is what a node
// kind added to core/ast looks like to this renderer before a case is written
// for it.
//
// It has to be declared here rather than borrowed from core/ast: every kind
// there is routed, so nothing in the corpus can reach the default arm.
type unroutedNode struct{}

// Accept hands the visitor this node, as every ast.Node does.
func (n *unroutedNode) Accept(visitor ast.Visitor) error { return visitor.VisitNode(n) }

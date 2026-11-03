package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// A trigger or routine body can end in a line comment: the catalog keeps one
// written before the statement's semicolon, as in
//
//	CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1 -- note
//	;
//
// where information_schema.TRIGGERS reads the body back as
// `SET NEW.a = 1 -- note`. Rendered with its semicolon on the same line, the
// semicolon is part of the comment, and in a migration file the next
// statement runs into this one: MySQL 8.4 refuses such a file with Error 1064
// near the next CREATE TRIGGER. Each row renders the body and pins where the
// semicolon goes.

func TestVisitNode_TriggerBodyLineComment(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "dash comment",
			body: "SET NEW.a = 1 -- note",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1 -- note\n;\n",
		},
		{
			name: "hash comment",
			body: "SET NEW.a = 1 # note",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1 # note\n;\n",
		},
		{
			name: "comment ending in a semicolon",
			body: "SET NEW.a = 1 -- note;",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1 -- note;\n;\n",
		},
		{
			name: "block comment",
			body: "SET NEW.a = 1 /* note */",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1 /* note */;\n",
		},
		{
			name: "statement",
			body: "SET NEW.a = 1",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1;\n",
		},
		{
			name: "statement with its semicolon",
			body: "SET NEW.a = 1;",
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.a = 1;\n",
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRenderer(dialect)
				node := ast.NewCreateTrigger("trg", "t").SetTiming("BEFORE").SetEvent("INSERT").SetBody(test.body)

				c.Assert(r.VisitNode(node), qt.IsNil)
				c.Assert(r.Output(), qt.Equals, test.want)
			})
		}
	}
}

func TestVisitNode_RoutineBodyLineComment(t *testing.T) {
	tests := []struct {
		name string
		kind string
		body string
		want string
	}{
		{
			name: "function",
			kind: schemamodel.FunctionKindFunction,
			body: "RETURN 1 -- note",
			want: "CREATE FUNCTION `f`() RETURNS int READS SQL DATA RETURN 1 -- note\n;\n",
		},
		{
			name: "procedure",
			kind: schemamodel.FunctionKindProcedure,
			body: "BEGIN\n  SELECT 1;\nEND -- note",
			want: "CREATE PROCEDURE `f`() READS SQL DATA BEGIN\n  SELECT 1;\nEND -- note\n;\n",
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRenderer(dialect)
				node := ast.NewCreateFunction("f").SetKind(test.kind).SetReturns("int").SetLanguage("sql").SetBody(test.body)

				c.Assert(r.VisitNode(node), qt.IsNil)
				c.Assert(r.Output(), qt.Equals, test.want)
			})
		}
	}
}

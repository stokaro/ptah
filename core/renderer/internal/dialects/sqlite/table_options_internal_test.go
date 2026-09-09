package sqlite

// White-box testing required: the subject is sqliteTableOptionKeys, the
// unexported list writeTableOptions reports the complement of. A black-box test
// would have to spell the list again, and two copies of a list cannot disagree
// with each other in the way that matters here -- a key named as kept and not
// written is a loss nothing reports (stokaro/ptah#2976).

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/renderdiag"
)

// renderedFor names the clause each kept key produces. SQLite spells one option
// two ways in the declaration and one way in the output, which is why the list
// has three entries and this map has three keys.
var renderedFor = map[string]string{
	"STRICT":        "STRICT",
	"WITHOUT_ROWID": "WITHOUT ROWID",
	"WITHOUT ROWID": "WITHOUT ROWID",
}

// TestVisitCreateTable_RendersEveryTableOptionItKeeps binds the list to the
// writer that reads it.
//
// A key the list names is claimed to be carried, so the statement has to show
// it. Without this, deleting a clause from writeTableOptions leaves its key in
// the list and the option is lost in the silence the report exists to end.
func TestVisitCreateTable_RendersEveryTableOptionItKeeps(t *testing.T) {
	for _, key := range sqliteTableOptionKeys {
		t.Run(key, func(t *testing.T) {
			c := qt.New(t)
			clause, declared := renderedFor[key]
			c.Assert(declared, qt.IsTrue,
				qt.Commentf("%s names no clause, so this case would assert nothing", key))

			renderer := New()
			sink := &renderdiag.Sink{}
			renderer.ReportOmissionsTo(sink)

			node := ast.NewCreateTable("users").AddColumn(ast.NewColumn("id", "INTEGER"))
			node.SetOption(key, "true")

			c.Assert(node.Accept(renderer), qt.IsNil)
			c.Assert(renderer.Output(), qt.Contains, clause)
			c.Assert(sink.Omissions(), qt.HasLen, 0)
		})
	}
}

// TestVisitCreateTable_ReportsAMySQLTableOption is the other half.
//
// Every assertion above is satisfied by a renderer that reports nothing, so an
// option SQLite has no clause for has to arrive as a record. Before this,
// SQLite dropped all four MySQL-family options and said nothing at all.
func TestVisitCreateTable_ReportsAMySQLTableOption(t *testing.T) {
	c := qt.New(t)

	renderer := New()
	sink := &renderdiag.Sink{}
	renderer.ReportOmissionsTo(sink)

	node := ast.NewCreateTable("users").AddColumn(ast.NewColumn("id", "INTEGER"))
	node.SetOption("ENGINE", "InnoDB")

	c.Assert(node.Accept(renderer), qt.IsNil)
	c.Assert(sink.Omissions(), qt.DeepEquals, []renderdiag.Omission{{
		Reason:   renderdiag.ReasonUnsupported,
		Kind:     renderdiag.TableKind,
		Name:     "users",
		Property: renderdiag.TableOptionProperty + " ENGINE",
		Detail:   "InnoDB",
	}})
	c.Assert(renderer.Output(), qt.Not(qt.Contains), "InnoDB",
		qt.Commentf("the render output is unchanged by the report; only the record is new"))
}

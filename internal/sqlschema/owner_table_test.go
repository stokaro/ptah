package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

// shelfParser reads `CREATE SHELF TABLE [IF NOT EXISTS] name`, a table its
// owner defines with no column list.
type shelfParser struct{}

func (shelfParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(claim.Text)
		values = append(values, sqlext.Value{Table: &sqlext.TableDeclaration{
			Name: words[len(words)-1], IfNotExists: strings.Contains(strings.ToUpper(claim.Text), "IF NOT EXISTS"),
		}})
	}
	return sqlext.Result{Values: values}, nil
}

// shelfOwners registers target "shelves" with an owner whose statement
// declares a table.
func shelfOwners() sqlext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/shelves",
		Targets: []engine.Target{{Name: "shelves"}},
		SQL: []sqlext.Extension{{
			Owner:    "example.org/shelves",
			Targets:  []string{"shelves"},
			Patterns: []sqlext.Pattern{{Name: "shelf", Site: sqlext.Statement, Words: []string{"CREATE", "SHELF", "TABLE"}}},
			Parser:   shelfParser{},
		}},
	})).SQL()
}

// TestReadWith_PlacesATableAnOwnersStatementDeclares reads a table an owner's
// statement declares as the CREATE TABLE every reader of a table reads,
// standing where the statement stood, with its guard, and declares it beside
// the tables of the shared grammar.
func TestReadWith_PlacesATableAnOwnersStatementDeclares(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TABLE a (id INTEGER);\nCREATE SHELF TABLE IF NOT EXISTS b;\nCREATE TABLE c (id INTEGER);\n"

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "shelves", nil, sqlschema.Options{Owners: shelfOwners()})

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 3)
	shelf, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[1]))
	c.Assert(shelf.Name, qt.Equals, "b")
	c.Assert(shelf.IfNotExists, qt.IsTrue)
	c.Assert(shelf.Columns, qt.HasLen, 0)
	c.Assert(tableNames(database), qt.DeepEquals, []string{"a", "b", "c"})
}

// TestReadWith_WithoutTheTableOwnerRefusesItsSyntax is the control: the
// shared grammar has no CREATE SHELF TABLE.
func TestReadWith_WithoutTheTableOwnerRefusesItsSyntax(t *testing.T) {
	c := qt.New(t)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE SHELF TABLE b;"), "shelves", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.ErrorMatches, `unsupported CREATE target: SHELF at position 7`)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(statements, qt.IsNil)
}

package sqlschema_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

// shapeStatement is the payload of the shape owner's statements.
type shapeStatement struct {
	Text string
}

func (*shapeStatement) Kind() schemaext.Kind { return "example.org/shape/statement" }

func (s *shapeStatement) CloneExtension() ast.ExtensionPayload { return new(*s) }

// shapeParser reads `CREATE TYPE name AS (` and `CREATE TYPE name AS RANGE`
// as its own statements, and a COMMENT ON TYPE of a type it read; it declines
// a COMMENT ON TYPE of any other type, which the shared grammar then reads.
type shapeParser struct {
	requests *[]sqlext.Request
}

func (p shapeParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	owned := make(map[string]bool)
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(claim.Text)
		if claim.Pattern == "shape" {
			owned[words[2]] = true
			values = append(values, sqlext.Value{Statement: &shapeStatement{Text: claim.Text}})
			continue
		}
		if !owned[words[3]] {
			values = append(values, sqlext.Value{Declined: true})
			continue
		}
		values = append(values, sqlext.Value{Statement: &shapeStatement{Text: claim.Text}})
	}
	return sqlext.Result{Values: values}, nil
}

// shapeOwners registers target "custom" with the shape owner, through the
// runtime a command builds.
func shapeOwners(parser shapeParser) sqlext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/shape",
		Targets: []engine.Target{{Name: "custom"}},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/shape",
			Targets: []string{"custom"},
			Patterns: []sqlext.Pattern{
				{Name: "shape", Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS"}, Next: []string{"(", "RANGE"}},
				{Name: "comment", Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "TYPE"}},
			},
			Parser: parser,
		}},
	})).SQL()
}

// nodeTypes names the type of each statement, in order.
func nodeTypes(statements *ast.StatementList) []string {
	names := make([]string, 0, len(statements.Statements))
	for _, statement := range statements.Statements {
		names = append(names, reflect.TypeOf(statement).Elem().Name())
	}
	return names
}

// TestReadWith_ClaimsAStatementByTheWordsAfterItsName pins a pattern that
// reads the statement's name and claims it by the words after: a quoted
// qualified name, a bare qualified name, a bracketed one with a closing
// bracket inside it, and a statement whose words after the name the pattern
// does not take, which stays with the shared grammar.
func TestReadWith_ClaimsAStatementByTheWordsAfterItsName(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	source := "CREATE TYPE \"app\".\"addr\" AS (street TEXT);\nCREATE TYPE app.span AS RANGE (subtype = int4);\n" +
		"CREATE TYPE [dbo].[odd]]name] AS (x INT);\nCREATE TYPE mood AS ENUM ('ok', 'bad');\n"

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "custom", nil,
		sqlschema.Options{Owners: shapeOwners(shapeParser{requests: &requests})})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 3)
	c.Assert(requests[0].Claims[0].Text, qt.Equals, `CREATE TYPE "app"."addr" AS (street TEXT)`)
	c.Assert(requests[0].Claims[1].Text, qt.Equals, "CREATE TYPE app.span AS RANGE (subtype = int4)")
	c.Assert(requests[0].Claims[2].Text, qt.Equals, "CREATE TYPE [dbo].[odd]]name] AS (x INT)")
	c.Assert(nodeTypes(statements), qt.DeepEquals, []string{"ExtensionStatement", "ExtensionStatement", "ExtensionStatement", "EnumNode"})
	c.Assert(database.Enums, qt.HasLen, 1)
}

// TestReadWith_ReadsADeclinedStatementWhereItStood pins a statement the
// owner declined: the shared grammar reads it by itself and it stands where
// it stood, between two statements and as the last statement of a document,
// and what it says reaches the model, here the comment of a common enum.
func TestReadWith_ReadsADeclinedStatementWhereItStood(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{name: "between two statements",
			source: "CREATE TYPE mood AS ENUM ('ok');\nCOMMENT ON TYPE mood IS 'feelings';\nCREATE TABLE t (id INTEGER);\n",
			want:   []string{"EnumNode", "CommentNode", "CreateTableNode"}},
		{name: "as the last statement",
			source: "CREATE TYPE mood AS ENUM ('ok');\nCREATE TABLE t (id INTEGER);\nCOMMENT ON TYPE mood IS 'feelings'",
			want:   []string{"EnumNode", "CreateTableNode", "CommentNode"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request

			database, statements, err := sqlschema.ReadWith(t.Context(), []byte(test.source), "custom", nil,
				sqlschema.Options{Owners: shapeOwners(shapeParser{requests: &requests})})

			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			c.Assert(nodeTypes(statements), qt.DeepEquals, test.want)
			c.Assert(database.Enums, qt.HasLen, 1)
			c.Assert(database.Enums[0].Comment, qt.Equals, "feelings")
		})
	}
}

// TestReadWith_KeepsAStatementTheOwnerAnswered is the control: a COMMENT ON
// TYPE of a type the owner read stays the owner's statement.
func TestReadWith_KeepsAStatementTheOwnerAnswered(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	source := "CREATE TYPE addr AS (street TEXT);\nCOMMENT ON TYPE addr IS 'where';\n"

	_, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "custom", nil,
		sqlschema.Options{Owners: shapeOwners(shapeParser{requests: &requests})})

	c.Assert(err, qt.IsNil)
	c.Assert(nodeTypes(statements), qt.DeepEquals, []string{"ExtensionStatement", "ExtensionStatement"})
	c.Assert(statements.Statements[1], qt.DeepEquals, ast.Node(&ast.ExtensionStatement{Payload: &shapeStatement{Text: "COMMENT ON TYPE addr IS 'where'"}}))
}

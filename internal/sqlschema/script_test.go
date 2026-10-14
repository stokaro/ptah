package sqlschema_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// errUnreadable is the parse refusal of gadgetScript.
var errUnreadable = errors.New("unreadable gadget document")

// gadget is an owner's statement that declares a role named by the payload.
type gadget struct{ name string }

func (*gadget) Kind() schemaext.Kind                   { return "example.org/gadget" }
func (w *gadget) CloneExtension() ast.ExtensionPayload { return &gadget{name: w.name} }

// polish is an owner's ALTER TABLE operation that sets the table's comment.
// It names its clause for a refusal.
type polish struct{}

func (*polish) Kind() schemaext.Kind                 { return "example.org/polish" }
func (*polish) CloneExtension() ast.ExtensionPayload { return &polish{} }
func (*polish) AlterClause() string                  { return "POLISH" }

// unnamed is an owner's ALTER TABLE operation the owner does not apply and
// that names no clause.
type unnamed struct{}

func (*unnamed) Kind() schemaext.Kind                 { return "example.org/unnamed" }
func (*unnamed) CloneExtension() ast.ExtensionPayload { return &unnamed{} }

// scopeSeen is what gadgetScript saw of a scope it was handed.
type scopeSeen struct {
	Statement    string
	BaseTables   int
	DatabasePath string
}

// gadgetScript is an owner whose grammar parses a document by looking it up
// in documents, converts its gadgets and polishes, and records what it saw.
type gadgetScript struct {
	documents map[string]func() []ast.Node
	seen      *[]scopeSeen
}

func (s gadgetScript) Parse(_ context.Context, file sqlext.SourceFile) (*ast.StatementList, error) {
	build, ok := s.documents[file.Text]
	if !ok {
		return nil, errUnreadable
	}
	return &ast.StatementList{Statements: build()}, nil
}

func (s gadgetScript) Statement(scope sqlschema.Scope, statement ast.Node) (bool, error) {
	extension, ok := statement.(*ast.ExtensionStatement)
	if !ok {
		return false, nil
	}
	declared, ok := extension.Payload.(*gadget)
	if !ok {
		return false, nil
	}
	*s.seen = append(*s.seen, scopeSeen{Statement: declared.name, BaseTables: len(scope.Base().Tables), DatabasePath: scope.DatabasePath()})
	database := scope.Database()
	database.Roles = append(database.Roles, schemamodel.Role{Name: scope.Name(declared.name)})
	return true, nil
}

func (s gadgetScript) AlterOperation(scope sqlschema.Scope, table sqlschema.Table, operation *ast.ExtensionAlterOperation) (bool, error) {
	if _, ok := operation.Payload.(*polish); !ok {
		return false, nil
	}
	*s.seen = append(*s.seen, scopeSeen{Statement: "polish " + table.Qualified, BaseTables: len(scope.Base().Tables), DatabasePath: scope.DatabasePath()})
	table.Declared.Comment = "polished"
	return true, nil
}

// gadgetDocuments are the documents gadgetScript reads, by their text.
func gadgetDocuments() map[string]func() []ast.Node {
	alter := func(table string, payload ast.ExtensionPayload) *ast.AlterTableNode {
		return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}}
	}
	return map[string]func() []ast.Node{
		"tables": func() []ast.Node {
			table := ast.NewCreateTable("t")
			table.Columns = []*ast.ColumnNode{ast.NewColumn("id", "INTEGER")}
			return []ast.Node{table, &ast.ExtensionStatement{Payload: &gadget{name: "first"}}}
		},
		"changes": func() []ast.Node {
			return []ast.Node{alter("t", &polish{}), &ast.ExtensionStatement{Payload: &gadget{name: "second"}}}
		},
		"polish missing":   func() []ast.Node { return []ast.Node{alter("missing", &polish{})} },
		"unnamed":          func() []ast.Node { return []ast.Node{alter("t", &unnamed{})} },
		"unmodeled":        func() []ast.Node { return []ast.Node{&ast.ExtensionStatement{Payload: &polish{}}} },
		"declares nothing": func() []ast.Node { return nil },
	}
}

// TestConvertScript_HappyPath converts a load the owner parsed: the shared
// conversion declares the table, the owner converts its own statement in each
// file with the earlier files as the base, and applies its operation to the
// table an earlier file declared, which the shared conversion resolved.
func TestConvertScript_HappyPath(t *testing.T) {
	c := qt.New(t)
	var seen []scopeSeen
	request := sqlext.GrammarRequest{Target: "custom", DatabasePath: "/root/db", Files: []sqlext.SourceFile{
		{Path: "a.sql", Text: "tables"}, {Path: "b.sql", Text: "changes"},
	}}

	result, err := sqlschema.ConvertScript(t.Context(), request, gadgetScript{documents: gadgetDocuments(), seen: &seen})

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.DeepEquals, []scopeSeen{
		{Statement: "first", DatabasePath: "/root/db"},
		{Statement: "polish t", BaseTables: 1, DatabasePath: "/root/db"},
		{Statement: "second", BaseTables: 1, DatabasePath: "/root/db"},
	})
	c.Assert(result.Database.Tables, qt.HasLen, 1)
	c.Assert(result.Database.Tables[0].Comment, qt.Equals, "polished")
	c.Assert(result.Database.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "first"}, {Name: "second"}})
	c.Assert(result.Files, qt.DeepEquals, []sqlext.FileDeclarations{
		{Path: "a.sql", Objects: []sqlext.Declaration{{Kind: "table", Name: "t"}, {Kind: "role", Name: "first"}}, Tables: []sqlext.TableStatement{{Name: "t"}}},
		{Path: "b.sql", Objects: []sqlext.Declaration{{Kind: "role", Name: "second"}}},
	})
}

// TestConvertScript_FailurePath names the refused document and stops there:
// a document the owner cannot parse, an operation of the owner's on a table
// the load does not declare, which the refusal names by the payload's
// clause, an operation the owner leaves, and a statement neither the owner
// nor the shared conversion models.
func TestConvertScript_FailurePath(t *testing.T) {
	tests := []struct {
		name, text, wantErr string
		wantIs              error
	}{
		{name: "unparsed", text: "garbage", wantErr: `b.sql: unreadable gadget document`, wantIs: errUnreadable},
		{name: "undeclared table", text: "polish missing", wantIs: sqlext.ErrUndeclared,
			wantErr: `b.sql: the schema model has no place for this statement: ALTER TABLE missing POLISH names a table this schema does not declare`},
		{name: "operation left", text: "unnamed", wantIs: sqlschema.ErrUnmodeledStatement,
			wantErr: `b.sql: the schema model has no place for this statement: ALTER TABLE t extension operation`},
		{name: "statement left", text: "unmodeled", wantIs: sqlschema.ErrUnmodeledStatement,
			wantErr: `b.sql: the schema model has no place for this statement: \*ast.ExtensionStatement`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var seen []scopeSeen
			request := sqlext.GrammarRequest{Target: "custom", Files: []sqlext.SourceFile{
				{Path: "a.sql", Text: "tables"}, {Path: "b.sql", Text: test.text}, {Path: "c.sql", Text: "declares nothing"},
			}}

			result, err := sqlschema.ConvertScript(t.Context(), request, gadgetScript{documents: gadgetDocuments(), seen: &seen})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, sqlext.GrammarResult{})
		})
	}
}

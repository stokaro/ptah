package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/internal/bufwriter"
	"ptah.run/core/renderer/internal/dialects/mysqllike"
)

// TestVisitCreateTable_CarriesAColumnComment pins the clause on the path that
// did not write it.
//
// There are two column renderers here: the one CREATE TABLE uses and the one
// the ALTER operations use. Only the second wrote a COMMENT, so one declaration
// produced two different databases depending on whether the column arrived with
// its table or after it. Measured on MariaDB 12.3 from one HCL document, read
// back from information_schema (stokaro/ptah#2164):
//
//	applied to an empty database  -> email=[]
//	applied as ADD COLUMN         -> email=[login address]
//
// The table's own comment survived both, which is what made the loss hard to
// see: the schema looked commented.
func TestVisitCreateTable_CarriesAColumnComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{
			name:    "a comment on the column",
			comment: "login address",
			want:    "COMMENT 'login address'",
		},
		{
			// An apostrophe is the character a comment is most likely to hold,
			// and an unescaped one ends the literal and breaks the statement.
			name:    "a comment holding an apostrophe",
			comment: "the buyer's address",
			want:    "COMMENT 'the buyer''s address'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderColumnComment(c, test.comment), qt.Contains, test.want)
		})
	}
}

// The control. A column with no comment must gain no clause: COMMENT ” is a
// comment, and it is not the same as having none.
func TestVisitCreateTable_AColumnWithoutACommentGainsNoClause(t *testing.T) {
	c := qt.New(t)

	c.Assert(renderColumnComment(c, ""), qt.Not(qt.Contains), "COMMENT")
}

func renderColumnComment(c *qt.C, comment string) string {
	c.Helper()
	writer := &bufwriter.Writer{}
	renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))
	column := ast.NewColumn("email", "VARCHAR(255)")
	column.Comment = comment
	table := &ast.CreateTableNode{
		Name:    "customers",
		Columns: []*ast.ColumnNode{column},
	}

	c.Assert(renderer.VisitCreateTable(table), qt.IsNil)

	return renderer.GetOutput()
}

// The ALTER path kept the comment before the CREATE path learned to, and
// nothing pinned it: the mutation that removes it from ADD COLUMN survived
// until this test existed. Both paths now share one clause, so both are
// measured, or a later edit takes the working half down with the broken one.
func TestVisitAlterTable_AddColumnCarriesTheComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{
			name:    "a comment on the added column",
			comment: "login address",
			want:    "ADD COLUMN `email` VARCHAR(255) COMMENT 'login address';",
		},
		{
			// The control: an added column with no comment gains no clause.
			name: "no comment on the added column",
			want: "ADD COLUMN `email` VARCHAR(255);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderAddColumn(c, test.comment), qt.Contains, test.want)
		})
	}
}

func renderAddColumn(c *qt.C, comment string) string {
	c.Helper()
	writer := &bufwriter.Writer{}
	renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))
	column := ast.NewColumn("email", "VARCHAR(255)")
	column.Comment = comment
	alter := &ast.AlterTableNode{
		Name:       "customers",
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
	}

	c.Assert(renderer.VisitAlterTable(alter), qt.IsNil)

	return renderer.GetOutput()
}

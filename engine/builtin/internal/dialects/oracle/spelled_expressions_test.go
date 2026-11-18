package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// spelledTable is a table whose names reach the renderer spelled, as a read of
// an Oracle catalog or a quoted name in a SQL schema file gives them: the
// table stored as Cust and its columns stored as Id, Status and Code.
func spelledTable(status, code, twice *ast.ColumnNode) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:    `"Cust"`,
		Columns: []*ast.ColumnNode{{Name: `"Id"`, Type: "NUMBER(10)", Primary: true}, status, code, twice},
	}
}

// TestSpelledColumn_QuotedReference_HappyPath renders CHECK, DEFAULT and
// virtual-column expressions that name a spelled column the way Oracle
// requires, quoted. The expression guard looks for the name the column is
// stored under, so `"Status"` in the text is that column and is accepted; an
// unnamed CHECK is named from the stored names, Cust_Status_check, which is
// what the comparison predicts for it.
func TestSpelledColumn_QuotedReference_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table *ast.CreateTableNode
		want  string
	}{
		{
			name: "a CHECK",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true, Check: `"Status" IN ('a','b')`},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true},
			),
			want: `  "Status" VARCHAR2(5) CONSTRAINT Cust_Status_check CHECK ("Status" IN ('a','b')),`,
		},
		{
			name: "a DEFAULT",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true, Default: &ast.DefaultValue{Expression: `'c' || "Status"`}},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true},
			),
			want: `  "Code" VARCHAR2(10) DEFAULT 'c' || "Status",`,
		},
		{
			name: "a virtual column",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true, GeneratedExpression: `"Id" * 2`},
			),
			want: `  "Twice" NUMBER(10) GENERATED ALWAYS AS ("Id" * 2) VIRTUAL`,
		},
		{
			name: "a lower-case column in a CHECK",
			table: &ast.CreateTableNode{Name: "cust", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "NUMBER(10)", Primary: true},
				{Name: `"status"`, Type: "VARCHAR2(5)", Nullable: true, Check: `"status" IN ('a','b')`},
			}},
			want: `  "status" VARCHAR2(5) CONSTRAINT cust_status_check CHECK ("status" IN ('a','b'))`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(render(c, capability.Oracle23(), test.table), qt.Contains, test.want)
		})
	}
}

// TestSpelledColumn_BareReference_FailurePath refuses the same expressions
// when they name the spelled column bare. A bare status or Status folds to
// STATUS, which is not the column stored as Status, and the server answers
// with an error that names neither; the refusal names the column and the
// spelling to write instead.
func TestSpelledColumn_BareReference_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		table   *ast.CreateTableNode
		wantErr string
	}{
		{
			name: "a CHECK",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true, Check: `Status IN ('a','b')`},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true},
			),
			wantErr: `.*column "Status" of table .* needs quoting in Oracle, and the expression "Status IN \('a','b'\)" names it without quotes; .* so write "Status" in the expression`,
		},
		{
			name: "a DEFAULT",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true, Default: &ast.DefaultValue{Expression: `'c' || status`}},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true},
			),
			wantErr: `.*column "Status" of table .* the expression "'c' \|\| status" names it without quotes; .* so write "Status" in the expression`,
		},
		{
			name: "a virtual column",
			table: spelledTable(
				&ast.ColumnNode{Name: `"Status"`, Type: "VARCHAR2(5)", Nullable: true},
				&ast.ColumnNode{Name: `"Code"`, Type: "VARCHAR2(10)", Nullable: true},
				&ast.ColumnNode{Name: `"Twice"`, Type: "NUMBER(10)", Nullable: true, GeneratedExpression: `Id * 2`},
			),
			wantErr: `.*column "Id" of table .* the expression "Id \* 2" names it without quotes; .* so write "Id" in the expression`,
		},
		{
			name: "a lower-case column in a CHECK",
			table: &ast.CreateTableNode{Name: "cust", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "NUMBER(10)", Primary: true},
				{Name: `"status"`, Type: "VARCHAR2(5)", Nullable: true, Check: `status IN ('a','b')`},
			}},
			wantErr: `.*column "status" of table "cust" needs quoting in Oracle, and the expression "status IN \('a','b'\)" names it without quotes; .* so write "status" in the expression`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderErr(c, capability.Oracle23(), test.table), qt.ErrorMatches, test.wantErr)
		})
	}
}

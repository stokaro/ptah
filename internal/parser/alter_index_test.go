package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// PostgreSQL renames an index with ALTER INDEX, which names the index alone:
// bare, schema-qualified, or quoted. Each form is read into the node that
// carries it, the spelling kept for the reader to fold.
func TestParse_AlterIndex_HappyPath(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want ast.AlterIndexNode
	}{
		{
			name: "a bare name",
			sql:  "ALTER INDEX ix RENAME TO other;",
			want: ast.AlterIndexNode{Name: "ix", NewName: "other"},
		},
		{
			name: "IF EXISTS and a schema",
			sql:  "ALTER INDEX IF EXISTS app.ix RENAME TO other;",
			want: ast.AlterIndexNode{Name: "app.ix", IfExists: true, NewName: "other"},
		},
		{
			name: "quoted names",
			sql:  `ALTER INDEX "App"."Ix" RENAME TO "Other";`,
			want: ast.AlterIndexNode{Name: `"App"."Ix"`, NewName: `"Other"`},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.DeepEquals, []ast.Node{&row.want})
		})
	}
}

// An ALTER INDEX action other than RENAME TO changes nothing a schema file
// holds, so it is refused by name, and so is a qualified new name, which
// PostgreSQL's grammar does not take. On another engine the statement stays
// unsupported: MySQL has no ALTER INDEX, and the engines that do are not
// measured.
func TestParse_AlterIndex_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name: "SET TABLESPACE", dialect: "postgres",
			sql:     "ALTER INDEX ix SET TABLESPACE pg_default;",
			wantErr: `ALTER INDEX ix SET at position \d+: only RENAME TO is read; an index's other settings are not part of a schema file`,
		},
		{
			name: "ATTACH PARTITION", dialect: "postgres",
			sql:     "ALTER INDEX ix ATTACH PARTITION ix_2026;",
			wantErr: `ALTER INDEX ix ATTACH at position \d+: only RENAME TO is read; .*`,
		},
		{
			name: "RENAME without TO", dialect: "postgres",
			sql:     "ALTER INDEX ix RENAME other;",
			wantErr: `expected TO after ALTER INDEX ix RENAME: .*`,
		},
		{
			name: "a qualified new name", dialect: "postgres",
			sql:     "ALTER INDEX ix RENAME TO app.other;",
			wantErr: `ALTER INDEX ix RENAME TO app\. at position \d+: the new name is bare, because the index stays in its schema`,
		},
		{
			name: "IF without EXISTS", dialect: "postgres",
			sql:     "ALTER INDEX IF ix RENAME TO other;",
			wantErr: `expected EXISTS after ALTER INDEX IF: .*`,
		},
		{
			name: "MySQL", dialect: "mysql",
			sql:     "ALTER INDEX ix RENAME TO other;",
			wantErr: `unsupported ALTER target: INDEX at position \d+`,
		},
		{
			name: "CockroachDB, an index named without its table", dialect: "cockroachdb",
			sql:     "ALTER INDEX ix RENAME TO other;",
			wantErr: `ALTER INDEX ix at position \d+: name the index through its table, as table@index`,
		},
		{
			name: "CockroachDB, a rename", dialect: "cockroachdb",
			sql:     "ALTER INDEX c@ix RENAME TO other;",
			wantErr: `ALTER INDEX c@ix RENAME at position \d+: only VISIBLE and NOT VISIBLE are read; .*`,
		},
		{
			name: "no dialect", dialect: "",
			sql:     "ALTER INDEX ix RENAME TO other;",
			wantErr: `unsupported ALTER target: INDEX at position \d+`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect(row.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

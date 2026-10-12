package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/internal/parser"
)

// parseMySQL parses sql in dialect with the MySQL owner's column options.
func parseMySQL(dialect, sql string) (*ast.StatementList, error) {
	return parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(must.Must(sqlext.NewSet(mysqlsource.SQL())))).Parse()
}

// columnSettings answers the settings of each column of a statement's table.
func columnSettings(c *qt.C, node ast.Node) []mysqlschema.ColumnSettings {
	c.Helper()
	var columns []*ast.ColumnNode
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		columns = typed.Columns
	case *ast.AlterTableNode:
		for _, operation := range typed.Operations {
			columns = append(columns, operation.(*ast.ModifyColumnOperation).Column)
		}
	}
	settings := make([]mysqlschema.ColumnSettings, 0, len(columns))
	for _, column := range columns {
		value, _, err := mysqlschema.Settings(column.Facets)
		c.Assert(err, qt.IsNil)
		settings = append(settings, value)
	}
	return settings
}

// A column's character set and ON UPDATE clause are read into one facet,
// whatever order they come in and whatever host clauses stand between them;
// a clause written twice keeps the last, as MySQL does; and each column keeps
// its own.
func TestSQL_ReadsColumnSettings(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []mysqlschema.ColumnSettings
	}{
		{name: "both, around host clauses", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (ts timestamp CHARSET utf8mb4 NOT NULL ON UPDATE CURRENT_TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP COMMENT 'x');",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP(6)"}}},
		{name: "the last of two", dialect: platform.MariaDB,
			sql:  "CREATE TABLE t (v varchar(10) CHARACTER SET latin1 COLLATE latin1_bin CHARSET utf8mb4);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}}},
		{name: "two options side by side", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (b timestamp CHARSET latin1 ON UPDATE NOW() CHARACTER SET utf8mb4);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4", OnUpdate: "NOW()"}}},
		{name: "each column its own", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a varchar(10) CHARSET latin1, b timestamp ON UPDATE NOW(), c int);",
			want: []mysqlschema.ColumnSettings{{Charset: "latin1"}, {OnUpdate: "NOW()"}, {}}},
		{name: "a modified column", dialect: platform.MySQL,
			sql:  "ALTER TABLE t MODIFY COLUMN ts timestamp ON UPDATE CURRENT_TIMESTAMP NOT NULL;",
			want: []mysqlschema.ColumnSettings{{OnUpdate: "CURRENT_TIMESTAMP"}}},
		{name: "a document that names no target", dialect: "",
			sql:  "CREATE TABLE t (v varchar(10) character set utf8mb4 not null);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(columnSettings(c, statements.Statements[0]), qt.DeepEquals, test.want)
		})
	}
}

// A malformed option is refused at its position, an option of another
// dialect's document is no column attribute the grammar reads, and a CHARACTER
// without SET opens no option.
func TestSQL_RefusesAMalformedOption(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{name: "no character set name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARACTER SET 'utf8');",
			wantErr: `expected a character set name after CHARACTER SET at position 37`},
		{name: "no charset name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARSET 'utf8');",
			wantErr: `expected charset name: expected identifier, got String at position 31`},
		{name: "words after the name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARSET utf8 BINARY);",
			wantErr: `unexpected "BINARY" after the character set at position 36`},
		{name: "no expression", dialect: platform.MySQL, sql: "CREATE TABLE t (ts timestamp ON UPDATE NOT NULL);",
			wantErr: `expected an expression after ON UPDATE at position 39`},
		{name: "CHARACTER without SET", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARACTER utf8);",
			wantErr: `unsupported column attribute: CHARACTER at position 23`},
		{name: "another dialect", dialect: platform.Postgres, sql: "CREATE TABLE t (v text CHARSET utf8);",
			wantErr: `unsupported column attribute: CHARSET at position 23`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(test.dialect, test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// An option ends where the next clause of the column begins, whichever
// clause the grammar reads there, so the clause is read by the grammar and
// not refused as part of the option.
func TestSQL_AnOptionEndsAtTheNextClause(t *testing.T) {
	for _, clause := range []string{
		"NOT NULL", "NULL", "PRIMARY KEY", "UNIQUE", "AUTO_INCREMENT", "DEFAULT 'x'", "CHECK (v <> '')",
		"CONSTRAINT v_set CHECK (v <> '')", "REFERENCES p (v)", "COLLATE utf8mb4_bin", "COMMENT 'x'",
		"AS (concat(v, 'x'))", "GENERATED ALWAYS AS (concat(v, 'x'))",
	} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL("", "CREATE TABLE t (v varchar(10) CHARSET utf8mb4 "+clause+");")

			c.Assert(err, qt.IsNil)
			c.Assert(columnSettings(c, statements.Statements[0]), qt.DeepEquals, []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}})
		})
	}
}

// The clauses the grammar reads only after a constraint end an option too, so
// the grammar, and not the owner, answers one written after an option.
func TestSQL_AnOptionEndsAtAClauseOfAConstraint(t *testing.T) {
	for _, test := range []struct {
		clause  string
		wantErr string
	}{
		{clause: "ENFORCED", wantErr: `misplaced ENFORCED clause at position 46: .*`},
		{clause: "DEFERRABLE", wantErr: `misplaced deferral clause at position 46: .*`},
		{clause: "INITIALLY DEFERRED", wantErr: `misplaced deferral clause at position 46: .*`},
	} {
		t.Run(test.clause, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL("", "CREATE TABLE t (v varchar(10) CHARSET utf8mb4 "+test.clause+");")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

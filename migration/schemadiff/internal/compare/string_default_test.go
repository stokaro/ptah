package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A string default and an expression default are different defaults on a text
// column, even where the expression's text is the string's characters: one
// inserts `1 + 2`, the other inserts 3. A quoted literal differs from an
// expression the catalog reports, and an expression from a quoted string.
func TestColumns_AStringDefaultIsNoExpression(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		reported                  string
		want                      map[string]string
	}{
		{
			name: "Oracle, the string a sequence reference spells", dialect: platform.Oracle, columnType: "VARCHAR2(30)",
			literal: "'app.seq.NEXTVAL'", reported: `"APP"."SEQ"."NEXTVAL"`,
			want: map[string]string{"default": `"APP"."SEQ"."NEXTVAL" -> 'app.seq.NEXTVAL'`},
		},
		{
			name: "SQL Server, the string a sequence clause spells", dialect: platform.SQLServer, columnType: "nvarchar(30)",
			literal: "'NEXT VALUE FOR s'", reported: "(NEXT VALUE FOR [s])",
			want: map[string]string{"default_expr": "(NEXT VALUE FOR [s]) -> 'NEXT VALUE FOR s'"},
		},
		{
			name: "MySQL, the string the expression spells", dialect: platform.MySQL, columnType: "varchar(30)",
			expression: "(1 + 2)", reported: "'1 + 2'",
			want: map[string]string{"default": "'1 + 2' -> (1 + 2)"},
		},
		{
			name: "MySQL, a call as a string", dialect: platform.MySQL, columnType: "varchar(30)",
			expression: "(now())", reported: "'now()'",
			want: map[string]string{"default": "'now()' -> (now())"},
		},
		{
			name: "MySQL, a string with the parentheses", dialect: platform.MySQL, columnType: "varchar(30)",
			expression: "(1 + 2)", reported: "'(1 + 2)'",
			want: map[string]string{"default": "'(1 + 2)' -> (1 + 2)"},
		},
		{
			name: "MySQL, a declared string against the expression", dialect: platform.MySQL, columnType: "varchar(30)",
			literal: "'(1 + 2)'", reported: "(1 + 2)",
			want: map[string]string{"default_expr": "(1 + 2) -> '(1 + 2)'"},
		},
		{
			name: "MySQL, a declared string against a call", dialect: platform.MySQL, columnType: "varchar(40)",
			literal: "'uuid()'", reported: "(uuid())",
			want: map[string]string{"default_expr": "(uuid()) -> 'uuid()'"},
		},
		{
			name: "MySQL, an expression that is a string against the expression", dialect: platform.MySQL, columnType: "varchar(30)",
			expression: "('1 + 2')", reported: "(1 + 2)",
			want: map[string]string{"default_expr": "(1 + 2) -> ('1 + 2')"},
		},
		{
			name: "MariaDB, the string the expression spells", dialect: platform.MariaDB, columnType: "varchar(30)",
			expression: "(1 + 2)", reported: "'1 + 2'",
			want: map[string]string{"default": "'1 + 2' -> (1 + 2)"},
		},
		{
			name: "MariaDB, a declared string against the expression", dialect: platform.MariaDB, columnType: "varchar(30)",
			literal: "'1 + 2'", reported: "1 + 2",
			want: map[string]string{"default_expr": "1 + 2 -> '1 + 2'"},
		},
		{
			name: "PostgreSQL, a call as a string", dialect: platform.Postgres, columnType: "text",
			expression: "now()", reported: "'now()'::text",
			want: map[string]string{"default_expr": "'now()'::text -> now()"},
		},
		{
			name: "PostgreSQL, a declared string against a call", dialect: platform.Postgres, columnType: "text",
			literal: "'now()'", reported: "now()",
			want: map[string]string{"default_expr": "now() -> 'now()'"},
		},
		{
			name: "SQLite, the string the expression spells", dialect: platform.SQLite, columnType: "TEXT",
			expression: "(1 + 2)", reported: "'1 + 2'",
			want: map[string]string{"default": "'1 + 2' -> (1 + 2)"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultExpr: row.expression}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, row.want)
		})
	}
}

// A string compared with a string, a number with a number, a literal a model
// writes without quotes, and a column that does not store text are compared
// as before: the quotes and the casts a catalog adds are its spelling of the
// same default.
func TestColumns_AStringDefaultMatchesItsOwnSpellings(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		set                       bool
		reported                  string
	}{
		{name: "MySQL, a quoted literal", dialect: platform.MySQL, columnType: "varchar(30)", literal: "'1 + 2'", reported: "'1 + 2'"},
		{name: "MySQL, an unquoted literal", dialect: platform.MySQL, columnType: "varchar(30)", literal: "abc", reported: "'abc'"},
		{name: "an unquoted literal against the catalog a model makes", dialect: platform.MySQL, columnType: "varchar(16)", literal: "x", reported: "x"},
		{name: "an unquoted literal with a space against the catalog a model makes", dialect: platform.Postgres, columnType: "text", literal: "a b", reported: "a b"},
		{name: "MySQL, an expression against itself", dialect: platform.MySQL, columnType: "varchar(30)", expression: "(1 + 2)", reported: "(1 + 2)"},
		{name: "PostgreSQL, a cast literal", dialect: platform.Postgres, columnType: "varchar(30)", literal: "'a'", reported: "'a'::character varying"},
		{name: "PostgreSQL, an expression that is a string", dialect: platform.Postgres, columnType: "text", expression: "('a')", reported: "'a'::text"},
		{name: "PostgreSQL, a number on a varchar", dialect: platform.Postgres, columnType: "varchar(10)", literal: "0", reported: "0"},
		{name: "PostgreSQL, a quoted number against a bare one", dialect: platform.Postgres, columnType: "varchar(10)", literal: "'0'", reported: "0"},
		{name: "PostgreSQL, a number expression against a quoted number", dialect: platform.Postgres, columnType: "varchar(10)", expression: "0", reported: "'0'::character varying"},
		{name: "MySQL, an empty string against the empty default the catalog reports", dialect: platform.MySQL, columnType: "varchar(10)", literal: "''", reported: ""},
		{name: "PostgreSQL, an empty literal a model declares against an empty string", dialect: platform.Postgres, columnType: "varchar(10)", set: true, reported: "''::character varying"},
		{name: "PostgreSQL, a number expression on a text column", dialect: platform.Postgres, columnType: "text", expression: "(1)", reported: "1"},
		{name: "PostgreSQL, a quoted number on an integer", dialect: platform.Postgres, columnType: "integer", literal: "-1", reported: "'-1'::integer"},
		{name: "Oracle, a literal on a CLOB", dialect: platform.Oracle, columnType: "CLOB", literal: "none", reported: "'none'"},
		{name: "Oracle, a sequence reference on a text column", dialect: platform.Oracle, columnType: "VARCHAR2(30)", expression: "app.seq.NEXTVAL", reported: `"APP"."SEQ"."NEXTVAL"`},
		{name: "SQL Server, a sequence clause on a text column", dialect: platform.SQLServer, columnType: "nvarchar(30)", expression: "NEXT VALUE FOR s", reported: "(NEXT VALUE FOR [s])"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultSet: row.set, DefaultExpr: row.expression}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, make(map[string]string))
		})
	}
}

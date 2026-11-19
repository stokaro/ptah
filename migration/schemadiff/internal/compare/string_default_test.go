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
// expression the catalog reports, and an expression from a quoted string. A
// literal a struct tag or a YAML document declares without quotes is a string
// too, so it differs from an expression spelled with the same characters.
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
			name: "MySQL, a bare literal against the expression", dialect: platform.MySQL, columnType: "varchar(30)",
			literal: "1 + 2", reported: "(1 + 2)",
			want: map[string]string{"default_expr": "(1 + 2) -> 1 + 2"},
		},
		{
			name: "MySQL, a bare literal against the expression it spells", dialect: platform.MySQL, columnType: "varchar(30)",
			literal: "(1 + 2)", reported: "(1 + 2)",
			want: map[string]string{"default_expr": "(1 + 2) -> (1 + 2)"},
		},
		{
			name: "MySQL, a bare literal against a call", dialect: platform.MySQL, columnType: "varchar(30)",
			literal: "now()", reported: "(now())",
			want: map[string]string{"default_expr": "(now()) -> now()"},
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
			name: "MariaDB, a bare literal against the expression", dialect: platform.MariaDB, columnType: "varchar(30)",
			literal: "1 + 2", reported: "1 + 2",
			want: map[string]string{"default_expr": "1 + 2 -> 1 + 2"},
		},
		{
			name: "PostgreSQL, a bare literal against the call it spells", dialect: platform.Postgres, columnType: "varchar(30)",
			literal: "now()", reported: "now()",
			want: map[string]string{"default_expr": "now() -> now()"},
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
		{
			name: "SQLite, a bare literal against the expression it spells", dialect: platform.SQLite, columnType: "TEXT",
			literal: "(1 + 2)", reported: "(1 + 2)",
			want: map[string]string{"default_expr": "(1 + 2) -> (1 + 2)"},
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

// A string compared with a string, a number with a number, and a column that
// does not store text are compared as values: the quotes and the casts a
// catalog adds are its spelling of the same default. A literal a model declares
// without quotes is that string, and the catalog a model makes holds it quoted.
func TestColumns_AStringDefaultMatchesItsOwnSpellings(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		set                       bool
		reported                  string
	}{
		{name: "MySQL, a quoted literal", dialect: platform.MySQL, columnType: "varchar(30)", literal: "'1 + 2'", reported: "'1 + 2'"},
		{name: "MySQL, an unquoted literal", dialect: platform.MySQL, columnType: "varchar(30)", literal: "abc", reported: "'abc'"},
		{name: "an unquoted literal against the catalog a model makes", dialect: platform.MySQL, columnType: "varchar(16)", literal: "x", reported: "'x'"},
		{name: "an unquoted literal with a space against the catalog a model makes", dialect: platform.Postgres, columnType: "text", literal: "a b", reported: "'a b'"},
		{name: "an unquoted literal with a quote against a catalog", dialect: platform.Postgres, columnType: "text", literal: "it's", reported: "'it''s'::text"},
		{name: "an empty literal against a catalog", dialect: platform.MySQL, columnType: "varchar(16)", literal: "", set: true, reported: "''"},
		{name: "ClickHouse, a quote escaped with a backslash", dialect: platform.ClickHouse, columnType: "String", literal: `'it\'s;x'`, reported: `'it\'s;x'`},
		{name: "MySQL, an expression against itself", dialect: platform.MySQL, columnType: "varchar(30)", expression: "(1 + 2)", reported: "(1 + 2)"},
		{name: "PostgreSQL, a cast literal", dialect: platform.Postgres, columnType: "varchar(30)", literal: "'a'", reported: "'a'::character varying"},
		{name: "PostgreSQL, an expression that is a string", dialect: platform.Postgres, columnType: "text", expression: "('a')", reported: "'a'::text"},
		{name: "PostgreSQL, a number on a varchar", dialect: platform.Postgres, columnType: "varchar(10)", literal: "0", reported: "0"},
		{name: "PostgreSQL, a quoted number against a bare one", dialect: platform.Postgres, columnType: "varchar(10)", literal: "'0'", reported: "0"},
		{name: "PostgreSQL, a number expression against a quoted number", dialect: platform.Postgres, columnType: "varchar(10)", expression: "0", reported: "'0'::character varying"},
		{name: "PostgreSQL, an empty literal a model declares against an empty string", dialect: platform.Postgres, columnType: "varchar(10)", set: true, reported: "''::character varying"},
		{name: "PostgreSQL, a number expression on a text column", dialect: platform.Postgres, columnType: "text", expression: "(1)", reported: "1"},
		{name: "PostgreSQL, a quoted number on an integer", dialect: platform.Postgres, columnType: "integer", literal: "-1", reported: "'-1'::integer"},
		{name: "Oracle, a literal on a CLOB", dialect: platform.Oracle, columnType: "CLOB", literal: "none", reported: "'none'"},
		{name: "MySQL, a bare literal with a backslash", dialect: platform.MySQL, columnType: "varchar(30)", literal: `a\b`, reported: `'a\\b'`},
		{name: "MariaDB, a bare literal with a backslash before a quote", dialect: platform.MariaDB, columnType: "varchar(30)", literal: `it\'s`, reported: `'it\\''s'`},
		{name: "MySQL, a quote escaped with a backslash against a doubled one", dialect: platform.MySQL, columnType: "varchar(30)", literal: `'it\'s'`, reported: `'it''s'`},
		{name: "ClickHouse, a bare literal with a backslash", dialect: platform.ClickHouse, columnType: "String", literal: `a\b`, reported: `'a\\b'`},
		{name: "ClickHouse, a bare literal with a quote", dialect: platform.ClickHouse, columnType: "Nullable(String)", literal: `it's;x`, reported: `'it\'s;x'`},
		{name: "PostgreSQL, a bare literal with a backslash", dialect: platform.Postgres, columnType: "text", literal: `a\b`, reported: `'a\b'::text`},
		{name: "Oracle, a sequence reference on a text column", dialect: platform.Oracle, columnType: "VARCHAR2(30)", expression: "app.seq.NEXTVAL", reported: `"APP"."SEQ"."NEXTVAL"`},
		{name: "SQL Server, a sequence clause on a text column", dialect: platform.SQLServer, columnType: "nvarchar(30)", expression: "NEXT VALUE FOR s", reported: "(NEXT VALUE FOR [s])"},
		{name: "MySQL, a quote after a backslash against a doubled quote", dialect: platform.MySQL, columnType: "varchar(30)", literal: `'a\'b\\c'`, reported: `'a''b\\c'`},
		{name: "MariaDB, a quote after a backslash against a doubled quote", dialect: platform.MariaDB, columnType: "varchar(30)", literal: `'a\'b'`, reported: `'a''b'`},
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

// Where a backslash escapes, a backslash the declaration holds is not the
// escape a catalog spelling starts: a bare a\b is three characters, and the
// reported 'a\b' is a and a backspace.
func TestColumns_ABackslashIsNotTheEscapeItStarts(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, reported         string
		want                      map[string]string
	}{
		{
			name: "MySQL", dialect: platform.MySQL, columnType: "varchar(30)",
			literal: `a\b`, reported: `'a\b'`,
			want: map[string]string{"default": `'a\b' -> a\b`},
		},
		{
			name: "ClickHouse", dialect: platform.ClickHouse, columnType: "String",
			literal: `a\b`, reported: `'a\b'`,
			want: map[string]string{"default": `'a\b' -> a\b`},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultSet: true}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, row.want)
		})
	}
}

// No default and the empty string are different defaults: one inserts NULL for
// an omitted value, the other an empty string. Each catalog reports the empty
// string in quotes, and no default as nothing.
func TestColumns_AnEmptyStringIsNotNoDefault(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal                   string
		set                       bool
		reported                  *string
		want                      map[string]string
	}{
		{
			name: "MySQL, an empty string against no default", dialect: platform.MySQL, columnType: "varchar(10)",
			literal: "''",
			want:    map[string]string{"default_expr": " -> ''"},
		},
		{
			name: "MySQL, no default against an empty string", dialect: platform.MySQL, columnType: "varchar(10)",
			reported: new("''"),
			want:     map[string]string{"default": "'' -> "},
		},
		{
			name: "MariaDB, no default against an empty string", dialect: platform.MariaDB, columnType: "varchar(10)",
			reported: new("''"),
			want:     map[string]string{"default": "'' -> "},
		},
		{
			name: "PostgreSQL, an empty string against no default", dialect: platform.Postgres, columnType: "varchar(10)",
			literal: "''",
			want:    map[string]string{"default_expr": " -> ''"},
		},
		{
			name: "PostgreSQL, no default against an empty string", dialect: platform.Postgres, columnType: "varchar(10)",
			reported: new("''::character varying"),
			want:     map[string]string{"default_expr": "''::character varying -> "},
		},
		{
			name: "an empty literal a model declares against no default", dialect: platform.Postgres, columnType: "text",
			set:  true,
			want: map[string]string{"default_expr": " -> ''"},
		},
		{
			// A varbinary stores no text, so only the empty string's own
			// check tells it from no default.
			name: "MySQL, no default against an empty string on a varbinary", dialect: platform.MySQL, columnType: "varbinary(8)",
			reported: new("''"),
			want:     map[string]string{"default": "'' -> "},
		},
		{
			name: "no default against no default", dialect: platform.MySQL, columnType: "varchar(10)",
			want: make(map[string]string),
		},
		{
			name: "an empty string against an empty string", dialect: platform.MariaDB, columnType: "varchar(10)",
			literal: "''", reported: new("''"),
			want: make(map[string]string),
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultSet: row.set}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: row.reported}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, row.want)
		})
	}
}

// A column that stores bytes holds one default however a catalog writes its
// bytes: a string, a hexadecimal string and MySQL's 0x form are one default,
// and a different string is another.
func TestColumns_ABinaryDefaultIsItsBytes(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		reported                  string
		want                      map[string]string
	}{
		{name: "MariaDB, a string expression against its hexadecimal string", dialect: platform.MariaDB, columnType: "varbinary(8)", expression: "('xy')", reported: "x'7879'", want: make(map[string]string)},
		{name: "MySQL, a string against its hexadecimal string", dialect: platform.MySQL, columnType: "varbinary(8)", literal: "'xy'", reported: "x'7879'", want: make(map[string]string)},
		{name: "MySQL, 0x against x", dialect: platform.MySQL, columnType: "varbinary(8)", expression: "0x7879", reported: "x'7879'", want: make(map[string]string)},
		{name: "MySQL, an introduced string expression against its hexadecimal string", dialect: platform.MySQL, columnType: "varbinary(8)", expression: "('xy')", reported: "(_latin1'xy')", want: make(map[string]string)},
		{name: "MySQL, an odd count of hexadecimal digits", dialect: platform.MySQL, columnType: "varbinary(8)", expression: "0xabc", reported: "x'0abc'", want: make(map[string]string)},
		{name: "MariaDB, a hexadecimal number on an int", dialect: platform.MariaDB, columnType: "int", expression: "0x1f", reported: "31", want: make(map[string]string)},
		{name: "MySQL, a bit number on a bit column", dialect: platform.MySQL, columnType: "bit(3)", expression: "0b110", reported: "b'110'", want: make(map[string]string)},
		{name: "MySQL, a bit string on an int", dialect: platform.MySQL, columnType: "int", expression: "b'1'", reported: "1", want: make(map[string]string)},
		{name: "MariaDB, a bit number on an int", dialect: platform.MariaDB, columnType: "bigint", expression: "0B101", reported: "5", want: make(map[string]string)},
		{name: "MySQL, a number against a bit string the catalog reports", dialect: platform.MySQL, columnType: "int", expression: "5", reported: "b'101'", want: make(map[string]string)},
		{
			name: "MySQL, a bit string on an int against another number", dialect: platform.MySQL, columnType: "int",
			expression: "b'101'", reported: "4",
			want: map[string]string{"default_expr": "4 -> b'101'"},
		},
		{
			name: "MySQL, another number on an int", dialect: platform.MySQL, columnType: "int",
			expression: "0x1f", reported: "30",
			want: map[string]string{"default_expr": "30 -> 0x1f"},
		},
		{
			name: "MariaDB, another string", dialect: platform.MariaDB, columnType: "varbinary(8)",
			expression: "('xz')", reported: "x'7879'",
			want: map[string]string{"default_expr": "x'7879' -> ('xz')"},
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

// A hexadecimal or bit literal on a MySQL-family text column is the string of
// its bytes: the server stores the string, and the reader reports it in
// quotes. Either side may be the literal.
func TestColumns_AByteLiteralOnATextColumnIsItsString(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		reported                  string
	}{
		{name: "MySQL, a hexadecimal number", dialect: platform.MySQL, columnType: "varchar(8)", expression: "0x41", reported: "'A'"},
		{name: "MariaDB, a hexadecimal string", dialect: platform.MariaDB, columnType: "varchar(8)", expression: "x'41'", reported: "'A'"},
		{name: "MySQL, a bit string short of a byte", dialect: platform.MySQL, columnType: "varchar(8)", expression: "b'1100010'", reported: "'b'"},
		{name: "MariaDB, a bit number", dialect: platform.MariaDB, columnType: "text", expression: "0b1100010", reported: "'b'"},
		{name: "MySQL, a backslash and a quote", dialect: platform.MySQL, columnType: "varchar(8)", expression: "0x5C27", reported: `'\\'''`},
		{name: "MariaDB, two bytes of one character", dialect: platform.MariaDB, columnType: "varchar(8)", expression: "X'C3A9'", reported: "'é'"},
		{name: "MySQL, a string against a hexadecimal number", dialect: platform.MySQL, columnType: "varchar(8)", literal: "'A'", reported: "0x41"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultExpr: row.expression}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, make(map[string]string))
		})
	}
}

// A literal a model declares keeps its characters, and bytes that are no text
// keep their spelling, so neither meets the string a server reports.
func TestColumns_AByteLiteralStaysApartFromAnotherString(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		reported                  string
		want                      map[string]string
	}{
		{
			name: "MySQL, a model literal spelled like a hexadecimal number", dialect: platform.MySQL, columnType: "varchar(8)",
			literal: "0x61", reported: "'a'",
			want: map[string]string{"default": "'a' -> 0x61"},
		},
		{
			name: "MariaDB, bytes that are no UTF-8 text", dialect: platform.MariaDB, columnType: "varchar(8)",
			expression: "x'ff'", reported: "'a'",
			want: map[string]string{"default": "'a' -> x'ff'"},
		},
		{
			name: "MySQL, another string", dialect: platform.MySQL, columnType: "varchar(8)",
			expression: "0x42", reported: "'A'",
			want: map[string]string{"default": "'A' -> 0x42"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultSet: row.literal != "", DefaultExpr: row.expression}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, row.want)
		})
	}
}

// A call to ucase or lcase is a call to upper or lower on the MySQL family,
// whichever side writes it, and the same word inside a string is the string.
func TestColumns_AFunctionAliasIsTheFunction(t *testing.T) {
	rows := []struct {
		name, dialect, columnType string
		literal, expression       string
		reported                  string
		want                      map[string]string
	}{
		{
			name: "MySQL, ucase against what MySQL reports", dialect: platform.MySQL, columnType: "varchar(30)",
			expression: "(ucase('abc'))", reported: "upper(_utf8mb4'abc')",
			want: make(map[string]string),
		},
		{
			name: "MariaDB, lcase in upper case", dialect: platform.MariaDB, columnType: "varchar(30)",
			expression: "(LCASE('ABC'))", reported: "lower('ABC')",
			want: make(map[string]string),
		},
		{
			name: "MariaDB, upper against an alias a reader keeps", dialect: platform.MariaDB, columnType: "varchar(30)",
			expression: "(upper('abc'))", reported: "ucase('abc')",
			want: make(map[string]string),
		},
		{
			name: "MariaDB, the word inside a string", dialect: platform.MariaDB, columnType: "varchar(30)",
			literal: "ucase(x)", reported: "'upper(x)'",
			want: map[string]string{"default": "'upper(x)' -> ucase(x)"},
		},
		{
			name: "PostgreSQL, which takes no alias", dialect: platform.Postgres, columnType: "text",
			expression: "(ucase('abc'))", reported: "upper('abc'::text)",
			want: map[string]string{"default_expr": "upper('abc'::text) -> (ucase('abc'))"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := schemamodel.Field{Name: "c", Type: row.columnType, Nullable: true, Default: row.literal, DefaultSet: row.literal != "", DefaultExpr: row.expression}
			dbCol := catalog.Column{Name: "c", DataType: row.columnType, ColumnType: row.columnType, IsNullable: "YES", ColumnDefault: new(row.reported)}

			result := compare.ColumnsWithDialect(genCol, dbCol, row.dialect)

			c.Assert(result.Changes, qt.DeepEquals, row.want)
		})
	}
}

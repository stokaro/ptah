package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestTableElementTypo_FailurePath is stokaro/ptah#2753.
//
// A table-body element whose first word is one keystroke off a recognised
// keyword fell through to the column parser, which read the word as a column
// name and whatever followed as a type. `ptah schema render` exited 0 and
// emitted a CREATE TABLE for a table nobody described: a column `FULLTEXTT` of
// type `ft_b(bio)`, which no engine has. The only tell was a `0 indexes` on the
// summary line.
func TestTableElementTypo_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "FULLTEXT misspelled, the reported input",
			dialect: platform.MySQL,
			sql: `CREATE TABLE t (
				id BIGINT NOT NULL PRIMARY KEY,
				bio TEXT NOT NULL,
				FULLTEXTT ft_b (bio)
			);`,
			wantErr: `table "t" declares "FULLTEXTT" with type "ft_b\(bio\)".*"bio" is a column of "t".*misspelled.*`,
		},
		{
			name:    "KEY misspelled, naming a column the body declares later",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE t (KEYY k (b), b INT);`,
			wantErr: `table "t" declares "KEYY" with type "k\(b\)".*"b" is a column of "t".*`,
		},
		{
			name:    "INDEX misspelled over two columns",
			dialect: platform.MariaDB,
			sql:     `CREATE TABLE t (a INT, b INT, INDEXX i (a, b));`,
			wantErr: `table "t" declares "INDEXX" with type "i\(a, b\)".*"a", "b" are each a column of "t".*`,
		},
		{
			name:    "UNIQUE misspelled, on a dialect with no MySQL index grammar at all",
			dialect: platform.Postgres,
			sql:     `CREATE TABLE t (a INT, UNIQE u (a));`,
			wantErr: `table "t" declares "UNIQE" with type "u\(a\)".*"a" is a column of "t".*`,
		},
		{
			// A column nobody can spell without quotes. The rule reads the
			// argument as the text it is rather than as an identifier shape,
			// which is what lets this one be caught at all.
			name:    "misspelled over a quoted column name",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE t ("my col" INT, KEYY k ("my col"));`,
			wantErr: `table "t" declares "KEYY" with type .*my col.*is a column of "t".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestTableElementTypo_HappyPath is the half that keeps the refusal narrow.
//
// This parser accepts a type it has never heard of on purpose, because
// extension and user-defined types are ordinary. Every row here is a type whose
// parentheses hold something, in a table chosen to collide with it as far as
// the grammar allows -- the PostGIS row declares a column called `point` beside
// a `geometry(Point, 4326)`, which is the closest a real schema gets to the
// shape the refusal reports.
func TestTableElementTypo_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		column  string
		want    string
	}{
		{
			name:    "a mixed argument list, beside a column of that name",
			dialect: platform.Postgres,
			sql:     `CREATE TABLE g (point TEXT, shape geometry(Point, 4326));`,
			column:  "shape",
			want:    "geometry(Point, 4326)",
		},
		{
			name:    "a width",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE g (name VARCHAR(255));`,
			column:  "name",
			want:    "VARCHAR(255)",
		},
		{
			name:    "a precision and a scale",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE g (amount DECIMAL(10,2));`,
			column:  "amount",
			want:    "DECIMAL(10,2)",
		},
		{
			name:    "string literals",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE g (a TEXT, b TEXT, kind ENUM('a','b'));`,
			column:  "kind",
			want:    "ENUM('a','b')",
		},
		{
			name:    "a bare type name that is not a column here",
			dialect: platform.Postgres,
			sql:     `CREATE TABLE g (label Nullable(String));`,
			column:  "label",
			want:    "Nullable(String)",
		},
		{
			name:    "two words inside the parentheses",
			dialect: platform.Oracle,
			sql:     `CREATE TABLE g (CHAR INT, name VARCHAR2(50 CHAR));`,
			column:  "name",
			want:    "VARCHAR2(50 CHAR)",
		},
		{
			// A quoted column name is recorded WITH its quotes, so `"50 CHAR"`
			// and the argument `50 CHAR` are different strings and the type is
			// left alone. This is the closest an Oracle schema gets to the
			// shape the refusal reports.
			name:    "a two-word argument beside a quoted column spelled the same",
			dialect: platform.Oracle,
			sql:     `CREATE TABLE g ("50 CHAR" INT, name VARCHAR2(50 CHAR));`,
			column:  "name",
			want:    "VARCHAR2(50 CHAR)",
		},
		{
			// `id` is a column and `other` is not, and the rule asks for every
			// argument rather than any. Reading it as "any" would refuse a
			// user-defined type for sharing one word with a column, which is
			// what makes this row the one that measures that quantifier.
			name:    "arguments of which only some name columns",
			dialect: platform.Postgres,
			sql:     `CREATE TABLE g (id INT, pair mytype(other, id));`,
			column:  "pair",
			want:    "mytype(other, id)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declaredColumnType(c, table, test.column), qt.Equals, test.want)
		})
	}
}

// TestTableElementTypo_ClickHouseWritesThisShapeOrdinarily is the dialect
// carve-out, asserted as the difference it is.
//
// `Nullable(T)`, `Array(T)` and `LowCardinality(T)` all take a bare type name,
// so on ClickHouse the conjunction the refusal rests on stops being unusual: a
// table with a column named `String` writes it every day. The same input is
// refused on MySQL, which is what says the carve-out is a decision about
// ClickHouse rather than a rule that never fires.
func TestTableElementTypo_ClickHouseWritesThisShapeOrdinarily(t *testing.T) {
	const document = `CREATE TABLE t (String Int32, label Nullable(String));`

	t.Run("clickhouse accepts it", func(t *testing.T) {
		c := qt.New(t)
		statements, err := parser.NewParser(document,
			parser.WithDialect(platform.ClickHouse)).Parse()
		c.Assert(err, qt.IsNil)
		table, ok := statements.Statements[0].(*ast.CreateTableNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(declaredColumnType(c, table, "label"), qt.Equals, "Nullable(String)")
	})

	t.Run("mysql refuses the same document", func(t *testing.T) {
		c := qt.New(t)
		statements, err := parser.NewParser(document,
			parser.WithDialect(platform.MySQL)).Parse()
		c.Assert(err, qt.ErrorMatches, `table "t" declares "label" with type "Nullable\(String\)".*`)
		c.Assert(statements, qt.IsNil)
	})
}

// TestTableElementTypo_ARealIndexStillParses is the control the refusal would
// otherwise be free to break.
//
// A repair that refused the shape outright would satisfy the failure path above
// and destroy the feature: a table-level index declared with its keyword is the
// thing the misspelling was reaching for.
func TestTableElementTypo_ARealIndexStillParses(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{
			name:    "KEY naming a column the body declares later",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE t (KEY k (b), b INT);`,
			want:    "k",
		},
		{
			name:    "FULLTEXT, the keyword the reported input missed",
			dialect: platform.MySQL,
			sql:     `CREATE TABLE t (bio TEXT, FULLTEXT ft_b (bio));`,
			want:    "ft_b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Name, qt.Equals, test.want)
		})
	}
}

// declaredColumnType is the type the parser recorded for one named column.
//
// Named rather than positional, because these tables put the column under test
// beside the one it is meant to collide with and the order carries meaning.
func declaredColumnType(c *qt.C, table *ast.CreateTableNode, name string) string {
	c.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			return column.Type
		}
	}
	c.Fatalf("table %q declares no column %q", table.Name, name)
	return ""
}

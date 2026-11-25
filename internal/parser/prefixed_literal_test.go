package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

func parseDialect(c *qt.C, dialect, sql string) (*ast.StatementList, error) {
	c.Helper()
	return parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
}

// A bit string, a hexadecimal string in either spelling, a string with a
// character set or a national prefix, adjacent strings, and MySQL's
// double-quoted string are each one literal default, held in the form the
// catalog reports: `db read` writes the first two for bit and binary columns,
// and the MySQL family stores the others as the string they spell.
func TestParse_PrefixedLiteralDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
		want                  ast.DefaultValue
	}{
		{name: "a bit string", dialect: "mysql", column: "b bit(1) DEFAULT b'1'", want: ast.DefaultValue{Expression: "b'1'"}},
		{name: "a bit string in upper case", dialect: "mysql", column: "b bit(3) DEFAULT B'101'", want: ast.DefaultValue{Expression: "b'101'"}},
		{name: "a hexadecimal string", dialect: "mariadb", column: "x varbinary(8) DEFAULT X'6A6B'", want: ast.DefaultValue{Expression: "x'6a6b'"}},
		{name: "MySQL's hexadecimal number", dialect: "mysql", column: "x varbinary(8) DEFAULT 0x61626364", want: ast.DefaultValue{Expression: "0x61626364"}},
		{name: "MySQL's bit number", dialect: "mysql", column: "b bit(3) DEFAULT 0b101", want: ast.DefaultValue{Expression: "0b101"}},
		{name: "a character set introducer", dialect: "mysql", column: "u varchar(30) DEFAULT _utf8mb4'u'", want: ast.DefaultValue{Value: "'u'", ValueSet: true}},
		{name: "a national string", dialect: "mariadb", column: "n varchar(30) DEFAULT N'n'", want: ast.DefaultValue{Value: "'n'", ValueSet: true}},
		{name: "adjacent strings", dialect: "mysql", column: "j varchar(30) DEFAULT 'a' 'b'", want: ast.DefaultValue{Value: "'ab'", ValueSet: true}},
		{name: "a double-quoted string", dialect: "mysql", column: `d varchar(30) DEFAULT "d'q"`, want: ast.DefaultValue{Value: "'d''q'", ValueSet: true}},
		{name: "a doubled double quote", dialect: "mysql", column: `d varchar(30) DEFAULT "a""b"`, want: ast.DefaultValue{Value: `'a"b'`, ValueSet: true}},
		{name: "an escape kept", dialect: "mysql", column: `d varchar(30) DEFAULT "a\"b" 'c\\d'`, want: ast.DefaultValue{Value: `'a\"bc\\d'`, ValueSet: true}},
		{name: "a clause after the string", dialect: "mysql", column: "k varchar(30) DEFAULT 'a' NOT NULL", want: ast.DefaultValue{Value: "'a'", ValueSet: true}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parseDialect(c, row.dialect, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+");")

			c.Assert(err, qt.IsNil)
			c.Assert(*defaultOf(c, statements), qt.DeepEquals, row.want)
		})
	}
}

// A prefix and a string with a space between them are two tokens, and a
// character set introducer or adjacent strings outside the MySQL family are
// no literal either: the server refuses each. A bit or hexadecimal string
// outside the MySQL family is refused as well, since the other engines read
// those spellings with other types.
func TestParse_PrefixedLiteralDefault_FailurePath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
	}{
		{name: "a space after the introducer", dialect: "mysql", column: "u varchar(30) DEFAULT _utf8mb4 'u'"},
		{name: "a space after the bit prefix", dialect: "mysql", column: "b bit(1) DEFAULT b '1'"},
		{name: "an introducer on PostgreSQL", dialect: "postgres", column: "u text DEFAULT _utf8mb4'u'"},
		{name: "adjacent strings on one line on PostgreSQL", dialect: "postgres", column: "j text DEFAULT 'a' 'b'"},
		{name: "a bit string on PostgreSQL", dialect: "postgres", column: "b bit(3) DEFAULT B'101'"},
		{name: "a hexadecimal string on SQLite", dialect: "sqlite", column: "x blob DEFAULT X'6A'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parseDialect(c, row.dialect, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+");")

			c.Assert(err, qt.ErrorMatches, `.*expected ',' or '\)' after table element`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

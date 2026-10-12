package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// keyVisibility is what a guard test compares of an index.
type keyVisibility struct {
	Invisible bool
	Comment   string
}

// firstIndex parses one statement and returns the first index it declares, in
// a table body or as a CREATE INDEX.
func firstIndex(c *qt.C, dialect, sql string, opts ...parser.Option) *ast.IndexNode {
	c.Helper()
	statements, err := parser.NewParser(sql, append([]parser.Option{parser.WithDialect(dialect)}, opts...)...).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	if ok {
		return index
	}
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Indexes, qt.HasLen, 1)
	return table.Indexes[0]
}

// MySQL 8.4's SHOW CREATE TABLE writes an invisible index as `KEY k (a)
// /*!80000 INVISIBLE */`, so a dump read with the MySQL dialect reads the
// option inside the guard, wherever the guard stands among the key's options.
func TestParse_AGuardedKeyOptionIsRead(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want keyVisibility
	}{
		{name: "the guard SHOW CREATE TABLE writes",
			sql:  "CREATE TABLE t (a int, KEY `k` (`a`) /*!80000 INVISIBLE */);",
			want: keyVisibility{Invisible: true}},
		{name: "after another option",
			sql:  "CREATE TABLE t (a int, KEY k (a) COMMENT 'x' /*!80000 INVISIBLE */);",
			want: keyVisibility{Invisible: true, Comment: "x"}},
		{name: "before another option",
			sql:  "CREATE TABLE t (a int, KEY k (a) /*!80000 INVISIBLE */ COMMENT 'x');",
			want: keyVisibility{Invisible: true, Comment: "x"}},
		{name: "in CREATE INDEX",
			sql:  "CREATE INDEX k ON t (a) /*!80000 INVISIBLE */;",
			want: keyVisibility{Invisible: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := firstIndex(c, platform.MySQL, test.sql)

			c.Assert(keyVisibility{Invisible: index.Invisible, Comment: index.Comment}, qt.Equals, test.want)
		})
	}
}

// Every guard among a key's options is read, not only the last: MySQL 8.4
// writes a FULLTEXT index's parser after its visibility, each in a guard of its
// own.
func TestParse_EveryGuardAmongAKeysOptionsIsRead(t *testing.T) {
	c := qt.New(t)

	index := firstIndex(c, platform.MySQL,
		"CREATE TABLE t (bio text, FULLTEXT KEY `ft` (`bio`) COMMENT 'x' /*!80000 INVISIBLE */ /*!50100 WITH PARSER `ngram` */);",
		withMySQLOwners())

	c.Assert(keyVisibility{Invisible: index.Invisible, Comment: index.Comment}, qt.Equals, keyVisibility{Invisible: true, Comment: "x"})
	c.Assert(indexParser(c, index), qt.Equals, "ngram")
}

// A guard whose body opens with no key option is stepped over, as a guard is
// anywhere else, and a document read with no dialect reads every guard as a
// comment.
func TestParse_AGuardHoldingNoKeyOptionIsSteppedOver(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "another clause", dialect: platform.MySQL, sql: "CREATE TABLE t (a int, KEY k (a) /*!50100 SOMETHING else */);"},
		{name: "no dialect", dialect: "", sql: "CREATE TABLE t (a int, KEY k (a) /*!80000 INVISIBLE */);"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := firstIndex(c, test.dialect, test.sql)

			c.Assert(index.Invisible, qt.IsFalse)
		})
	}
}

// A guard that opens with a key option holds nothing else, and an option in a
// guard is refused where the bare option is.
func TestParse_AGuardedKeyOption_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "more than options", sql: "CREATE TABLE t (a int, KEY k (a) /*!80000 INVISIBLE extra */);",
			wantErr: `extra at position 52: a version guard among a key's options holds only key options`},
		{name: "an invisible primary key", sql: "CREATE TABLE t (a int, PRIMARY KEY (a) /*!80000 INVISIBLE */);",
			wantErr: `INVISIBLE at position 48: MySQL 8.4.11 answers ERROR 3522, a primary key index cannot be invisible`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.MySQL)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

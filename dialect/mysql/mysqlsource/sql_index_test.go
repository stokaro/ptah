package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// keyOptions is what a test compares of a key the owner read options for.
type keyOptions struct {
	Name      string
	Unique    bool
	BlockSize uint64
	Parser    string
}

// readKeyOptions answers the owner's options of facets, a key's.
func readKeyOptions(c *qt.C, name string, unique bool, facets schemaext.Facets) keyOptions {
	c.Helper()
	size, _, err := mysqlschema.IndexBlockSize(facets)
	c.Assert(err, qt.IsNil)
	options, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](facets, mysqlschema.IndexKind)
	c.Assert(err, qt.IsNil)
	read := keyOptions{Name: name, Unique: unique, BlockSize: size}
	if found {
		read.Parser = options.Parser
	}
	return read
}

// indexOptions answers the options of each index a statement declares.
func indexOptions(c *qt.C, node ast.Node) []keyOptions {
	c.Helper()
	var indexes []*ast.IndexNode
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		indexes = typed.Indexes
	case *ast.IndexNode:
		indexes = []*ast.IndexNode{typed}
	}
	options := make([]keyOptions, 0, len(indexes))
	for _, index := range indexes {
		options = append(options, readKeyOptions(c, index.Name, index.Unique, index.Facets))
	}
	return options
}

// A key's KEY_BLOCK_SIZE and a FULLTEXT index's WITH PARSER are read wherever
// they stand among the key's options, bare or in the version guard a dump
// writes; an option written twice keeps the last, as MySQL 8.4 and MariaDB
// 11.8 do; and each key keeps its own.
func TestSQL_ReadsIndexOptions(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []keyOptions
	}{
		{name: "a key's block size", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=8);",
			want: []keyOptions{{Name: "k", BlockSize: 8}}},
		{name: "both, around a comment", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (bio text, FULLTEXT KEY ft (bio) KEY_BLOCK_SIZE 4 WITH PARSER ngram COMMENT 'x');",
			want: []keyOptions{{Name: "ft", BlockSize: 4, Parser: "ngram"}}},
		{name: "the guarded parser MySQL 8.4 writes last", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (bio text, FULLTEXT KEY `ft` (`bio`) COMMENT 'x' /*!80000 INVISIBLE */ /*!50100 WITH PARSER `ngram` */);",
			want: []keyOptions{{Name: "`ft`", Parser: "ngram"}}},
		{name: "a guard ends the option before it", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (bio text, FULLTEXT KEY ft (bio) KEY_BLOCK_SIZE=8 /*!50100 WITH PARSER ngram */ COMMENT 'x');",
			want: []keyOptions{{Name: "ft", BlockSize: 8, Parser: "ngram"}}},
		{name: "the last of two", dialect: platform.MariaDB,
			sql:  "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=4 COMMENT 'x' KEY_BLOCK_SIZE=8);",
			want: []keyOptions{{Name: "k", BlockSize: 8}}},
		{name: "each key its own", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a int, b int, c text, KEY k (a) KEY_BLOCK_SIZE=4, KEY l (b), FULLTEXT KEY ft (c) WITH PARSER ngram);",
			want: []keyOptions{{Name: "k", BlockSize: 4}, {Name: "l"}, {Name: "ft", Parser: "ngram"}}},
		{name: "a unique key with an option is a unique index", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a int, UNIQUE KEY u (a) KEY_BLOCK_SIZE=8);",
			want: []keyOptions{{Name: "u", Unique: true, BlockSize: 8}}},
		{name: "a zero size is none, and still a unique index", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a int, UNIQUE KEY u (a) KEY_BLOCK_SIZE=0);",
			want: []keyOptions{{Name: "u", Unique: true}}},
		{name: "CREATE FULLTEXT INDEX", dialect: platform.MySQL,
			sql:  "CREATE FULLTEXT INDEX ft ON t (bio) KEY_BLOCK_SIZE=8 WITH PARSER ngram COMMENT 'y';",
			want: []keyOptions{{Name: "ft", BlockSize: 8, Parser: "ngram"}}},
		{name: "a document that names no target", dialect: "",
			sql:  "CREATE TABLE t (a int, KEY k (a) key_block_size 8);",
			want: []keyOptions{{Name: "k", BlockSize: 8}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(indexOptions(c, statements.Statements[0]), qt.DeepEquals, test.want)
		})
	}
}

// A PRIMARY KEY's options are the owner's settings of the key, kept on its
// constraint rather than on an index.
func TestSQL_ReadsAPrimaryKeysBlockSize(t *testing.T) {
	c := qt.New(t)

	statements, err := parseMySQL(platform.MySQL, "CREATE TABLE t (a int, PRIMARY KEY (a) KEY_BLOCK_SIZE=8 COMMENT 'pk');")

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(table.Indexes, qt.HasLen, 0)
	c.Assert(table.Constraints, qt.HasLen, 1)
	c.Assert(readKeyOptions(c, "", false, table.Constraints[0].Facets), qt.Equals, keyOptions{BlockSize: 8})
	c.Assert(table.Constraints[0].Comment, qt.Equals, "pk")
}

// A malformed option is refused at its position in the document, inside a
// version guard too, and WITH PARSER on a key that is not FULLTEXT is refused
// rather than ignored.
func TestSQL_RefusesAMalformedIndexOption(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{name: "no integer", dialect: platform.MySQL, sql: "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE='8');",
			wantErr: `KEY_BLOCK_SIZE at position 33: expected a non-negative integer`},
		{name: "past MariaDB's limit", dialect: platform.MariaDB, sql: "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=65536);",
			wantErr: `KEY_BLOCK_SIZE 65536 exceeds the mariadb limit 65535; the server would truncate it`},
		{name: "words after the size", dialect: platform.MySQL, sql: "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=8 PAGES);",
			wantErr: `unexpected "PAGES" after the block size at position 50`},
		{name: "no parser name", dialect: platform.MySQL, sql: "CREATE TABLE t (bio text, FULLTEXT KEY ft (bio) WITH PARSER 'x');",
			wantErr: `expected a parser name after WITH PARSER: expected identifier, got String at position 60`},
		{name: "no parser name in a guard", dialect: platform.MySQL, sql: "CREATE TABLE t (bio text, FULLTEXT KEY ft (bio) /*!50100 WITH PARSER */);",
			wantErr: `expected a parser name after WITH PARSER: expected identifier, got EOF at position 69`},
		{name: "words after the parser name", dialect: platform.MySQL, sql: "CREATE TABLE t (bio text, FULLTEXT KEY ft (bio) WITH PARSER ngram extra);",
			wantErr: `unexpected "extra" after the parser name at position 66`},
		{name: "a parser on a spatial index", dialect: platform.MySQL, sql: "CREATE TABLE t (g geometry NOT NULL, SPATIAL KEY s (g) WITH PARSER ngram);",
			wantErr: `WITH PARSER belongs to a FULLTEXT index, and this one is SPATIAL`},
		{name: "a parser on a CREATE INDEX", dialect: platform.MySQL, sql: "CREATE INDEX k ON t (a) WITH PARSER ngram;",
			wantErr: `WITH PARSER belongs to a FULLTEXT index, and this one is an ordinary index`},
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

// An option ends where the key's next option begins, whichever option the
// grammar reads there or the owner's own next one, so the next option is read
// and not refused as part of the one before it.
func TestSQL_AnIndexOptionEndsAtTheNextOption(t *testing.T) {
	tests := []struct {
		option string
		want   keyOptions
	}{
		{option: "USING BTREE", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "COMMENT 'x'", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "VISIBLE", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "INVISIBLE", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "IGNORED", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "NOT IGNORED", want: keyOptions{Name: "k", BlockSize: 8}},
		{option: "KEY_BLOCK_SIZE=2", want: keyOptions{Name: "k", BlockSize: 2}},
	}
	for _, test := range tests {
		t.Run(test.option, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL("", "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=8 "+test.option+");")

			c.Assert(err, qt.IsNil)
			c.Assert(indexOptions(c, statements.Statements[0]), qt.DeepEquals, []keyOptions{test.want})
		})
	}
}

// An engine attribute ends an option too, so the grammar refuses it by name
// rather than the owner reading it as part of the block size.
func TestSQL_AnIndexOptionEndsAtARefusedOption(t *testing.T) {
	c := qt.New(t)

	statements, err := parseMySQL(platform.MySQL, "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE=8 ENGINE_ATTRIBUTE '{}');")

	c.Assert(err, qt.ErrorMatches, `ENGINE_ATTRIBUTE at position 50: an engine attribute of an index is not modeled; declare the key without it`)
	c.Assert(statements, qt.IsNil)
}

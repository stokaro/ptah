package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/parser"
)

// mysqlIndexKeywordTable wraps one table-body declaration in a table that has a
// column for each index kind to sit on.
func mysqlIndexKeywordTable(declaration string) string {
	return fmt.Sprintf(
		"CREATE TABLE t (id BIGINT NOT NULL, bio TEXT NOT NULL, geom GEOMETRY NOT NULL, PRIMARY KEY (id), %s);",
		declaration,
	)
}

// TestParse_MySQLTableBodyIndexKeywords covers stokaro/ptah#2747.
//
// MySQL spells a table-body index with either KEY or INDEX after SPATIAL and
// FULLTEXT, and mysqldump writes the KEY form for both. The reader accepted
// only `SPATIAL INDEX`: `SPATIAL KEY` was refused for the keyword, and neither
// FULLTEXT spelling reached an index branch at all, so the declaration fell
// through to column parsing and the index NAME came back as an unsupported
// column attribute.
//
// Each row asserts the index is an index -- one entry in Indexes and nothing in
// Constraints -- because the failure this file guards against is the one
// #2711 and #2713 both had, where a table-body index arrived as a uniqueness
// guarantee the author never wrote.
func TestParse_MySQLTableBodyIndexKeywords(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantName    string
		wantType    string
		wantParser  string
	}{
		{name: "SPATIAL INDEX", declaration: "SPATIAL INDEX sp_g (geom)", wantName: "sp_g", wantType: "SPATIAL"},
		{name: "SPATIAL KEY", declaration: "SPATIAL KEY sp_g (geom)", wantName: "sp_g", wantType: "SPATIAL"},
		{name: "FULLTEXT INDEX", declaration: "FULLTEXT INDEX ft_b (bio)", wantName: "ft_b", wantType: "FULLTEXT"},
		{name: "FULLTEXT KEY", declaration: "FULLTEXT KEY ft_b (bio)", wantName: "ft_b", wantType: "FULLTEXT"},
		{
			name:        "FULLTEXT KEY with a parser",
			declaration: "FULLTEXT KEY ft_b (bio) WITH PARSER ngram",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantParser:  "ngram",
		},
		{
			name:        "FULLTEXT INDEX with a parser",
			declaration: "FULLTEXT INDEX ft_b (bio) WITH PARSER ngram",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantParser:  "ngram",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := parsedTable(c, mysqlIndexKeywordTable(test.declaration))

			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Name, qt.Equals, test.wantName)
			c.Assert(table.Indexes[0].Type, qt.Equals, test.wantType)
			c.Assert(table.Indexes[0].Parser, qt.Equals, test.wantParser)
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
			// The table's own PRIMARY KEY is a constraint and belongs here.
			// What must not be is a uniqueness guarantee standing in for the
			// index, which is the shape #2711 and #2713 each reported.
			c.Assert(constraintTypes(table), qt.Not(qt.Contains), ast.UniqueConstraint)
		})
	}
}

// TestParse_MySQLTableBodyIndexKeywordsAreNotAWildcard is the control the
// change above cannot be made without.
//
// Accepting KEY beside INDEX widens a keyword the reader used to require, and
// the cheapest way to make every row of the happy path pass is to stop
// requiring a keyword at all. Then `SPATIAL sp_g (geom)` parses, a misspelled
// FULLTEXT parses, and the reader silently invents an index for a declaration
// MySQL refuses -- which is the looser-than-the-server direction the
// compatibility policy rules out.
//
// Each row is refused by MySQL, so each must be refused here.
func TestParse_MySQLTableBodyIndexKeywordsAreNotAWildcard(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
	}{
		{name: "SPATIAL followed by neither keyword", declaration: "SPATIAL sp_g (geom)"},
		{name: "SPATIAL followed by a wrong keyword", declaration: "SPATIAL CLUSTER sp_g (geom)"},
		{name: "FULLTEXT followed by neither keyword", declaration: "FULLTEXT ft_b (bio)"},
		{name: "FULLTEXT followed by a wrong keyword", declaration: "FULLTEXT CLUSTER ft_b (bio)"},
		{name: "the keyword itself misspelled", declaration: "FULLTEX KEY ft_b (bio)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				mysqlIndexKeywordTable(test.declaration),
				parser.WithDialect("mysql"),
			).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
		})
	}
}

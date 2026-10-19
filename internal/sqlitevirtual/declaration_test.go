package sqlitevirtual_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlitevirtual"
)

func TestDeclarationRendered_HappyPath(t *testing.T) {
	docs := sqlitevirtual.Table{Schema: "main", Name: "docs", Module: "fts5", Arguments: "body"}
	tests := []struct {
		name     string
		rendered []string
		table    sqlitevirtual.Table
	}{
		{
			name:     "the statement on a line of its own",
			rendered: []string{"CREATE TABLE \"t\" (\"a\" int);\nCREATE VIRTUAL TABLE \"main\".\"docs\" USING fts5(body);\n"},
			table:    docs,
		},
		{
			name:     "in the second of two documents, with a comment after it",
			rendered: []string{"", "CREATE VIRTUAL TABLE \"main\".\"docs\" USING fts5(body); -- kept\n"},
			table:    docs,
		},
		{
			name:     "an empty argument list for a module that takes none",
			rendered: []string{"CREATE VIRTUAL TABLE \"main\".\"t\" USING rtree();\n"},
			table:    sqlitevirtual.Table{Schema: "main", Name: "t", Module: "rtree"},
		},
		{
			name:     "a module whose name needs quoting",
			rendered: []string{"CREATE VIRTUAL TABLE \"docs\" USING \"fts-5\"(body);\n"},
			table:    sqlitevirtual.Table{Name: "docs", Module: "fts-5", Arguments: "body"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlitevirtual.DeclarationRendered(test.rendered, test.table), qt.IsTrue)
		})
	}
}

func TestDeclarationRendered_FailurePath(t *testing.T) {
	docs := sqlitevirtual.Table{Schema: "main", Name: "docs", Module: "fts5", Arguments: "body"}
	tests := []struct {
		name     string
		rendered []string
	}{
		{name: "an HCL block naming the table", rendered: []string{"table \"docs\" {\n  schema = schema.main\n}\n"}},
		{name: "the statement inside a comment", rendered: []string{"-- CREATE VIRTUAL TABLE \"main\".\"docs\" USING fts5(body);\n"}},
		{name: "other arguments", rendered: []string{"CREATE VIRTUAL TABLE \"main\".\"docs\" USING fts5(title);\n"}},
		{name: "text after the terminator", rendered: []string{"CREATE VIRTUAL TABLE \"main\".\"docs\" USING fts5(body); SELECT 1;\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlitevirtual.DeclarationRendered(test.rendered, docs), qt.IsFalse)
		})
	}
}

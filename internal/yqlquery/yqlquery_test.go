package yqlquery_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/yqlquery"
)

// shape is what a test reads off a query: its kind and the text YDB is sent.
type shape struct {
	Kind yqlquery.Kind
	Text string
}

func shapes(queries []yqlquery.Query) []shape {
	out := make([]shape, 0, len(queries))
	for _, query := range queries {
		out = append(out, shape{Kind: query.Kind, Text: query.Text})
	}
	return out
}

func TestSplit_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []shape
	}{
		{
			name: "every scheme statement is a query of its own",
			text: "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id));\nALTER TABLE a ADD COLUMN v Utf8;\nDROP TABLE b;",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
				{Kind: yqlquery.Scheme, Text: "ALTER TABLE a ADD COLUMN v Utf8"},
				{Kind: yqlquery.Scheme, Text: "DROP TABLE b"},
			},
		},
		{
			name: "consecutive data statements are one query",
			text: "UPSERT INTO a (id) VALUES (1l);\nUPDATE a SET v = 'x'u WHERE id = 1l;\nDELETE FROM b WHERE id = 2l;",
			want: []shape{
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id) VALUES (1l);\nUPDATE a SET v = 'x'u WHERE id = 1l;\nDELETE FROM b WHERE id = 2l"},
			},
		},
		{
			name: "a scheme statement ends a run of data statements",
			text: "UPSERT INTO a (id) VALUES (1l);\nTRUNCATE TABLE b;\nUPSERT INTO a (id) VALUES (2l);",
			want: []shape{
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id) VALUES (1l)"},
				{Kind: yqlquery.Scheme, Text: "TRUNCATE TABLE b"},
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id) VALUES (2l)"},
			},
		},
		{
			name: "a named expression reaches every query after it",
			text: "$v = 41l;\nUPSERT INTO a (id) VALUES ($v);\nCREATE TABLE c (id Int64 NOT NULL, PRIMARY KEY (id));\nUPSERT INTO c (id) VALUES ($v);",
			want: []shape{
				{Kind: yqlquery.Data, Text: "$v = 41l;\nUPSERT INTO a (id) VALUES ($v)"},
				{Kind: yqlquery.Scheme, Text: "$v = 41l;\nCREATE TABLE c (id Int64 NOT NULL, PRIMARY KEY (id))"},
				{Kind: yqlquery.Data, Text: "$v = 41l;\nUPSERT INTO c (id) VALUES ($v)"},
			},
		},
		{
			name: "a definition inside a run stays in place and reaches the queries after it",
			text: "UPSERT INTO a (id) VALUES (1l);\n$v = 2l;\nUPSERT INTO a (id) VALUES ($v);\nDROP TABLE b;",
			want: []shape{
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id) VALUES (1l);\n$v = 2l;\nUPSERT INTO a (id) VALUES ($v)"},
				{Kind: yqlquery.Scheme, Text: "$v = 2l;\nDROP TABLE b"},
			},
		},
		{
			name: "a pragma heads every query after it",
			text: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id));\nCREATE TABLE b (id Int64 NOT NULL, PRIMARY KEY (id));",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
				{Kind: yqlquery.Scheme, Text: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE b (id Int64 NOT NULL, PRIMARY KEY (id))"},
			},
		},
		{
			name: "a pragma reaches only the queries after it",
			text: "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id));\nPRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE b (id Int64 NOT NULL, PRIMARY KEY (id));",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
				{Kind: yqlquery.Scheme, Text: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE b (id Int64 NOT NULL, PRIMARY KEY (id))"},
			},
		},
		{
			name: "an action and the statements that run it are one query",
			text: "DEFINE ACTION $ins($x) AS\n  UPSERT INTO a (id) VALUES ($x);\nEND DEFINE;\nDO $ins(7l);\nDO $ins(8l);",
			want: []shape{
				{Kind: yqlquery.Data, Text: "DEFINE ACTION $ins($x) AS\n  UPSERT INTO a (id) VALUES ($x);\nEND DEFINE;\nDO $ins(7l);\nDO $ins(8l)"},
			},
		},
		{
			name: "the translation setting heads every query",
			text: "--!syntax_v1\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id));\nUPSERT INTO a (id) VALUES (1l);",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "--!syntax_v1\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
				{Kind: yqlquery.Data, Text: "--!syntax_v1\nUPSERT INTO a (id) VALUES (1l)"},
			},
		},
		{
			name: "comments are removed and a semicolon in a literal stays",
			text: "-- seed\nUPSERT INTO a (id, v) VALUES (1l, 'a;b'u); -- trailing\n/* block */ DROP TABLE b;",
			want: []shape{
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id, v) VALUES (1l, 'a;b'u)"},
				{Kind: yqlquery.Scheme, Text: "DROP TABLE b"},
			},
		},
		{
			name: "UPSERT OBJECT writes a scheme object",
			text: "UPSERT OBJECT s (TYPE SECRET) WITH value = \"v\";",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "UPSERT OBJECT s (TYPE SECRET) WITH value = \"v\""},
			},
		},
		{
			name: "definitions after the last statement join the last query",
			text: "DROP TABLE b;\nPRAGMA TablePathPrefix = \"/local/app\";",
			want: []shape{
				{Kind: yqlquery.Scheme, Text: "DROP TABLE b;\nPRAGMA TablePathPrefix = \"/local/app\""},
			},
		},
		{
			name: "a text of nothing but definitions is one data query",
			text: "--!syntax_v1\n$x = 1;\nDECLARE $p AS Int64;",
			want: []shape{
				{Kind: yqlquery.Data, Text: "--!syntax_v1\n$x = 1;\nDECLARE $p AS Int64"},
			},
		},
		{
			name: "a delimiter inside a literal is text",
			text: "UPSERT INTO a (id, v) VALUES (1l, @@\nDELIMITER //\n-- atlas:delimiter //\n@@);",
			want: []shape{
				{Kind: yqlquery.Data, Text: "UPSERT INTO a (id, v) VALUES (1l, @@\nDELIMITER //\n-- atlas:delimiter //\n@@)"},
			},
		},
		{
			name: "no statement is no query",
			text: "-- nothing here\n;\n",
			want: []shape{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queries, err := yqlquery.Split(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(shapes(queries), qt.DeepEquals, test.want)
		})
	}
}

// Each source statement belongs to exactly one query, so the queries' sources
// cover the text in order, and a definition made between two queries belongs
// to the one after it.
func TestSplit_SourcesCoverEveryStatementOnce(t *testing.T) {
	c := qt.New(t)
	text := "DROP TABLE b;\n$v = 2l;\nPRAGMA TablePathPrefix = \"/local/app\";\n" +
		"CREATE TABLE c (id Int64 NOT NULL, PRIMARY KEY (id)) ;\nUPSERT INTO c (id) VALUES ($v);\nDECLARE $p AS Int64;"

	queries, err := yqlquery.Split(text)

	c.Assert(err, qt.IsNil)
	sources := make([]string, 0, len(queries))
	statements := make([][]string, 0, len(queries))
	for _, query := range queries {
		sources = append(sources, query.Source)
		statements = append(statements, query.Statements)
	}
	c.Assert(sources, qt.DeepEquals, []string{
		"DROP TABLE b;",
		"$v = 2l;\nPRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE c (id Int64 NOT NULL, PRIMARY KEY (id)) ;",
		"UPSERT INTO c (id) VALUES ($v);\nDECLARE $p AS Int64;",
	})
	c.Assert(statements, qt.DeepEquals, [][]string{
		{"DROP TABLE b"},
		{"CREATE TABLE c (id Int64 NOT NULL, PRIMARY KEY (id))"},
		{"UPSERT INTO c (id) VALUES ($v)", "DECLARE $p AS Int64"},
	})
}

func TestSplit_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		wantErr  error
		wantText string
	}{
		{
			name:     "the ANSI lexer reads text differently",
			text:     "--!ansi_lexer\nSELECT 1;",
			wantErr:  yqlquery.ErrTranslationSetting,
			wantText: `unsupported YQL translation setting "--!ansi_lexer": .*`,
		},
		{
			name:     "PostgreSQL syntax is disabled on the server",
			text:     "--!syntax_v1\n--!syntax_pg\nSELECT 1;",
			wantErr:  yqlquery.ErrTranslationSetting,
			wantText: `unsupported YQL translation setting "--!syntax_pg": .*`,
		},
		{
			name:     "a setting YDB does not know",
			text:     "--!antlr4\nSELECT 1;",
			wantErr:  yqlquery.ErrTranslationSetting,
			wantText: `unsupported YQL translation setting "--!antlr4": .*`,
		},
		{
			name:     "a client delimiter statement",
			text:     "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id));\nDELIMITER //\nDROP TABLE a //",
			wantErr:  yqlquery.ErrClientDelimiter,
			wantText: `client delimiter directive in YQL: "DELIMITER //" has no meaning in YQL.*`,
		},
		{
			name:     "a client delimiter right after the translation setting",
			text:     "--!syntax_v1\nDELIMITER //\nDROP TABLE a //",
			wantErr:  yqlquery.ErrClientDelimiter,
			wantText: `client delimiter directive in YQL: "DELIMITER //" has no meaning in YQL.*`,
		},
		{
			name:     "an Atlas delimiter directive",
			text:     "-- atlas:delimiter //\nDROP TABLE a //",
			wantErr:  yqlquery.ErrClientDelimiter,
			wantText: `client delimiter directive in YQL: "-- atlas:delimiter //" has no meaning in YQL.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queries, err := yqlquery.Split(test.text)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(err, qt.ErrorMatches, test.wantText)
			c.Assert(queries, qt.IsNil)
		})
	}
}

func TestKindOf_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want yqlquery.Kind
	}{
		{name: "scheme statement", text: "CREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))", want: yqlquery.Scheme},
		{name: "scheme statement with carried definitions", text: "--!syntax_v1\n$v = 1l;\nPRAGMA x;\nDROP TABLE a;\n", want: yqlquery.Scheme},
		{name: "data statements", text: "$v = 1l;\nUPSERT INTO a (id) VALUES ($v);\nDELETE FROM b;\n", want: yqlquery.Data},
		{name: "definitions alone", text: "$v = 1l;", want: yqlquery.Data},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			kind, ok := yqlquery.KindOf(test.text)
			c.Assert(ok, qt.IsTrue)
			c.Assert(kind, qt.Equals, test.want)
		})
	}
}

func TestKindOf_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "two queries", text: "DROP TABLE a; DROP TABLE b;"},
		{name: "no query", text: "-- nothing"},
		{name: "a text Split refuses", text: "--!ansi_lexer\nDROP TABLE a;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			kind, ok := yqlquery.KindOf(test.text)
			c.Assert(ok, qt.IsFalse)
			c.Assert(kind, qt.Equals, yqlquery.Kind(0))
		})
	}
}

func TestKind_String(t *testing.T) {
	c := qt.New(t)
	c.Assert(yqlquery.Data.String(), qt.Equals, "data")
	c.Assert(yqlquery.Scheme.String(), qt.Equals, "scheme")
	c.Assert(yqlquery.Kind(0).String(), qt.Equals, "unknown")
}

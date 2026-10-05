package ydbcomment_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbcomment"
)

// Each statement is written in the PostgreSQL family's grammar, with YDB's
// quoting, and an empty comment as NULL.
func TestStatement_Text_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement ydbcomment.Statement
		want      string
	}{
		{name: "table", statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "shop/users", Comment: "People"},
			want: "COMMENT ON TABLE `shop/users` IS 'People'"},
		{name: "column", statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "users", Name: "email", Comment: "Login"},
			want: "COMMENT ON COLUMN `users`.`email` IS 'Login'"},
		{name: "index", statement: ydbcomment.Statement{Object: ydbcomment.Index, Path: "users", Name: "by_email", Comment: "Lookup"},
			want: "COMMENT ON INDEX `by_email` ON `users` IS 'Lookup'"},
		{name: "view", statement: ydbcomment.Statement{Object: ydbcomment.View, Path: "shop/active", Comment: "Recent"},
			want: "COMMENT ON VIEW `shop/active` IS 'Recent'"},
		{name: "removal", statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "users", Name: "email"},
			want: "COMMENT ON COLUMN `users`.`email` IS NULL"},
		{name: "escapes", statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t", Comment: "it's a\\b\n"},
			want: `COMMENT ON TABLE ` + "`t`" + ` IS 'it\'s a\\b\n'`},
		{name: "a backtick in a name", statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "t", Name: "a`b", Comment: "x"},
			want: "COMMENT ON COLUMN `t`.`a\\`b` IS 'x'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.statement.Text()
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A statement that names nothing, or a comment YDB cannot hold, is not
// written.
func TestStatement_Text_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement ydbcomment.Statement
		wantErr   string
	}{
		{name: "no object", statement: ydbcomment.Statement{Path: "t"},
			wantErr: `invalid comment statement: unknown object 0`},
		{name: "no column", statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "t", Comment: "x"},
			wantErr: `invalid comment statement: a column comment names no column`},
		{name: "too long", statement: ydbcomment.Statement{Object: ydbcomment.View, Path: "v", Comment: strings.Repeat("x", 4097)},
			wantErr: `invalid comment statement: the comment is 4097 bytes long, and YDB keeps at most 4096 bytes in an attribute value`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.statement.Text()
			c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// The attribute a statement sets is the object's key.
func TestStatement_Key(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbcomment.Statement{Object: ydbcomment.Index, Path: "t", Name: "i"}.Key(), qt.Equals, "ptah.comment.index.i")
	c.Assert(ydbcomment.Statement{Object: ydbcomment.View, Path: "v"}.Key(), qt.Equals, "ptah.comment")
}

// A statement Text wrote reads back as the statement it was written from,
// whatever its comment holds, so a plan file runs the change the plan made.
func TestRecognize_ReadsBackWhatTextWrote(t *testing.T) {
	comments := []string{
		"",
		"plain",
		`quotes ' and " and backticks ` + "`",
		`a backslash \ and \n written out`,
		"a newline\nand a tab\tand a return\r",
		"a NUL \x00 and DEL \x7f",
		"текст и 漢字",
		strings.Repeat("x", 4096),
		"-- not a comment; COMMENT ON TABLE t IS 'x'",
	}
	objects := []ydbcomment.Statement{
		{Object: ydbcomment.Table, Path: "shop/users"},
		{Object: ydbcomment.Column, Path: "users", Name: "e-mail`x"},
		{Object: ydbcomment.Index, Path: "dir/sub/t", Name: "a.b"},
		{Object: ydbcomment.View, Path: "/local/abs/v"},
	}
	for _, object := range objects {
		for _, comment := range comments {
			t.Run(object.Path+"/"+strings.ReplaceAll(comment[:min(12, len(comment))], "\n", " "), func(t *testing.T) {
				c := qt.New(t)
				statement := object
				statement.Comment = comment
				text, err := statement.Text()
				c.Assert(err, qt.IsNil)
				query, recognized, err := ydbcomment.Recognize(text + ";")
				c.Assert(err, qt.IsNil)
				c.Assert(recognized, qt.IsTrue)
				c.Assert(query, qt.DeepEquals, ydbcomment.Query{Statement: statement})
			})
		}
	}
}

// A hand-written statement reads too: lower case, plain names, double quotes,
// an empty string as a removal, and the path prefix in effect.
func TestRecognize_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want ydbcomment.Query
	}{
		{name: "lower case with plain names", text: "comment on table users is 'x'",
			want: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "users", Comment: "x"}}},
		{name: "double quotes with the Utf8 suffix", text: `COMMENT ON VIEW v IS "it's"u`,
			want: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.View, Path: "v", Comment: "it's"}}},
		{name: "an empty string removes", text: "COMMENT ON COLUMN t.c IS ''",
			want: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "t", Name: "c"}}},
		{name: "NULL in lower case", text: "COMMENT ON INDEX i ON t IS null",
			want: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Index, Path: "t", Name: "i"}}},
		{name: "under a prefix", text: "PRAGMA TablePathPrefix('/local/.ptah/r');\nCOMMENT ON TABLE `t` IS 'x';",
			want: ydbcomment.Query{
				Statement:  ydbcomment.Statement{Object: ydbcomment.Table, Path: "t", Comment: "x"},
				PathPrefix: "/local/.ptah/r",
			}},
		{name: "after the translation setting", text: "--!syntax_v1\nCOMMENT ON TABLE `t` IS 'x'",
			want: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t", Comment: "x"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, recognized, err := ydbcomment.Recognize(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(query, qt.DeepEquals, test.want)
		})
	}
}

// COMMENT ON another kind of object is not Ptah's statement: it reaches YDB,
// whose parse error says YQL has none. Neither is text that only mentions one.
func TestRecognize_NotRecognized(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "a sequence", text: "COMMENT ON SEQUENCE s IS 'x'"},
		{name: "a constraint", text: "COMMENT ON CONSTRAINT k ON t IS 'x'"},
		{name: "a schema", text: "COMMENT ON SCHEMA s IS 'x'"},
		{name: "a materialized view", text: "COMMENT ON MATERIALIZED VIEW m IS 'x'"},
		{name: "a string", text: "SELECT 'COMMENT ON TABLE t IS x'"},
		{name: "a line comment", text: "-- COMMENT ON TABLE t IS 'x'\nSELECT 1"},
		{name: "a column named comment", text: "SELECT comment FROM t"},
		{name: "no comment at all", text: "CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id))"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, recognized, err := ydbcomment.Recognize(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsFalse)
			c.Assert(query, qt.DeepEquals, ydbcomment.Query{})
		})
	}
}

// A statement Ptah cannot run as written is refused before anything reaches
// YDB, with what to write instead.
func TestRecognize_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "no IS", text: "COMMENT ON TABLE t 'x'",
			wantErr: `invalid comment statement: COMMENT ON TABLE: the comment follows IS, as a string or NULL, and nothing follows it`},
		{name: "something after the comment", text: "COMMENT ON TABLE t IS 'x' 'y'",
			wantErr: `invalid comment statement: COMMENT ON TABLE: the comment follows IS, as a string or NULL, and nothing follows it`},
		{name: "no table", text: "COMMENT ON TABLE",
			wantErr: `invalid comment statement: COMMENT ON TABLE: the statement names no table`},
		{name: "a named expression", text: "COMMENT ON TABLE $t IS 'x'",
			wantErr: `invalid comment statement: COMMENT ON TABLE: \$t does not name a table; write its path in backticks`},
		{name: "a column without its table", text: "COMMENT ON COLUMN c IS 'x'",
			wantErr: "invalid comment statement: COMMENT ON COLUMN: name the column as `table`.`column`"},
		{name: "an index without its table", text: "COMMENT ON INDEX i IS 'x'",
			wantErr: "invalid comment statement: COMMENT ON INDEX: name the index as `index` ON `table`"},
		{name: "a number", text: "COMMENT ON TABLE t IS 5",
			wantErr: `invalid comment statement: COMMENT ON TABLE: the comment 5 is not a string Ptah reads; .*`},
		{name: "a raw string", text: "COMMENT ON TABLE t IS @@x@@",
			wantErr: `invalid comment statement: COMMENT ON TABLE: the comment @@x@@ is not a string Ptah reads; .*`},
		{name: "too long", text: "COMMENT ON VIEW v IS '" + strings.Repeat("x", 4097) + "'",
			wantErr: `invalid comment statement: COMMENT ON VIEW: the comment is 4097 bytes long, and YDB keeps at most 4096 bytes in an attribute value`},
		{name: "a column name too long", text: "COMMENT ON COLUMN t." + strings.Repeat("c", 81) + " IS NULL",
			wantErr: `invalid comment statement: COMMENT ON COLUMN: YDB keeps the comment as the table attribute .*; a column name takes at most 80`},
		{name: "two statements", text: "COMMENT ON TABLE t IS 'a'; COMMENT ON TABLE u IS 'b'",
			wantErr: `invalid comment statement: a query runs one COMMENT ON TABLE statement, and this one holds two; put each in a query of its own`},
		{name: "not last", text: "COMMENT ON TABLE t IS 'a'; SELECT 1",
			wantErr: `invalid comment statement: COMMENT ON TABLE is followed by another statement in its query, and Ptah runs it through YDB's table service, which takes it alone`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, recognized, err := ydbcomment.Recognize(test.text)
			c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(query, qt.DeepEquals, ydbcomment.Query{})
		})
	}
}

// The absolute path of the object a query names follows the prefix in effect.
func TestQuery_Absolute_HappyPath(t *testing.T) {
	c := qt.New(t)
	query := ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t"}, PathPrefix: "/local/r"}
	got, err := query.Absolute("/local")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "/local/r/t")
}

// A relative prefix is refused as YDB refuses it.
func TestQuery_Absolute_FailurePath(t *testing.T) {
	c := qt.New(t)
	query := ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t"}, PathPrefix: "relp"}
	got, err := query.Absolute("/local")
	c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
	c.Assert(err, qt.ErrorMatches, `invalid comment statement: path relp/t is not in database /local: .*`)
	c.Assert(got, qt.Equals, "")
}

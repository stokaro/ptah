package ydbcomment_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbcomment"
)

// Each object's comment has its own key: a table's and a view's is the one
// own key, and a column's and an index's carry the name after a prefix of
// their own, so the two can never name the same attribute.
func TestKey(t *testing.T) {
	tests := []struct {
		name   string
		object ydbcomment.Object
		of     string
		want   string
	}{
		{name: "table", object: ydbcomment.Table, want: "ptah.comment"},
		{name: "view", object: ydbcomment.View, want: "ptah.comment"},
		{name: "column", object: ydbcomment.Column, of: "email", want: "ptah.comment.column.email"},
		{name: "index", object: ydbcomment.Index, of: "users_email_idx", want: "ptah.comment.index.users_email_idx"},
		{name: "index with a dot", object: ydbcomment.Index, of: "a.b", want: "ptah.comment.index.a.b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcomment.Key(test.object, test.of), qt.Equals, test.want)
		})
	}
}

// Read takes the comments out of an object's attributes and leaves every other
// attribute out: YDB's own, another tool's, and a key under Ptah's prefix that
// names no column or index.
func TestRead(t *testing.T) {
	c := qt.New(t)
	got := ydbcomment.Read(map[string]string{
		"ptah.comment":                "Users who sign in",
		"ptah.comment.column.email":   "Login",
		"ptah.comment.column.id":      "",
		"ptah.comment.index.by_email": "Lookup",
		"ptah.comment.index.a.b":      "dotted",
		"ptah.comment.column.":        "no name",
		"ptah.comment.index.":         "no name",
		"ptah.comment.other":          "someone else's",
		"ptah.commentary":             "someone else's",
		"__async_replica":             "true",
		"owner":                       "team",
	})
	c.Assert(got, qt.DeepEquals, ydbcomment.Comments{
		Own:     "Users who sign in",
		Columns: map[string]string{"email": "Login"},
		Indexes: map[string]string{"by_email": "Lookup", "a.b": "dotted"},
	})
}

// Attributes with no comment under Ptah's keys read as no comment at all.
func TestRead_NoComments(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbcomment.Read(map[string]string{"__async_replica": "true"}), qt.DeepEquals, ydbcomment.Comments{})
	c.Assert(ydbcomment.Read(nil), qt.DeepEquals, ydbcomment.Comments{})
}

// Attributes writes back the keys Read reads, and leaves an empty comment out.
func TestComments_Attributes(t *testing.T) {
	c := qt.New(t)
	comments := ydbcomment.Comments{
		Own:     "t",
		Columns: map[string]string{"a": "ca", "b": ""},
		Indexes: map[string]string{"i": "ci"},
	}
	c.Assert(comments.Attributes(), qt.DeepEquals, map[string]string{
		"ptah.comment":          "t",
		"ptah.comment.column.a": "ca",
		"ptah.comment.index.i":  "ci",
	})
	c.Assert(ydbcomment.Read(comments.Attributes()), qt.DeepEquals, ydbcomment.Comments{
		Own:     "t",
		Columns: map[string]string{"a": "ca"},
		Indexes: map[string]string{"i": "ci"},
	})
}

// A comment YDB can hold is accepted up to each limit, which is counted in
// bytes.
func TestRefusal_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		object  ydbcomment.Object
		of      string
		comment string
	}{
		{name: "table", object: ydbcomment.Table, comment: "x"},
		{name: "no comment", object: ydbcomment.Column, of: "c"},
		{name: "a column name of 80 bytes", object: ydbcomment.Column, of: strings.Repeat("c", 80), comment: "x"},
		{name: "an index name of 81 bytes", object: ydbcomment.Index, of: strings.Repeat("i", 81), comment: "x"},
		{name: "4096 bytes", object: ydbcomment.Table, comment: strings.Repeat("x", 4096)},
		{name: "2048 two-byte characters", object: ydbcomment.View, comment: strings.Repeat("é", 2048)},
		{name: "control characters", object: ydbcomment.Table, comment: "a\nb\x00c"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcomment.Refusal(test.object, test.of, test.comment), qt.Equals, "")
		})
	}
}

// A comment YDB cannot hold is refused with the limit it breaks.
func TestRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		object  ydbcomment.Object
		of      string
		comment string
		want    string
	}{
		{name: "a column name of 81 bytes", object: ydbcomment.Column, of: strings.Repeat("c", 81), comment: "x",
			want: `YDB keeps the comment as the table attribute "ptah.comment.column.` + strings.Repeat("c", 81) +
				`", 101 bytes long, and an attribute key takes at most 100 bytes; a column name takes at most 80`},
		{name: "an index name of 82 bytes", object: ydbcomment.Index, of: strings.Repeat("i", 82),
			want: `YDB keeps the comment as the table attribute "ptah.comment.index.` + strings.Repeat("i", 82) +
				`", 101 bytes long, and an attribute key takes at most 100 bytes; an index name takes at most 81`},
		{name: "4097 bytes", object: ydbcomment.Table, comment: strings.Repeat("x", 4097),
			want: "the comment is 4097 bytes long, and YDB keeps at most 4096 bytes in an attribute value"},
		{name: "2049 two-byte characters", object: ydbcomment.Table, comment: strings.Repeat("é", 2049),
			want: "the comment is 4098 bytes long, and YDB keeps at most 4096 bytes in an attribute value"},
		{name: "not UTF-8", object: ydbcomment.Table, comment: "\xff\xfe",
			want: "the comment is not UTF-8 text, which YDB keeps in an attribute value"},
		{name: "a column with no name", object: ydbcomment.Column, comment: "x",
			want: "a column comment names no column"},
		{name: "an index with no name", object: ydbcomment.Index, comment: "x",
			want: "an index comment names no index"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcomment.Refusal(test.object, test.of, test.comment), qt.Equals, test.want)
		})
	}
}

// An object holds its comments while they take MaxObjectBytes or less, keys
// and text together, and not one byte more.
func TestComments_SizeRefusal(t *testing.T) {
	// "ptah.comment" is 12 bytes, and two column keys of one-letter names are
	// 21 bytes each: 54 bytes of keys.
	fits := ydbcomment.Comments{
		Own:     strings.Repeat("t", 4096),
		Columns: map[string]string{"a": strings.Repeat("a", 4096), "b": strings.Repeat("b", 10240-54-8192)},
	}
	over := ydbcomment.Comments{
		Own:     fits.Own,
		Columns: map[string]string{"a": fits.Columns["a"], "b": fits.Columns["b"] + "b"},
	}
	c := qt.New(t)
	c.Assert(ydbcomment.Size(fits.Attributes()), qt.Equals, 10240)
	c.Assert(fits.SizeRefusal(`table "t"`), qt.Equals, "")
	c.Assert(over.SizeRefusal(`table "t"`), qt.Equals, `the comments of table "t" take 10241 bytes as YDB `+
		`table attributes, keys and text together, and YDB keeps at most 10240 bytes of attributes on one object`)
}

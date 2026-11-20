package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

func TestCommentText(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain text is unchanged", in: "a\tb 'c' \"d\" é", want: "a\tb 'c' \"d\" é"},
		{name: "line feed", in: "a\nb", want: `a\nb`},
		{name: "carriage return", in: "a\rb", want: `a\rb`},
		{name: "crlf", in: "a\r\nb", want: `a\r\nb`},
		{name: "vertical tab and form feed", in: "a\vb\fc", want: `a\x0bb\x0cc`},
		{name: "nul and delete", in: "a\x00b\x7fc", want: `a\x00b\x7fc`},
		{name: "next line", in: "a\u0085b", want: `a\x85b`},
		{name: "line and paragraph separator", in: "a\u2028b\u2029c", want: `a\u2028b\u2029c`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.CommentText(test.in), qt.Equals, test.want)
		})
	}
}

func TestLineComment(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty text is the bare marker", in: "", want: "--"},
		{name: "text", in: "note", want: "-- note"},
		{name: "injection stays one line", in: "first\nDROP TABLE victim;\n-- end", want: `-- first\nDROP TABLE victim;\n-- end`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.LineComment(test.in), qt.Equals, test.want)
		})
	}
}

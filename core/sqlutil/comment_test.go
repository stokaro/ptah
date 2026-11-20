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

func TestNeutralizeDirectives(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "text without a directive is unchanged", in: "audit log for the users table", want: "audit log for the users table"},
		{name: "a +ptah marker", in: "+ptah no_transaction", want: "+ ptah no_transaction"},
		{name: "a +ptah marker in upper case", in: "+PTAH no_transaction", want: "+ PTAH no_transaction"},
		{name: "a +goose pragma", in: "+goose NO TRANSACTION", want: "+ goose NO TRANSACTION"},
		{name: "atlas:txmode", in: "atlas:txmode none", want: "atlas :txmode none"},
		{name: "atlas:delimiter", in: "atlas:delimiter $$", want: "atlas :delimiter $$"},
		{name: "atlas:checkpoint", in: "atlas:checkpoint", want: "atlas :checkpoint"},
		{name: "atlas:nolint", in: "atlas:nolint DS101", want: "atlas :nolint DS101"},
		{name: "atlas:txtar", in: "atlas:txtar", want: "atlas :txtar"},
		{name: "atlas:import", in: "atlas:import other.sql", want: "atlas :import other.sql"},
		{name: "a dbmate marker", in: "migrate:up", want: "migrate :up"},
		{name: "a schema annotation", in: "ptah:schema:field name=x", want: "ptah :schema:field name=x"},
		{name: "a marker inside a word", in: "xatlas:txmode none", want: "xatlas :txmode none"},
		{name: "a marker in the middle", in: "see atlas:txmode none here", want: "see atlas :txmode none here"},
		{name: "two markers", in: "+ptah a atlas:txmode b", want: "+ ptah a atlas :txmode b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.NeutralizeDirectives(test.in), qt.Equals, test.want)
		})
	}
}

func TestLineComment_BreaksADirective(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlutil.LineComment("+ptah no_transaction"), qt.Equals, "-- + ptah no_transaction")
}

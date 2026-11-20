package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/internal/atlashcl"
)

// TestURLFunctions_HappyPath pins each URL function to the value the pinned
// community binary v1.4.0 returned for the same call, read through the path a
// `data "hcl_schema"` block reported as missing, which prints the value byte
// for byte.
func TestURLFunctions_HappyPath(t *testing.T) {
	null := cty.NullVal(cty.String)
	tests := []struct {
		name string
		fn   string
		args []cty.Value
		want string
	}{
		{name: "escape every reserved character", fn: "urlescape",
			args: []cty.Value{cty.StringVal("a b@c/d?e=f&g#h+i%")}, want: "a+b%40c%2Fd%3Fe%3Df%26g%23h%2Bi%25"},
		{name: "user and password with reserved characters", fn: "urluserinfo",
			args: []cty.Value{cty.StringVal("postgres://h:5432/d?x=1"), cty.StringVal("a@b"), cty.StringVal("p@ss:w/rd")},
			want: "postgres://a%40b:p%40ss%3Aw%2Frd@h:5432/d?x=1"},
		{name: "a user alone drops the old password", fn: "urluserinfo",
			args: []cty.Value{cty.StringVal("postgres://old:pw@h/d"), cty.StringVal("a@b")}, want: "postgres://a%40b@h/d"},
		{name: "an empty password is kept", fn: "urluserinfo",
			args: []cty.Value{cty.StringVal("postgres://old:pw@h/d"), cty.StringVal("a@b"), cty.StringVal("")}, want: "postgres://a%40b:@h/d"},
		{name: "a null password is no password", fn: "urluserinfo",
			args: []cty.Value{cty.StringVal("postgres://h/d"), cty.StringVal("u"), null}, want: "postgres://u@h/d"},
		{name: "an empty user", fn: "urluserinfo",
			args: []cty.Value{cty.StringVal("postgres://h/d"), cty.StringVal(""), cty.StringVal("p")}, want: "postgres://:p@h/d"},
		{name: "replace a parameter and sort the query", fn: "urlqueryset",
			args: []cty.Value{cty.StringVal("postgres://h/d?x=1&sslmode=require"), cty.StringVal("sslmode"), cty.StringVal("disable")},
			want: "postgres://h/d?sslmode=disable&x=1"},
		{name: "add a parameter that needs escaping", fn: "urlqueryset",
			args: []cty.Value{cty.StringVal("postgres://h/d?b=2&a=1"), cty.StringVal("search_path"), cty.StringVal("a b,c")},
			want: "postgres://h/d?a=1&b=2&search_path=a+b%2Cc"},
		{name: "a repeated parameter becomes one", fn: "urlqueryset",
			args: []cty.Value{cty.StringVal("postgres://h/d?a=1&a=2"), cty.StringVal("a"), cty.StringVal("3")}, want: "postgres://h/d?a=3"},
		{name: "a path without a slash gets one", fn: "urlsetpath",
			args: []cty.Value{cty.StringVal("postgres://h/d?x=1"), cty.StringVal("other db")}, want: "postgres://h/other%20db?x=1"},
		{name: "a path with a slash", fn: "urlsetpath",
			args: []cty.Value{cty.StringVal("postgres://h/d?x=1"), cty.StringVal("/other")}, want: "postgres://h/other?x=1"},
		{name: "an empty path", fn: "urlsetpath",
			args: []cty.Value{cty.StringVal("postgres://h?x=1"), cty.StringVal("")}, want: "postgres://h?x=1"},
		{name: "the fragment stays", fn: "urlsetpath",
			args: []cty.Value{cty.StringVal("postgres://h/d#frag"), cty.StringVal("a")}, want: "postgres://h/a#frag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlashcl.ProjectFunctions()[tt.fn].Call(tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got.AsString(), qt.Equals, tt.want)
		})
	}
}

// TestURLFunctions_FailurePath holds the refusals: a URL that does not parse,
// as the pinned binary refuses it, and a query that does not decode, which
// that binary silently drops and Ptah refuses because the dropped pair is part
// of what the author wrote.
func TestURLFunctions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		fn      string
		args    []cty.Value
		wantErr string
	}{
		{name: "userinfo on a URL that does not parse", fn: "urluserinfo",
			args:    []cty.Value{cty.StringVal("postgres://h/%zz"), cty.StringVal("u")},
			wantErr: `parse "postgres://h/%zz": invalid URL escape "%zz"`},
		{name: "a path on a URL that does not parse", fn: "urlsetpath",
			args:    []cty.Value{cty.StringVal("postgres://h/%zz"), cty.StringVal("a")},
			wantErr: `parse "postgres://h/%zz": invalid URL escape "%zz"`},
		{name: "a query that does not decode", fn: "urlqueryset",
			args:    []cty.Value{cty.StringVal("postgres://h/d?%zz=1"), cty.StringVal("a"), cty.StringVal("b")},
			wantErr: `the query of "postgres://h/d\?%zz=1" does not decode, so setting "a" would drop part of it: invalid URL escape "%zz"`},
		{name: "two passwords", fn: "urluserinfo",
			args:    []cty.Value{cty.StringVal("postgres://h/d"), cty.StringVal("u"), cty.StringVal("p"), cty.StringVal("q")},
			wantErr: `urluserinfo takes a URL, a user and at most one password, got 4 arguments`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlashcl.ProjectFunctions()[tt.fn].Call(tt.args)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.Equals, cty.NilVal)
		})
	}
}

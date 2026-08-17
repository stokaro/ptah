package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// functionDocument puts one call in a column default, which is where the
// function set stokaro/ptah#1627 measured was probed on the pinned community
// binary v1.3.0.
func functionDocument(call string) []byte {
	return []byte(`schema "main" {
}

table "t" {
  schema = schema.main
  column "c" {
    type    = text
    default = ` + call + `
  }
}
`)
}

// TestSchemaFunctions_MeasuredPresentNamesResolve covers the three names
// stokaro/ptah#1627 found present on that binary and unimplemented here.
//
// The expected values are that binary's own, read on 2026-08-17 through
// `schema diff --dev-url sqlite://file?mode=memory`. The YAML quoting is the
// part worth pinning: `yamlencode({a = 1})` renders the key QUOTED there, which
// is go-cty-yaml's style rather than what marshalling a Go map produces, so a
// hand-rolled encoder would have diverged on the first map.
func TestSchemaFunctions_MeasuredPresentNamesResolve(t *testing.T) {
	tests := []struct {
		name string
		call string
		want string
	}{
		{
			name: "yamlencode quotes map keys",
			call: `yamlencode({a = 1})`,
			want: "\"a\": 1\n",
		},
		{
			name: "yamlencode quotes list strings",
			call: `yamlencode(["x", "y"])`,
			want: "- \"x\"\n- \"y\"\n",
		},
		{
			name: "yamlencode quotes a plain string",
			call: `yamlencode("plain")`,
			want: "\"plain\"\n",
		},
		{
			name: "yamldecode reads a mapping",
			call: `tostring(yamldecode("a: 1").a)`,
			want: "1",
		},
		{
			name: "print returns its argument",
			call: `print("hello")`,
			want: "hello",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(functionDocument(test.call), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Default, qt.Equals, test.want)
		})
	}
}

// TestSchemaFunctions_PrintWritesTheValueItReturns pins the side effect, which
// is the whole reason the function exists: an operator debugging a schema file
// wants to see the value, and returning it unchanged is what lets the call be
// wrapped around an expression rather than written beside it.
//
// The destination is standard output on that binary, measured with the streams
// separated: `print("hello")` puts `hello` on stdout and nothing on stderr.
func TestSchemaFunctions_PrintWritesTheValueItReturns(t *testing.T) {
	c := qt.New(t)
	var printed strings.Builder
	atlashcl.SetPrintDestinationForTest(c, &printed)

	db, err := atlashcl.Parse(functionDocument(`print("hello")`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields[0].Default, qt.Equals, "hello")
	c.Assert(printed.String(), qt.Equals, "hello\n")
}

// TestSchemaFunctions_UUIDIsATypeAndNotAFunction records the reading that
// removed the fourth name from that issue's list.
//
// `uuid` was recorded as measured-present because calling it does not answer
// with the absent-marker. Asked again, the answer is `Type "uuid" does not
// accept attributes` -- a type keyword, not a function -- and `type = uuid`
// renders a `uuid` column at exit 0 on both binaries. So there is nothing to
// implement and nothing to argue: the original probe read one non-absent answer
// as presence.
func TestSchemaFunctions_UUIDIsATypeAndNotAFunction(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`schema "main" {
}

table "t" {
  schema = schema.main
  column "c" {
    type = uuid
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(strings.ToLower(db.Fields[0].Type), qt.Equals, "uuid")

	_, callErr := atlashcl.Parse(functionDocument(`uuid()`), "schema.hcl")

	c.Assert(callErr, qt.IsNotNil)
}

package schemaext_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestCanonicalJSON_PreservePrecisionAndOrderedValues(t *testing.T) {
	tests := []struct{ input, want string }{
		{input: ` { "b": [], "a": 18446744073709551615 } `, want: `{"a":18446744073709551615,"b":[]}`},
		{input: `{"b":[2,1],"a":null}`, want: `{"a":null,"b":[2,1]}`},
		{input: `"\ud83d\ude00"`, want: `"😀"`},
		{input: `"\\ud800"`, want: `"\\ud800"`},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			c := qt.New(t)
			got, err := schemaext.CanonicalJSON([]byte(test.input))
			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, test.want)
		})
	}
}

func TestCanonicalJSON_RefuseAmbiguousOrLossyInput(t *testing.T) {
	for _, input := range []string{
		`{"x":1,"x":2}`, `{"outer":{"x":1,"\u0078":2}}`, `[] []`, `{`,
		`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`, "\"\xff\"",
		strings.Repeat("[", 258) + "0" + strings.Repeat("]", 258),
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			got, err := schemaext.CanonicalJSON([]byte(input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.IsNil)
		})
	}
}

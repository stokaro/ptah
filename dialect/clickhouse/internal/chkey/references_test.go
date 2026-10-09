package chkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/clickhouse/internal/chkey"
)

func TestPlainColumnReferences(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		want       []string
	}{
		{name: "one column", expression: "payload", want: []string{"payload"}},
		{name: "a catalog list", expression: "a, b", want: []string{"a", "b"}},
		{name: "a tuple", expression: "(a, `B c`, \"d\")", want: []string{"a", "B c", "d"}},
		{name: "call arguments", expression: "cityHash64(a, b)", want: []string{"a", "b"}},
		{name: "a nested call", expression: "lower(trim(payload))", want: []string{"payload"}},
		{name: "literals", expression: "tupleElement(t, 1), has(arr, NULL), if(f, nan, inf)", want: []string{"t", "arr", "f"}},
		{name: "operators beside a name", expression: "lower(paylod) + 1, a > 0", want: []string{"paylod"}},
		{name: "a lambda", expression: "arrayMap(x -> lower(x), arr)", want: []string{"arr"}},
		{name: "a lambda over a tuple", expression: "arrayMap((x, y) -> concat(x, y), a, b)", want: []string{"a", "b"}},
		{name: "a type after AS", expression: "CAST(x AS Nullable(String))", want: nil},
		{name: "a type after ::", expression: "toString(x::Nullable(String)), y", want: []string{"y"}},
		{name: "an interval", expression: "toStartOfInterval(ts, INTERVAL 1 HOUR)", want: []string{"ts"}},
		{name: "member access", expression: "n.size0, m['k']", want: nil},
		{name: "strings", expression: "JSONExtractString(j, 'k')", want: []string{"j"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chkey.PlainColumnReferences(test.expression), qt.DeepEquals, test.want)
		})
	}
}

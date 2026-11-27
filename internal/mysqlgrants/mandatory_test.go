package mysqlgrants_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlgrants"
)

// TestMandatoryAccounts reads the server setting rather than SQL string
// literals: the server's list and account readers have different quote rules.
func TestMandatoryAccounts(t *testing.T) {
	tests := []struct {
		name              string
		value             string
		doubleQuotedLists bool
		want              []mysqlgrants.Account
	}{
		{name: "empty"},
		{name: "default host", value: " reader , writer ", want: []mysqlgrants.Account{{User: "reader", Host: "%"}, {User: "writer", Host: "%"}}},
		{name: "host spelling", value: "'Reader' @ 'LOCALHOST'", want: []mysqlgrants.Account{{User: "Reader", Host: "LOCALHOST"}}},
		{name: "empty host", value: "reader@'',writer@", want: []mysqlgrants.Account{{User: "reader", Host: "%"}, {User: "writer", Host: "%"}}},
		{name: "quoted separators", value: "'a,b@c'@'h,x',`d,e@f`@`h,y`", want: []mysqlgrants.Account{{User: "a,b@c", Host: "h,x"}, {User: "d,e@f", Host: "h,y"}}},
		{name: "quotes of another kind", value: "`O'k`@'h', 'a`b'@'h'", want: []mysqlgrants.Account{{User: "O'k", Host: "h"}, {User: "a`b", Host: "h"}}},
		{name: "doubled quotes are delimiters", value: "'O''k'@'h'", want: []mysqlgrants.Account{{User: "Ok", Host: "h"}}},
		{name: "backslashes are bytes", value: "`a\\b`@'h'", want: []mysqlgrants.Account{{User: "a\\b", Host: "h"}}},
		{name: "quoted spaces", value: "'a b'@'h ', r\t@h", want: []mysqlgrants.Account{{User: "a b", Host: "h "}, {User: "r\t", Host: "h"}}},
		{name: "Unicode whitespace is literal", value: "r\u00a0@h\u2003", want: []mysqlgrants.Account{{User: "r\u00a0", Host: "h\u2003"}}},
		{name: "repeated separators", value: "reader@@local@host", want: []mysqlgrants.Account{{User: "reader", Host: "localhost"}}},
		{name: "duplicates", value: "reader,reader", want: []mysqlgrants.Account{{User: "reader", Host: "%"}, {User: "reader", Host: "%"}}},
		{name: "empty users", value: "'',@host,reader", want: []mysqlgrants.Account{{User: "reader", Host: "%"}}},
		{name: "unfinished quote", value: "'reader", want: []mysqlgrants.Account{{User: "reader", Host: "%"}}},
		{name: "older double quoted list", value: `"a,b"@"LOCALHOST"`, want: []mysqlgrants.Account{{User: "a", Host: "%"}, {User: "b@LOCALHOST", Host: "%"}}},
		{name: "newer double quoted list", value: `"a,b"@"LOCALHOST"`, doubleQuotedLists: true, want: []mysqlgrants.Account{{User: "a,b", Host: "LOCALHOST"}}},
		{name: "older double quoted space", value: `"a b"@"h "`, want: []mysqlgrants.Account{{User: "ab", Host: "h"}}},
		{name: "newer double quoted space", value: `"a b"@"h "`, doubleQuotedLists: true, want: []mysqlgrants.Account{{User: "a b", Host: "h "}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqlgrants.MandatoryAccounts(test.value, test.doubleQuotedLists)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

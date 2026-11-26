package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

// TestSemantics_StoredName pins the name each spelling reaches. Oracle stores
// a plain unquoted name folded to upper case and a quoted one as written, and
// a dialect stating no fold stores every name as written.
func TestSemantics_StoredName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		value   string
		want    string
	}{
		{name: "a bare name folds", dialect: "oracle", value: "reader", want: "READER"},
		{name: "a bare name in upper case stays", dialect: "oracle", value: "READER", want: "READER"},
		{name: "a quoted name is stored as written", dialect: "oracle", value: `"reader"`, want: "reader"},
		{name: "a quoted name in upper case is stored as written", dialect: "oracle", value: `"READER"`, want: "READER"},
		{name: "a doubled quote inside the quotes is one quote", dialect: "oracle", value: `"a""b"`, want: `a"b`},
		{name: "a bare character Oracle accepts folds with the rest", dialect: "oracle", value: "a$b", want: "A$B"},
		{name: "a name that is not a plain identifier is stored as written", dialect: "oracle", value: "my-role", want: "my-role"},
		{name: "a dialect stating no fold keeps a bare name", dialect: "postgres", value: "Reader", want: "Reader"},
		{name: "a dialect stating no fold keeps the quotes", dialect: "postgres", value: `"reader"`, want: `"reader"`},
		{name: "SQL Server ignores trailing ASCII spaces", dialect: "sqlserver", value: "reader  ", want: "reader"},
		{name: "SQL Server keeps leading spaces", dialect: "sqlserver", value: " reader", want: " reader"},
		{name: "SQL Server keeps trailing tabs", dialect: "sqlserver", value: "reader\t", want: "reader\t"},
		{name: "PostgreSQL keeps trailing spaces", dialect: "postgres", value: "reader ", want: "reader "},
		{name: "MariaDB keeps trailing spaces", dialect: "mariadb", value: "reader ", want: "reader "},
		{name: "Oracle keeps trailing spaces", dialect: "oracle", value: "READER ", want: "READER "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := identifier.ForDialect(test.dialect).StoredName(test.value)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

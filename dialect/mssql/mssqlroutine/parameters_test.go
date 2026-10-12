package mssqlroutine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/mssql/mssqlroutine"
)

func TestNormalizeParameters(t *testing.T) {
	for _, test := range []struct{ name, input, want string }{
		{"empty", "", ""},
		{"optional AS", "@a AS INT", "@a INT"},
		{"ordinary", "@a INT", "@a INT"},
		{"dimensions", "@name as NVARCHAR(50), @amount AS DECIMAL(10, 2)", "@name NVARCHAR(50), @amount DECIMAL(10,2)"},
		{"comments", "@a /* name */ AS /* type */ INT, -- next\n @b as VARCHAR(MAX)", "@a INT, @b VARCHAR(MAX)"},
		{"parameter named AS", "@AS AS INT", "@AS INT"},
		{"quoted type", "@a [AS], @b AS [some schema].[AS]", "@a [AS], @b [some schema].[AS]"},
		{"literal", "@a AS NVARCHAR(40) = N'AS,  Keep Case', @b AS INT OUTPUT", "@a NVARCHAR(40) = N'AS,  Keep Case', @b INT OUTPUT"},
		{"as outside parameter slot", "@a INT AS INT", "@a INT AS INT"},
		{"missing type", "@a AS", "@a AS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := mssqlroutine.NormalizeParameters(test.input)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(mssqlroutine.NormalizeParameters(got), qt.Equals, got)
		})
	}
}

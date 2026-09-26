package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// Canonicalize lowercases a routine's argument list and return clause the way
// PostgreSQL folds unquoted words, and keeps every quoted span as written. The
// renderer writes these fields into the CREATE, so a folded literal is a
// changed default: without the rule a routine declared `'X'` is created with
// `'x'` (stokaro/ptah#3673). A quoted name keeps its case for the same reason.
func TestFunctionCanonicalize_KeepsQuotedCase(t *testing.T) {
	rows := []struct {
		name       string
		parameters string
		want       string
	}{
		{name: "a string literal", parameters: "A INTEGER, B TEXT DEFAULT 'X'", want: "a integer, b text default 'X'"},
		{name: "a doubled quote inside a literal", parameters: "B TEXT = 'IT''S'", want: "b text = 'IT''S'"},
		{name: "an escape string", parameters: `B TEXT DEFAULT E'IT\'S'`, want: `b text default E'IT\'S'`},
		{name: "a dollar-quoted string", parameters: "B TEXT DEFAULT $$X$$", want: "b text default $$X$$"},
		{name: "a tagged dollar quote", parameters: "B TEXT DEFAULT $Q$X$Q$", want: "b text default $Q$X$Q$"},
		{name: "a quoted parameter name", parameters: `"Label" TEXT, "B""C" INT`, want: `"Label" text, "B""C" int`},
		{name: "a comment", parameters: "A INT -- Kept As Written", want: "a int -- Kept As Written"},
		{name: "a call over a literal", parameters: "B TEXT DEFAULT LOWER('Q')", want: "b text default lower('Q')"},
		{name: "unquoted words only", parameters: "IN A INT, OUT B NUMERIC(10, 2)", want: "in a int, out b numeric(10, 2)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			function := schemamodel.Function{Name: "f", Parameters: row.parameters}

			function.Canonicalize()

			c.Assert(function.Parameters, qt.Equals, row.want)
		})
	}
}

// The return clause is folded by the same rule: a column of RETURNS TABLE is
// a name, and a quoted one keeps its case.
func TestFunctionCanonicalize_KeepsQuotedCaseInTheReturnClause(t *testing.T) {
	c := qt.New(t)
	function := schemamodel.Function{Name: "f", Returns: `TABLE("Total" INTEGER, N TEXT)`}

	function.Canonicalize()

	c.Assert(function.Returns, qt.Equals, `TABLE("Total" integer, n text)`)
}

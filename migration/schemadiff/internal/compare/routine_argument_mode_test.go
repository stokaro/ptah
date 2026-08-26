package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestFunctionDefinitions_AnArgumentModeIsOneArgumentWhateverItsCase pins that
// the two sides of this comparison may spell a mode differently and still
// describe the same routine.
//
// They always do. The generated side has been through
// schemamodel.Function.Canonicalize, which lower-cases the whole parameter list;
// the database side is whatever the catalog printed, and the catalogs print
// upper case. Measured on PostgreSQL 17 and MySQL 9.7.2:
//
//	pg_get_function_arguments  a integer, OUT b integer
//	                           IN a integer, INOUT c integer
//	information_schema         OUT b int, INOUT c int
//
// Comparing those verbatim reported a difference between a database and its OWN
// description, so `ptah schema apply` planned to replace the routine with an
// identical one on every run. Measured against PostgreSQL 17, inspecting the
// database below and applying its own output back:
//
//	-- Modify function public.f_out: parameters
//	-- Modify function public.p_touch: parameters
//
// It reached a procedure first (stokaro/ptah#2209), but nothing about it is
// specific to procedures: `f_out` is an ordinary function, and its OUT argument
// never converged either.
func TestFunctionDefinitions_AnArgumentModeIsOneArgumentWhateverItsCase(t *testing.T) {
	tests := []struct {
		name     string
		desired  string
		database string
	}{
		{
			name:     "OUT as PostgreSQL prints it against OUT as Canonicalize writes it",
			desired:  "a integer, out b integer",
			database: "a integer, OUT b integer",
		},
		{
			name:     "INOUT, which a procedure's argument list is full of",
			desired:  "in a integer, inout c integer",
			database: "IN a integer, INOUT c integer",
		},
		{
			name:     "VARIADIC, the third mode that is not the default",
			desired:  "variadic parts text[]",
			database: "VARIADIC parts text[]",
		},
		{
			name:     "the default mode, written on one side only",
			desired:  "a integer",
			database: "IN a integer",
		},
		{
			name:     "a mixed spelling neither side is supposed to produce",
			desired:  "InOut c integer",
			database: "inout c integer",
		},
		{
			name:     "a type whose own parentheses hold a comma",
			desired:  "out amount numeric(10, 2)",
			database: "OUT amount numeric(10, 2)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitions(
				schemamodel.Function{Name: "r", Parameters: test.desired, Body: "BEGIN END;"},
				catalog.Function{Name: "r", Parameters: test.database, Body: "BEGIN END;"},
			)

			c.Assert(diff.Changes["parameters"], qt.Equals, "")
		})
	}
}

// TestFunctionDefinitions_ChangingAnArgumentModeIsStillAChange is the control.
//
// Folding case must not become folding the mode away: OUT, INOUT and VARIADIC
// change what an argument IS, and a routine that gained or lost one has to be
// replaced. Without these rows a fold that dropped every mode would pass the
// table above.
func TestFunctionDefinitions_ChangingAnArgumentModeIsStillAChange(t *testing.T) {
	tests := []struct {
		name     string
		desired  string
		database string
	}{
		{
			name:     "an argument that became OUT",
			desired:  "out b integer",
			database: "b integer",
		},
		{
			name:     "OUT is not INOUT",
			desired:  "out b integer",
			database: "INOUT b integer",
		},
		{
			name:     "VARIADIC is not the default",
			desired:  "parts text[]",
			database: "VARIADIC parts text[]",
		},
		{
			name:     "the argument itself still decides",
			desired:  "out b bigint",
			database: "OUT b integer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitions(
				schemamodel.Function{Name: "r", Parameters: test.desired, Body: "BEGIN END;"},
				catalog.Function{Name: "r", Parameters: test.database, Body: "BEGIN END;"},
			)

			c.Assert(diff.Changes["parameters"], qt.Not(qt.Equals), "")
		})
	}
}

// TestFunctionDefinitions_TheMySQLFamilyFoldsTheSameModes keeps the rule from
// being PostgreSQL's alone. MySQL reaches the same comparison through its own
// type normalization, and information_schema.PARAMETERS reports the mode in
// upper case there too.
func TestFunctionDefinitions_TheMySQLFamilyFoldsTheSameModes(t *testing.T) {
	c := qt.New(t)

	diff := compare.FunctionDefinitionsWithDialect(
		schemamodel.Function{Name: "p_out", Parameters: "a int, out b int, inout c int", Body: "SET b = a"},
		catalog.Function{Name: "p_out", Parameters: "a int, OUT b int, INOUT c int", Body: "SET b = a"},
		platform.MySQL,
	)

	c.Assert(diff.Changes["parameters"], qt.Equals, "")
}

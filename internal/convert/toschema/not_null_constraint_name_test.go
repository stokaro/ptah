package toschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// fieldNamed resolves one field of the parsed schema by column name.
func fieldNamed(c *qt.C, sql, column string) schemamodel.Field {
	c.Helper()

	statements, err := parser.NewParser(sql, parser.WithDialect("postgres")).Parse()
	c.Assert(err, qt.IsNil)
	database, err := toschema.ToDatabase(statements, "postgres")
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&database)

	for _, field := range database.Fields {
		if field.Name == column {
			return field
		}
	}
	c.Fatalf("no field named %q in the parsed schema", column)
	return schemamodel.Field{}
}

// TestToDatabase_CarriesTheNotNullConstraintName is the step the pipeline lost
// it at.
//
// The parser read the name into the AST node and the conversion to
// schemamodel.Field did not carry it, so every layer below saw an unnamed NOT
// NULL. Nothing failed: the AST assertion passed, the comparator compared two
// empty strings and agreed, and the planner emitted no rename for a name that
// had drifted. Only a run through the whole path showed it
// (stokaro/ptah#2161).
func TestToDatabase_CarriesTheNotNullConstraintName(t *testing.T) {
	c := qt.New(t)

	field := fieldNamed(c, `CREATE TABLE widget (a text CONSTRAINT c_keep NOT NULL);`, "a")

	c.Assert(field.NotNullConstraintName, qt.Equals, "c_keep")
	c.Assert(field.Nullable, qt.IsFalse)
}

// TestToDatabase_AnUnnamedNotNullCarriesNoName is the control: without it, a
// conversion that stamped every column would pass the test above.
func TestToDatabase_AnUnnamedNotNullCarriesNoName(t *testing.T) {
	c := qt.New(t)

	field := fieldNamed(c, `CREATE TABLE widget (a text NOT NULL);`, "a")

	c.Assert(field.NotNullConstraintName, qt.Equals, "")
	c.Assert(field.Nullable, qt.IsFalse)
}

// TestToDatabase_ANullableColumnCarriesNoName pins the guard the CheckName
// conversion already had: a name describes a constraint, and one on a nullable
// column names nothing.
func TestToDatabase_ANullableColumnCarriesNoName(t *testing.T) {
	c := qt.New(t)

	field := fieldNamed(c, `CREATE TABLE widget (a text);`, "a")

	c.Assert(field.NotNullConstraintName, qt.Equals, "")
	c.Assert(field.Nullable, qt.IsTrue)
}

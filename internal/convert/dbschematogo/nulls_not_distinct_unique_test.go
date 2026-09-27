package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_KeepsAColumnsUniqueThatTreatsNullsAsEqual describes a key the
// server named for its column as a constraint when it is NULLS NOT DISTINCT,
// because the column's `unique = true` cannot say so. Measured on PostgreSQL
// 18.6, `a int UNIQUE NULLS NOT DISTINCT` builds `<table>_a_key`. Described as
// the column's flag, the key compares equal to a plain UNIQUE, and a plan
// between the two changes nothing (stokaro/ptah#3821).
func TestConvert_KeepsAColumnsUniqueThatTreatsNullsAsEqual(t *testing.T) {
	c := qt.New(t)
	schema := uniqueSchema("customers_email_key")
	schema.Constraints[0].NullsDistinct = new(false)

	database := dbschematogo.ConvertDBSchemaToGoSchema(schema, "")

	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Name, qt.Equals, "customers_email_key")
	c.Assert(database.Constraints[0].NullsDistinct, qt.DeepEquals, new(false))
	c.Assert(emailField(c, database).Unique, qt.IsFalse)
}

// TestConvert_LeavesAColumnsUniqueThatTreatsNullsAsDistinctToTheColumn is the
// control: NULLS DISTINCT is what the flag means, so the key stays on it.
func TestConvert_LeavesAColumnsUniqueThatTreatsNullsAsDistinctToTheColumn(t *testing.T) {
	c := qt.New(t)
	schema := uniqueSchema("customers_email_key")
	schema.Constraints[0].NullsDistinct = new(true)

	database := dbschematogo.ConvertDBSchemaToGoSchema(schema, "")

	c.Assert(database.Constraints, qt.HasLen, 0)
	c.Assert(emailField(c, database).Unique, qt.IsTrue)
}

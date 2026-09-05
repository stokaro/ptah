package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// coveringUniqueSchema is one table carrying a unique index named i2, and
// whichever constraints the server reports for it.
//
// The index always carries the payload, because that is what pg_index reports
// on every server that has one. What varies between servers is the constraint
// row beside it, which is the whole question.
func coveringUniqueSchema(constraints []catalog.Constraint) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "a",
			Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "NO"},
				{Name: "name", DataType: "text", IsNullable: "YES"},
			},
		}},
		Indexes: []catalog.Index{{
			Name:           "i2",
			TableName:      "a",
			Columns:        []string{"email"},
			IncludeColumns: []string{"name"},
			IsUnique:       true,
		}},
		Constraints: constraints,
	}
}

// bareUniqueConstraint is the row CockroachDB reports beside the index: the same
// name, the same key columns, and no INCLUDE, because its pg_get_constraintdef
// does not print one.
func bareUniqueConstraint() catalog.Constraint {
	return catalog.Constraint{
		TableName:   "a",
		Name:        "i2",
		Type:        "UNIQUE",
		ColumnNames: []string{"email"},
	}
}

// TestConvert_CoveringUniqueIndexKeepsItsPayload covers stokaro/ptah#2589.
//
// CockroachDB reports the object twice and the two views disagree: pg_index
// carries the INCLUDE payload and pg_constraint does not. Describing it by the
// constraint drops the payload, and describing it by both produces one name and
// two objects. The index wins, and the constraint is not emitted.
func TestConvert_CoveringUniqueIndexKeepsItsPayload(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(
		coveringUniqueSchema([]catalog.Constraint{bareUniqueConstraint()}), "")

	c.Assert(converted.Indexes, qt.HasLen, 1)
	c.Assert(converted.Indexes[0].Name, qt.Equals, "i2")
	c.Assert(converted.Indexes[0].Unique, qt.IsTrue)
	c.Assert(converted.Indexes[0].IncludeColumns, qt.DeepEquals, []string{"name"})

	// One name, one object. Emitting the constraint as well is the failure this
	// fix is not allowed to trade the payload loss for.
	for _, constraint := range converted.Constraints {
		c.Assert(constraint.Name, qt.Not(qt.Equals), "i2")
	}
}

// TestConvert_UniqueConstraintStillOwnsAnIndexWithNothingToLose is the control
// that keeps the fix off the servers that never had the defect.
//
// PostgreSQL, MySQL and MariaDB report a unique constraint and its index as one
// object with no payload on either side. The constraint stays the description
// there, and the index stays suppressed, exactly as before.
func TestConvert_UniqueConstraintStillOwnsAnIndexWithNothingToLose(t *testing.T) {
	schema := coveringUniqueSchema([]catalog.Constraint{bareUniqueConstraint()})
	schema.Indexes[0].IncludeColumns = nil

	c := qt.New(t)
	converted := dbschematogo.ConvertDBSchemaToGoSchema(schema, "")

	c.Assert(converted.Indexes, qt.HasLen, 0)
	names := make([]string, 0, len(converted.Constraints))
	for _, constraint := range converted.Constraints {
		names = append(names, constraint.Name)
	}
	c.Assert(names, qt.Contains, "i2")
}

// TestConvert_ConstraintCarryingItsOwnPayloadKeepsTheObject is the second
// control, and the narrower one.
//
// A server whose constraint view DOES print the payload loses nothing by being
// the description, so the object must not move. Without this the rule would
// read as "a covering unique index always wins", which would hand the object to
// the index on every server that reports both views completely.
func TestConvert_ConstraintCarryingItsOwnPayloadKeepsTheObject(t *testing.T) {
	constraint := bareUniqueConstraint()
	constraint.IncludeColumns = []string{"name"}

	c := qt.New(t)
	converted := dbschematogo.ConvertDBSchemaToGoSchema(
		coveringUniqueSchema([]catalog.Constraint{constraint}), "")

	c.Assert(converted.Indexes, qt.HasLen, 0)
	names := make([]string, 0, len(converted.Constraints))
	for _, emitted := range converted.Constraints {
		names = append(names, emitted.Name)
	}
	c.Assert(names, qt.Contains, "i2")
}

// TestConvert_CoveringUniqueIndexWithNoConstraintRowIsUntouched is the
// PostgreSQL shape: no constraint row exists for a bare unique index, so there
// is nothing to decide and the index is emitted as it always was.
func TestConvert_CoveringUniqueIndexWithNoConstraintRowIsUntouched(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(coveringUniqueSchema(nil), "")

	c.Assert(converted.Indexes, qt.HasLen, 1)
	c.Assert(converted.Indexes[0].IncludeColumns, qt.DeepEquals, []string{"name"})
}

// TestConvert_ColumnUniqueIsClearedByTheOwningIndex covers the half a unit test
// did not reach and a live round trip did.
//
// The column's inline UNIQUE was cleared by finding a named unique CONSTRAINT
// over it. Once the covering index owns the object there is no such constraint,
// so the column kept `UNIQUE` and the replay grew a server-named
// `a_email_key` the source never had -- one object read back as two.
func TestConvert_ColumnUniqueIsClearedByTheOwningIndex(t *testing.T) {
	schema := coveringUniqueSchema([]catalog.Constraint{bareUniqueConstraint()})
	schema.Tables[0].Columns[1].IsUnique = true

	c := qt.New(t)
	converted := dbschematogo.ConvertDBSchemaToGoSchema(schema, "")

	c.Assert(converted.Indexes, qt.HasLen, 1)
	for _, field := range converted.Fields {
		c.Assert(field.Unique, qt.IsFalse,
			qt.Commentf("field %q keeps an inline UNIQUE beside the index that owns the object", field.Name))
	}
}

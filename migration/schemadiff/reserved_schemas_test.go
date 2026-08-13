package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfoRefusesADeclaredSystemSchema pins validation on
// the shared comparison path, including a schema-only desired state whose
// ordinary object diff would otherwise be empty.
func TestCompareWithDatabaseInfoRefusesADeclaredSystemSchema(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&goschema.Database{Schemas: []goschema.Schema{{Name: "pg_catalog"}}},
		&types.DBSchema{},
		types.DBInfo{Dialect: "postgres", Schema: "public"},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches,
		`.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfoKeepsAQuotedSystemSchemaLookalike(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&goschema.Database{Schemas: []goschema.Schema{{Name: "PG_CATALOG"}}},
		&types.DBSchema{},
		types.DBInfo{Dialect: "postgres", Schema: "public"},
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff, qt.IsNotNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

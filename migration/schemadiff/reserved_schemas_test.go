package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithDatabaseInfoRefusesADeclaredSystemSchema pins validation on
// the shared comparison path, including a schema-only desired state whose
// ordinary object diff would otherwise be empty.
func TestCompareWithDatabaseInfoRefusesADeclaredSystemSchema(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "pg_catalog"}}},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
		nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches,
		`.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfoKeepsAQuotedSystemSchemaLookalike(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "PG_CATALOG"}}},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
		nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff, qt.IsNotNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

func TestGetOrderedCreateStatementsRefusesDeclaredPostgresSystemSchemas(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{name: "catalog", schema: "pg_catalog"},
		{name: "reserved prefix", schema: "pg_application"},
		{name: "information schema", schema: "information_schema"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				&goschema.Database{Schemas: []goschema.Schema{{Name: test.schema}}},
				"postgres",
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "`+test.schema+`".*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatementsRefusesAnExplicitSystemSchemaBesideAnExtension(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		&goschema.Database{
			Schemas:    []goschema.Schema{{Name: "pg_catalog"}},
			Extensions: []goschema.Extension{{Name: "plpgsql", Schema: "pg_catalog"}},
		},
		"postgres",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatementsKeepsAQuotedSystemSchemaLookalike(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		&goschema.Database{Schemas: []goschema.Schema{{Name: "PG_CATALOG"}}},
		"postgres",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"CREATE SCHEMA IF NOT EXISTS \"PG_CATALOG\";\n"})
}

func TestGetOrderedCreateStatementsRefusesCockroachDBInternalSchema(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		&goschema.Database{Schemas: []goschema.Schema{{Name: "crdb_internal"}}},
		"cockroachdb",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "crdb_internal".*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatementsKeepsQuotedCockroachDBInternalLookalike(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		&goschema.Database{Schemas: []goschema.Schema{{Name: "CRDB_INTERNAL"}}},
		"cockroachdb",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"CREATE SCHEMA IF NOT EXISTS \"CRDB_INTERNAL\";\n"})
}

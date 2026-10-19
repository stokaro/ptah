package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgextension"
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
			statements, err := builtin.GetOrderedCreateStatements(
				&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: test.schema}}},
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

	statements, err := builtin.GetOrderedCreateStatements(
		&schemamodel.Database{
			Schemas:         []schemamodel.Schema{{Name: "pg_catalog"}},
			FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("plpgsql"), pgextension.DesiredExtension{Schema: "pg_catalog"})))),
			FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
		},
		"postgres",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatementsKeepsAQuotedSystemSchemaLookalike(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "PG_CATALOG"}}},
		"postgres",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"CREATE SCHEMA IF NOT EXISTS \"PG_CATALOG\";\n"})
}

func TestGetOrderedCreateStatementsRefusesCockroachDBInternalSchema(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "crdb_internal"}}},
		"cockroachdb",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "crdb_internal".*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatementsKeepsQuotedCockroachDBInternalLookalike(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "CRDB_INTERNAL"}}},
		"cockroachdb",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"CREATE SCHEMA IF NOT EXISTS \"CRDB_INTERNAL\";\n"})
}

// TestGetOrderedCreateStatementsRefusesSpannerSysSchema pins Spanner's own
// catalog schema: measured on the PGAdapter emulator v0.56.1, the server
// refuses `CREATE SCHEMA spanner_sys` and refuses a table placed in it, so the
// document is refused before anything is applied.
func TestGetOrderedCreateStatementsRefusesSpannerSysSchema(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "spanner_sys"}}},
		"spanner",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "spanner_sys".*`)
	c.Assert(statements, qt.IsNil)
}

// TestGetOrderedCreateStatementsCreatesOnlyTheSchemasATargetCreates pins the
// render against the plan for an object placed in a schema the document does
// not declare: the render creates the schema where the planner's precondition
// would, and leaves a schema the target owns to the server. crdb_internal is
// CockroachDB's own and a user's schema on PostgreSQL, measured on CockroachDB
// 26.3.2 and PostgreSQL 18.
func TestGetOrderedCreateStatementsCreatesOnlyTheSchemasATargetCreates(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantSchema bool
	}{
		{name: "a user schema on PostgreSQL", target: platform.Postgres, wantSchema: true},
		{name: "CockroachDB's own schema", target: platform.CockroachDB, wantSchema: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(defaultPrivilegeDeclaration(
				"app_owner", "crdb_internal", "TABLES", "app_reader", []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, nil,
			)))}

			statements, err := builtin.GetOrderedCreateStatements(database, test.target)

			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, `IN SCHEMA "crdb_internal" GRANT SELECT ON TABLES TO "app_reader"`)
			c.Assert(strings.Contains(rendered, `CREATE SCHEMA IF NOT EXISTS "crdb_internal"`), qt.Equals, test.wantSchema,
				qt.Commentf("%s", rendered))
		})
	}
}

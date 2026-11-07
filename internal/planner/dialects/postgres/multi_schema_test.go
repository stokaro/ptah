package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

func TestPlanner_GenerateMigrationAST_MultiSchemaTablesAndFKs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users", Schema: "auth"},
			{StructName: "Invoice", Name: "invoices", Schema: "billing"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "auth.users(id)"},
		},
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("postgres"), "auth.users", "billing.invoices"),
	}

	nodes, err := postgres.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired),
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS auth;")
	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS billing;")
	c.Assert(sql, qt.Contains, "CREATE TABLE auth.users")
	c.Assert(sql, qt.Contains, "CREATE TABLE billing.invoices")
	c.Assert(sql, qt.Contains, "ALTER TABLE billing.invoices ADD CONSTRAINT fk_invoices_user_id FOREIGN KEY (user_id) REFERENCES auth.users(id);")
}

func TestPlanner_GenerateMigrationAST_TrimsSchemaPreconditions(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users", Schema: " auth "},
			{StructName: "Account", Name: "accounts", Schema: "auth"},
			{StructName: "Blank", Name: "blank", Schema: "   "},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Account", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Blank", Name: "id", Type: "SERIAL", Primary: true},
		},
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("postgres"), "auth.users", "auth.accounts", "blank"),
	}

	nodes, err := postgres.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired),
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS auth;")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SCHEMA IF NOT EXISTS  auth ;")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SCHEMA IF NOT EXISTS    ;")
	c.Assert(countSQLLine(sql, "CREATE SCHEMA IF NOT EXISTS auth;"), qt.Equals, 1)
}

func TestPlanner_GenerateMigrationAST_DoesNotQualifyAmbiguousLeafFK(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "AuthUser", Name: "users", Schema: "auth"},
			{StructName: "CrmUser", Name: "users", Schema: "crm"},
			{StructName: "Invoice", Name: "invoices", Schema: "billing"},
		},
		Fields: []schemamodel.Field{
			{StructName: "AuthUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "CrmUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("postgres"), "auth.users", "crm.users", "billing.invoices"),
	}

	nodes, err := postgres.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired),
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "ALTER TABLE billing.invoices ADD CONSTRAINT fk_invoices_user_id FOREIGN KEY (user_id) REFERENCES users(id);")
	c.Assert(sql, qt.Not(qt.Contains), "REFERENCES auth.users(id)")
	c.Assert(sql, qt.Not(qt.Contains), "REFERENCES crm.users(id)")
}

func countSQLLine(sql, line string) int {
	count := 0
	for sqlLine := range strings.SplitSeq(sql, "\n") {
		if sqlLine == line {
			count++
		}
	}
	return count
}

// A name qualified with the schema the connection resolved needs no
// precondition, as an unqualified name needs none: the connector refuses a
// search_path that resolves to nothing, so the schema exists. An Atlas HCL
// document has to qualify every table with `schema.public`, and the
// precondition it planned against every PostgreSQL database is refused to a
// role that may create tables in `public` but not schemas in the database.
//
// The rows that keep the statement are the half that makes this a decision
// rather than a blanket drop of `public`: another schema on the same
// connection, `public` on a connection resolved to another schema, and a
// comparison that carries no connection, whose default schema nothing
// resolved.
func TestPlanner_SchemaPreconditionsSkipTheConnectedSchema(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		semantics *identifier.Semantics
		schema    string
		wantsPre  bool
	}{
		{name: "PostgreSQL connected to public", dialect: "postgres", semantics: connectedTo("postgres", "public"), schema: "public"},
		{name: "PostgreSQL connected to app", dialect: "postgres", semantics: connectedTo("postgres", "app"), schema: "app"},
		{name: "CockroachDB connected to public", dialect: "cockroachdb", semantics: connectedTo("cockroachdb", "public"), schema: "public"},
		{name: "YugabyteDB connected to public", dialect: "yugabytedb", semantics: connectedTo("yugabytedb", "public"), schema: "public"},
		{name: "another schema on a connection to public", dialect: "postgres", semantics: connectedTo("postgres", "public"), schema: "app", wantsPre: true},
		{name: "public on a connection to app", dialect: "postgres", semantics: connectedTo("postgres", "app"), schema: "public", wantsPre: true},
		{name: "public without a connection", dialect: "postgres", schema: "public", wantsPre: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: test.schema}},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
				},
				SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
			}
			diff := &difftypes.SchemaDiff{
				TablesAdded:         difftypes.TableCreationsFor(desired, identifier.ForDialect(test.dialect), test.schema+".users"),
				IdentifierSemantics: test.semantics,
			}

			nodes, err := postgres.NewForDialect(test.dialect, capability.Capabilities{}).
				GenerateMigrationAST(
					context.Background(), must.Must(builtin.New()),
					withDeclaredObjects(diff, desired),
				)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL(test.dialect, nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(strings.Contains(sql, "CREATE SCHEMA"), qt.Equals, test.wantsPre, qt.Commentf("%s", sql))
			c.Assert(sql, qt.Contains, `CREATE TABLE "`+test.schema+`"."users"`)
		})
	}
}

// connectedTo is the identifier semantics a connection to dialect reports
// when its search_path resolves to schema.
func connectedTo(dialect, schema string) *identifier.Semantics {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = schema
	return &semantics
}

// A schema the target owns is reached without a precondition: a server-owned
// schema such as PostgreSQL's pg_catalog or CockroachDB's crdb_internal cannot
// be created, so asking for it first would make the migration fail on its first
// statement.
func TestPlanner_SchemaPreconditionsSkipTheNamesATargetOwns(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		schema   string
		wantsPre bool
	}{
		{name: "PostgreSQL does not create pg_catalog", dialect: "postgres", schema: "pg_catalog", wantsPre: false},
		{name: "CockroachDB does not create crdb_internal", dialect: "cockroachdb", schema: "crdb_internal", wantsPre: false},
		{name: "CockroachDB creates a user schema", dialect: "cockroachdb", schema: "app", wantsPre: true},
		{name: "PostgreSQL creates public", dialect: "postgres", schema: "public", wantsPre: true},
		{name: "PostgreSQL creates a user schema", dialect: "postgres", schema: "app", wantsPre: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: test.schema}},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
				},
				SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
			}
			diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: test.schema + ".users"}}}

			nodes, err := postgres.NewForDialect(test.dialect, capability.Capabilities{}).
				GenerateMigrationAST(
					context.Background(), must.Must(builtin.New()),
					withDeclaredObjects(diff, desired),
				)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL(test.dialect, nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(strings.Contains(sql, "CREATE SCHEMA"), qt.Equals, test.wantsPre,
				qt.Commentf("%s", sql))
		})
	}
}

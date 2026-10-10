//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestPostgreSQLMultiSchemaGenerateApplyReadDiffIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupMultiSchemaIntegration(t, db)
	defer cleanupMultiSchemaIntegration(t, db)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Account", Name: "ptah_ms_accounts"},
			{StructName: "User", Name: "ptah_ms_users", Schema: "ptah_ms_auth"},
			{StructName: "Invoice", Name: "ptah_ms_invoices", Schema: "ptah_ms_billing"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "ptah_ms_auth.ptah_ms_users(id)"},
			{StructName: "Invoice", Name: "account_id", Type: "INTEGER", Foreign: "ptah_ms_accounts(id)"},
		},
		// Row-level security is the PostgreSQL row-security owner's.
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("ptah_ms_auth", "ptah_ms_users", "ptah_ms_users_visible"),
			pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Using: new("id IS NOT NULL")})))),
		FeatureCoverage:            must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}
	desired.Tables[1].Facets = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))

	// Planned from a comparison with nothing, so the owner answers for the
	// tables the plan creates.
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "postgres", must.Must(builtin.New())))
	nodes, err := planner.GenerateSchemaDiffAST(
		context.Background(), must.Must(builtin.New()),
		diff, "postgres",
	)
	c.Assert(err, qt.IsNil)
	migrationSQL, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	migrationSQLForAssert := legacyRenderedSQL(migrationSQL)
	c.Assert(migrationSQLForAssert, qt.Contains, "CREATE SCHEMA IF NOT EXISTS ptah_ms_auth;")
	c.Assert(migrationSQLForAssert, qt.Contains, "CREATE SCHEMA IF NOT EXISTS ptah_ms_billing;")
	c.Assert(migrationSQLForAssert, qt.Contains, "REFERENCES ptah_ms_auth.ptah_ms_users(id);")

	for _, stmt := range sqlutil.SplitStatements(migrationSQL) {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{"ptah_ms_auth", "ptah_ms_billing", "public"})
	c.Assert(err, qt.IsNil)
	live = filterMultiSchemaIntegrationTables(live)

	roundTripDiff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, live, "postgres", must.Must(builtin.New())))
	c.Assert(roundTripDiff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", roundTripDiff))
}

func cleanupMultiSchemaIntegration(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP SCHEMA IF EXISTS ptah_ms_billing CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS ptah_ms_auth CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_ms_accounts CASCADE")
}

func filterMultiSchemaIntegrationTables(in *catalog.Database) *catalog.Database {
	keepTables := map[string]struct{}{
		"ptah_ms_accounts":                 {},
		"ptah_ms_auth.ptah_ms_users":       {},
		"ptah_ms_billing.ptah_ms_invoices": {},
	}
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	return &out
}

func filterTables(in []catalog.Table, keep map[string]struct{}) []catalog.Table {
	out := make([]catalog.Table, 0, len(in))
	for _, table := range in {
		if _, ok := keep[table.QualifiedName()]; ok {
			out = append(out, table)
		}
	}
	return out
}

func filterIndexes(in []catalog.Index, keep map[string]struct{}) []catalog.Index {
	out := make([]catalog.Index, 0, len(in))
	for _, index := range in {
		if _, ok := keep[index.QualifiedTableName()]; ok {
			out = append(out, index)
		}
	}
	return out
}

func filterConstraints(in []catalog.Constraint, keep map[string]struct{}) []catalog.Constraint {
	out := make([]catalog.Constraint, 0, len(in))
	for _, constraint := range in {
		if _, ok := keep[constraint.QualifiedTableName()]; ok {
			out = append(out, constraint)
		}
	}
	return out
}

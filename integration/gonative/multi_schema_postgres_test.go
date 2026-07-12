//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPostgreSQLMultiSchemaGenerateApplyReadDiffIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupMultiSchemaIntegration(t, db)
	defer cleanupMultiSchemaIntegration(t, db)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Account", Name: "ptah_ms_accounts"},
			{StructName: "User", Name: "ptah_ms_users", Schema: "ptah_ms_auth"},
			{StructName: "Invoice", Name: "ptah_ms_invoices", Schema: "ptah_ms_billing"},
		},
		Fields: []goschema.Field{
			{StructName: "Account", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "ptah_ms_auth.ptah_ms_users(id)"},
			{StructName: "Invoice", Name: "account_id", Type: "INTEGER", Foreign: "ptah_ms_accounts(id)"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "ptah_ms_users_visible", Table: "ptah_ms_auth.ptah_ms_users", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "id IS NOT NULL"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{},
	}

	diff := &difftypes.SchemaDiff{
		TablesAdded:      []string{"ptah_ms_accounts", "ptah_ms_auth.ptah_ms_users", "ptah_ms_billing.ptah_ms_invoices"},
		RLSPoliciesAdded: []string{"ptah_ms_users_visible"},
	}
	nodes := planner.GenerateSchemaDiffAST(diff, generated, "postgres")
	migrationSQL, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationSQL, qt.Contains, "CREATE SCHEMA IF NOT EXISTS ptah_ms_auth;")
	c.Assert(migrationSQL, qt.Contains, "CREATE SCHEMA IF NOT EXISTS ptah_ms_billing;")
	c.Assert(migrationSQL, qt.Contains, "REFERENCES ptah_ms_auth.ptah_ms_users(id);")

	for _, stmt := range migrator.SplitSQLStatements(migrationSQL) {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"ptah_ms_auth", "ptah_ms_billing", "public"})
	c.Assert(err, qt.IsNil)
	live = filterMultiSchemaIntegrationTables(live)

	roundTripDiff := schemadiff.CompareWithDialect(generated, live, "postgres")
	c.Assert(roundTripDiff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", roundTripDiff))
}

func cleanupMultiSchemaIntegration(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP SCHEMA IF EXISTS ptah_ms_billing CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS ptah_ms_auth CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_ms_accounts CASCADE")
}

func filterMultiSchemaIntegrationTables(in *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	keepTables := map[string]struct{}{
		"ptah_ms_accounts":                 {},
		"ptah_ms_auth.ptah_ms_users":       {},
		"ptah_ms_billing.ptah_ms_invoices": {},
	}
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	out.RLSPolicies = filterRLSPolicies(in.RLSPolicies, keepTables)
	return &out
}

func filterTables(in []dbschematypes.DBTable, keep map[string]struct{}) []dbschematypes.DBTable {
	out := make([]dbschematypes.DBTable, 0, len(in))
	for _, table := range in {
		if _, ok := keep[table.QualifiedName()]; ok {
			out = append(out, table)
		}
	}
	return out
}

func filterIndexes(in []dbschematypes.DBIndex, keep map[string]struct{}) []dbschematypes.DBIndex {
	out := make([]dbschematypes.DBIndex, 0, len(in))
	for _, index := range in {
		if _, ok := keep[index.QualifiedTableName()]; ok {
			out = append(out, index)
		}
	}
	return out
}

func filterConstraints(in []dbschematypes.DBConstraint, keep map[string]struct{}) []dbschematypes.DBConstraint {
	out := make([]dbschematypes.DBConstraint, 0, len(in))
	for _, constraint := range in {
		if _, ok := keep[constraint.QualifiedTableName()]; ok {
			out = append(out, constraint)
		}
	}
	return out
}

func filterRLSPolicies(in []dbschematypes.DBRLSPolicy, keep map[string]struct{}) []dbschematypes.DBRLSPolicy {
	out := make([]dbschematypes.DBRLSPolicy, 0, len(in))
	for _, policy := range in {
		if _, ok := keep[policy.Table]; ok {
			out = append(out, policy)
		}
	}
	return out
}

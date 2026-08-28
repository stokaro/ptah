//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPostgreSQLMultiSchemaGenerateApplyReadDiffIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupMultiSchemaIntegration(t, db)
	defer cleanupMultiSchemaIntegration(t, db)

	policy := schemamodel.RLSPolicy{
		Name:            "ptah_ms_users_visible",
		Table:           "ptah_ms_auth.ptah_ms_users",
		PolicyFor:       "ALL",
		ToRoles:         "PUBLIC",
		UsingExpression: "id IS NOT NULL",
	}
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
		RLSPolicies: []schemamodel.RLSPolicy{policy},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{
			{Table: "ptah_ms_auth.ptah_ms_users"},
		},
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}

	diff := &difftypes.SchemaDiff{
		TablesAdded: []string{"ptah_ms_accounts", "ptah_ms_auth.ptah_ms_users", "ptah_ms_billing.ptah_ms_invoices"},
		RLSPoliciesAdded: []difftypes.RLSPolicyRef{
			{PolicyName: policy.Name, TableName: policy.Table, Desired: policy},
		},
		RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "ptah_ms_auth.ptah_ms_users"}},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, desired, "postgres")
	c.Assert(err, qt.IsNil)
	migrationSQL, err := renderer.RenderSQL("postgres", nodes...)
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

	roundTripDiff := schemadiff.CompareWithDialect(desired, live, "postgres")
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
	out.RLSPolicies = filterRLSPolicies(in.RLSPolicies, keepTables)
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

func filterRLSPolicies(in []catalog.RLSPolicy, keep map[string]struct{}) []catalog.RLSPolicy {
	out := make([]catalog.RLSPolicy, 0, len(in))
	for _, policy := range in {
		if _, ok := keep[policy.Table]; ok {
			out = append(out, policy)
		}
	}
	return out
}

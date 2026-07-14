//go:build integration

package gonative_test

import (
	"database/sql"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema/postgres"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestPostgreSQLRolesGrantsRoundTripAndBehaviorIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupRolesGrantsIntegration(t, db)
	defer cleanupRolesGrantsIntegration(t, db)

	target := rolesGrantsTarget()
	diff := schemadiff.Compare(target, &dbschematypes.DBSchema{})
	c.Assert(diff.HasChanges(), qt.IsTrue)

	nodes := planner.GenerateSchemaDiffAST(diff, target, "postgres")
	migrationSQL, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	for _, stmt := range migrator.SplitSQLStatements(migrationSQL) {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	reader := postgres.NewPostgreSQLReader(db, "public")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	filtered := filterRolesGrantsIntegrationSchema(live)

	roundTrip := schemadiff.Compare(target, filtered)
	c.Assert(roundTrip.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", roundTrip))

	_, err = db.Exec("GRANT ptah_grants_reader TO CURRENT_USER")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("GRANT ptah_grants_writer TO CURRENT_USER")
	c.Assert(err, qt.IsNil)

	_, err = db.Exec(`
		INSERT INTO ptah_grants_users (id, tenant_id, email)
		VALUES (1, 1, 'reader-visible@example.test'), (2, 2, 'reader-hidden@example.test')
	`)
	c.Assert(err, qt.IsNil)

	assertReaderRoleBehavior(t, db)
	assertWriterRoleBehavior(t, db)
}

func rolesGrantsTarget() *goschema.Database {
	target := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "RolesGrantUser", Name: "ptah_grants_users"},
			{StructName: "RolesGrantAuditLog", Name: "ptah_grants_audit_log"},
		},
		Fields: []goschema.Field{
			{StructName: "RolesGrantUser", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "RolesGrantUser", Name: "tenant_id", Type: "INTEGER", Nullable: false},
			{StructName: "RolesGrantUser", Name: "email", Type: "TEXT", Nullable: false},
			{StructName: "RolesGrantAuditLog", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "RolesGrantAuditLog", Name: "message", Type: "TEXT", Nullable: false},
		},
		Roles: []goschema.Role{
			{Name: "ptah_grants_reader", Inherit: true},
			{Name: "ptah_grants_writer", Inherit: true},
		},
		Grants: []goschema.Grant{
			{Role: "ptah_grants_reader", Privileges: []string{"USAGE"}, OnSchema: "public"},
			{Role: "ptah_grants_writer", Privileges: []string{"USAGE"}, OnSchema: "public"},
			{Role: "ptah_grants_reader", Privileges: []string{"SELECT"}, OnTable: "ptah_grants_users"},
			{Role: "ptah_grants_writer", Privileges: []string{"SELECT", "INSERT", "UPDATE", "DELETE"}, OnTable: "ptah_grants_users"},
			{Role: "ptah_grants_writer", Privileges: []string{"INSERT"}, OnTable: "ptah_grants_audit_log"},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "ptah_grants_users"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{
				Name:                "ptah_grants_tenant_isolation",
				Table:               "ptah_grants_users",
				PolicyFor:           "ALL",
				ToRoles:             "ptah_grants_reader,ptah_grants_writer",
				UsingExpression:     "(tenant_id = (current_setting('app.tenant_id'::text))::integer)",
				WithCheckExpression: "(tenant_id = (current_setting('app.tenant_id'::text))::integer)",
			},
		},
	}
	goschema.Finalize(target)
	return target
}

func assertReaderRoleBehavior(t *testing.T, db *sql.DB) {
	t.Helper()
	c := qt.New(t)

	tx, err := db.Begin()
	c.Assert(err, qt.IsNil)
	defer tx.Rollback()

	_, err = tx.Exec("SET LOCAL ROLE ptah_grants_reader")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("SET LOCAL app.tenant_id = '1'")
	c.Assert(err, qt.IsNil)

	var count int
	err = tx.QueryRow("SELECT count(*) FROM ptah_grants_users").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (3, 1, 'reader-write@example.test')")
	c.Assert(err, qt.Not(qt.IsNil))
}

func assertWriterRoleBehavior(t *testing.T, db *sql.DB) {
	t.Helper()
	c := qt.New(t)

	tx, err := db.Begin()
	c.Assert(err, qt.IsNil)
	defer tx.Rollback()

	_, err = tx.Exec("SET LOCAL ROLE ptah_grants_writer")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("SET LOCAL app.tenant_id = '1'")
	c.Assert(err, qt.IsNil)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (4, 1, 'writer-ok@example.test')")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("INSERT INTO ptah_grants_audit_log (id, message) VALUES (1, 'writer inserted a user')")
	c.Assert(err, qt.IsNil)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (5, 2, 'writer-rls-blocked@example.test')")
	c.Assert(err, qt.Not(qt.IsNil))
}

func cleanupRolesGrantsIntegration(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_grants_audit_log CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_grants_users CASCADE")
	for _, roleName := range []string{"ptah_grants_reader", "ptah_grants_writer"} {
		_, _ = db.Exec("REVOKE ALL PRIVILEGES ON SCHEMA public FROM " + roleName)
		_, _ = db.Exec("DROP OWNED BY " + roleName)
		_, _ = db.Exec("DROP ROLE IF EXISTS " + roleName)
	}
}

func filterRolesGrantsIntegrationSchema(in *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	keepTables := map[string]struct{}{
		"ptah_grants_users":     {},
		"ptah_grants_audit_log": {},
	}
	keepRoles := map[string]struct{}{
		"ptah_grants_reader": {},
		"ptah_grants_writer": {},
	}

	out := &dbschematypes.DBSchema{
		Tables:      filterTables(in.Tables, keepTables),
		Indexes:     filterIndexes(in.Indexes, keepTables),
		Constraints: filterConstraints(in.Constraints, keepTables),
		RLSPolicies: filterRLSPolicies(in.RLSPolicies, keepTables),
		Roles:       filterRoles(in.Roles, keepRoles),
		Grants:      filterGrants(in.Grants, keepRoles),
	}
	return out
}

func filterRoles(in []dbschematypes.DBRole, keep map[string]struct{}) []dbschematypes.DBRole {
	out := make([]dbschematypes.DBRole, 0, len(in))
	for _, role := range in {
		if _, ok := keep[role.Name]; ok {
			out = append(out, role)
		}
	}
	return out
}

func filterGrants(in []dbschematypes.DBGrant, keepRoles map[string]struct{}) []dbschematypes.DBGrant {
	out := make([]dbschematypes.DBGrant, 0, len(in))
	for _, grant := range in {
		if _, ok := keepRoles[grant.Role]; !ok {
			continue
		}
		if strings.HasPrefix(grant.ObjectName, "ptah_grants_") || grant.ObjectName == "public" {
			out = append(out, grant)
		}
	}
	return out
}

// TestGoFixtures_ParseDirForSchemaObjects exercises ParseDir on the Go annotation
// fixtures added for #279 (views, grants, constraints, triggers, matviews).
// This drives the real ParseDir path used by CLI in an integration-tagged test file.
// Does not require live DB.
func TestGoFixtures_ParseDirForSchemaObjects(t *testing.T) {
	c := qt.New(t)

	// Compute root and abs fixture from this source file's location (robust to test cwd)
	_, filename, _, _ := runtime.Caller(0)
	srcDir := filepath.Dir(filename)        // .../integration/gonative
	integrationDir := filepath.Dir(srcDir)  // .../integration
	rootDir := filepath.Dir(integrationDir) // module root
	absFixture := filepath.Join(rootDir, "integration/fixtures/entities/023-go-annotations-objects")
	result, err := goschema.ParseDir(absFixture)
	c.Assert(err, qt.IsNil, qt.Commentf("ParseDir on new objects fixture must succeed"))

	c.Assert(result.Views, qt.HasLen, 1)
	c.Assert(result.Views[0].Name, qt.Equals, "active_users")

	c.Assert(result.MaterializedViews, qt.HasLen, 1)
	c.Assert(result.MaterializedViews[0].Name, qt.Equals, "user_stats")

	c.Assert(result.Triggers, qt.HasLen, 1)
	c.Assert(result.Triggers[0].Name, qt.Equals, "users_set_updated_at")

	c.Assert(result.Grants, qt.HasLen, 3)
	c.Assert(result.Constraints, qt.HasLen, 1)
	c.Assert(result.Constraints[0].Name, qt.Equals, "users_email_check")

	c.Assert(result.Roles, qt.HasLen, 1)
	c.Assert(result.Roles[0].Name, qt.Equals, "fixture_app_user")

	// Exercise CLI generate entry point against the fixture (drives real ParseDir path in cmd/generate, per AC3)
	goMain := filepath.Join(rootDir, "cmd/main.go")
	genCmd := exec.Command("go", "run", goMain, "generate", "--root-dir", absFixture, "--dialect", "postgres")
	genOut, err := genCmd.CombinedOutput()
	c.Assert(err, qt.IsNil)
	outStr := string(genOut)
	c.Assert(outStr, qt.Contains, "CREATE VIEW active_users")
	c.Assert(outStr, qt.Contains, "CREATE MATERIALIZED VIEW user_stats")
	c.Assert(outStr, qt.Contains, "CREATE TRIGGER")
	c.Assert(outStr, qt.Contains, "GRANT ")
	c.Assert(outStr, qt.Contains, "WITH GRANT OPTION")
	c.Assert(outStr, qt.Contains, "CREATE ROLE fixture_app_user")
	c.Assert(outStr, qt.Contains, "CONSTRAINT users_email_check")
}

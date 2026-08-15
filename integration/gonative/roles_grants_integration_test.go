//go:build integration

package gonative_test

import (
	"bytes"
	"database/sql"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/readdb"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dbschema/postgres"
	"go.5x5.cz/ptah/internal/rolescope"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestPostgreSQLRolesGrantsRoundTripAndBehaviorIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	cleanupRolesGrantsIntegration(c, db)
	c.Cleanup(func() { cleanupRolesGrantsIntegration(c, db) })

	target := rolesGrantsTarget()
	diff := schemadiff.Compare(target, &dbschematypes.DBSchema{})
	c.Assert(diff.HasChanges(), qt.IsTrue)

	nodes, err := planner.GenerateSchemaDiffAST(diff, target, "postgres")
	c.Assert(err, qt.IsNil)
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

func TestPostgreSQLDBReadRoleCommentReplayIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupDBReadRoleIntegration(c, db)
	c.Cleanup(func() { cleanupDBReadRoleIntegration(c, db) })

	// The described role holds a privilege on the schema being read, which is
	// why the read describes it, and the GRANT that says so is emitted beside
	// the role -- a description names no role it does not also create.
	//
	// The stranger role exists in the same cluster and is used by nothing in
	// the schema being read. PostgreSQL roles are cluster-wide, so before
	// stokaro/ptah#1267 it was described here too, and reading one schema
	// disclosed every role on the server. It must not appear.
	_, err = db.Exec(`
CREATE ROLE ptah_db_read_role_137;
COMMENT ON ROLE ptah_db_read_role_137 IS 'Database read replay role';
CREATE ROLE ptah_db_read_stranger_137;
CREATE SCHEMA ptah_db_read_empty_schema_137;
GRANT USAGE ON SCHEMA ptah_db_read_empty_schema_137 TO ptah_db_read_role_137;`)
	c.Assert(err, qt.IsNil)

	cmd := readdb.NewReadDBCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetContext(t.Context())
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dsn,
		"--schemas", "ptah_db_read_empty_schema_137",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, `CREATE ROLE "ptah_db_read_role_137"`)
	c.Assert(stdout.String(), qt.Contains, `COMMENT ON ROLE "ptah_db_read_role_137" IS 'Database read replay role';`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "ptah_db_read_stranger_137")
	c.Assert(stderr.String(), qt.Contains, "Connected to postgres database successfully!")
	roleStatements := slices.DeleteFunc(
		migrator.SplitSQLStatements(stdout.String()),
		func(statement string) bool {
			return !strings.Contains(statement, "ptah_db_read_role_137")
		},
	)
	// CREATE ROLE, COMMENT ON ROLE, and the GRANT that is the role's reason
	// for being described at all.
	c.Assert(roleStatements, qt.HasLen, 3)

	// Drop the roles but keep the schema: the grant among the replayed
	// statements has to have somewhere to land.
	cleanupDBReadRolesOnly(c, db)
	for _, statement := range roleStatements {
		_, replayErr := db.Exec(statement)
		c.Assert(replayErr, qt.IsNil, qt.Commentf("role restore failed: %s", statement))
	}
	_, collisionErr := db.Exec(roleStatements[0])
	c.Assert(collisionErr, qt.ErrorMatches, `.*role "ptah_db_read_role_137" already exists.*`)

	var comment string
	err = db.QueryRow(`
SELECT shobj_description(oid, 'pg_authid')
FROM pg_roles
WHERE rolname = 'ptah_db_read_role_137'`).Scan(&comment)
	c.Assert(err, qt.IsNil)
	c.Assert(comment, qt.Equals, "Database read replay role")
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
			{Name: "ptah_grants_reader", Inherit: true, Comment: "Read tenant data"},
			{Name: "ptah_grants_writer", Inherit: true, Comment: "Write tenant data"},
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
	defer func() { c.Check(tx.Rollback(), qt.IsNil) }()

	_, err = tx.Exec("SET LOCAL ROLE ptah_grants_reader")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("SET LOCAL app.tenant_id = '1'")
	c.Assert(err, qt.IsNil)

	var count int
	err = tx.QueryRow("SELECT count(*) FROM ptah_grants_users").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (3, 1, 'reader-write@example.test')")
	c.Assert(err, qt.IsNotNil)
}

func assertWriterRoleBehavior(t *testing.T, db *sql.DB) {
	t.Helper()
	c := qt.New(t)

	tx, err := db.Begin()
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(tx.Rollback(), qt.IsNil) }()

	_, err = tx.Exec("SET LOCAL ROLE ptah_grants_writer")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("SET LOCAL app.tenant_id = '1'")
	c.Assert(err, qt.IsNil)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (4, 1, 'writer-ok@example.test')")
	c.Assert(err, qt.IsNil)
	_, err = tx.Exec("INSERT INTO ptah_grants_audit_log (id, message) VALUES (1, 'writer inserted a user')")
	c.Assert(err, qt.IsNil)

	_, err = tx.Exec("INSERT INTO ptah_grants_users (id, tenant_id, email) VALUES (5, 2, 'writer-rls-blocked@example.test')")
	c.Assert(err, qt.IsNotNil)
}

func cleanupRolesGrantsIntegration(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec("DROP TABLE IF EXISTS ptah_grants_audit_log CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec("DROP TABLE IF EXISTS ptah_grants_users CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec(`
DO $ptah_cleanup_roles$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ptah_grants_reader', 'ptah_grants_writer'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE %I FROM %I', role_name, current_user);
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$ptah_cleanup_roles$;`)
	c.Check(err, qt.IsNil)
}

func cleanupDBReadRoleIntegration(c *qt.C, db *sql.DB) {
	c.Helper()
	cleanupDBReadRolesOnly(c, db)
	_, err := db.Exec("DROP SCHEMA IF EXISTS ptah_db_read_empty_schema_137 CASCADE")
	c.Check(err, qt.IsNil)
}

func cleanupDBReadRolesOnly(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(`
DO $ptah_cleanup_role$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ptah_db_read_role_137', 'ptah_db_read_stranger_137'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE %I FROM %I', role_name, current_user);
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$ptah_cleanup_role$;`)
	c.Check(err, qt.IsNil)
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

	c.Assert(result.Grants, qt.HasLen, 4)
	c.Assert(result.Constraints, qt.HasLen, 1)
	c.Assert(result.Constraints[0].Name, qt.Equals, "users_email_check")

	c.Assert(result.Sequences, qt.HasLen, 1)
	c.Assert(result.Sequences[0].Name, qt.Equals, "fixture_order_seq")

	c.Assert(result.Roles, qt.HasLen, 1)
	c.Assert(result.Roles[0].Name, qt.Equals, "fixture_app_user")

	// Exercise CLI schema render entry point against the fixture (drives real ParseDir path in cmd/generate, per AC3)
	goMain := filepath.Join(rootDir, "cmd/main.go")
	genCmd := exec.Command("go", "run", goMain, "schema", "render", "--root-dir", absFixture, "--dialect", "postgres")
	genOut, err := genCmd.CombinedOutput()
	c.Assert(err, qt.IsNil)
	outStr := legacyRenderedSQL(string(genOut))
	c.Assert(outStr, qt.Contains, "CREATE VIEW active_users")
	c.Assert(outStr, qt.Contains, "CREATE MATERIALIZED VIEW user_stats")
	c.Assert(outStr, qt.Contains, "CREATE TRIGGER")
	c.Assert(outStr, qt.Contains, "CREATE SEQUENCE fixture_order_seq")
	c.Assert(outStr, qt.Contains, "ON SEQUENCE")
	c.Assert(outStr, qt.Contains, "GRANT ")
	c.Assert(outStr, qt.Contains, "WITH GRANT OPTION")
	c.Assert(outStr, qt.Contains, "CREATE ROLE fixture_app_user")
	c.Assert(outStr, qt.Contains, "CONSTRAINT users_email_check")
}

// TestPostgreSQLDescribedRolesCoverEveryRoleTheDescriptionNamesIntegration pins
// the invariant that scoping roles to the inspected schemas
// (stokaro/ptah#1267) has to preserve: a description defines every role it
// refers to. Break it and the document stops being readable -- the pinned
// Atlas community binary refuses the whole file with
// `Unsupported attribute; This object does not have an attribute named "..."`.
//
// The trap is information_schema.role_table_grants, which reports an owner's
// built-in privileges as grants even for a table whose pg_class.relacl is
// null, meaning nobody has ever run GRANT on it. Those are not privileges
// anyone granted, and a role reaching a description only through them is a
// role no GRANT statement put there.
func TestPostgreSQLDescribedRolesCoverEveryRoleTheDescriptionNamesIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupDescribedRolesIntegration(c, db)
	c.Cleanup(func() { cleanupDescribedRolesIntegration(c, db) })

	_, err = db.Exec(`
CREATE ROLE ptah_ref_granted_137;
CREATE ROLE ptah_ref_owner_137;
GRANT ptah_ref_owner_137 TO CURRENT_USER;
CREATE TABLE ptah_ref_granted_tbl_137 (id integer PRIMARY KEY);
GRANT SELECT ON ptah_ref_granted_tbl_137 TO ptah_ref_granted_137;
CREATE TABLE ptah_ref_untouched_137 (id integer PRIMARY KEY);
ALTER TABLE ptah_ref_untouched_137 OWNER TO ptah_ref_owner_137;`)
	c.Assert(err, qt.IsNil)

	reader := postgres.NewPostgreSQLReader(db, "public")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	described := make([]string, 0, len(live.Roles))
	for _, role := range live.Roles {
		described = append(described, role.Name)
	}

	for _, named := range rolesNamedByDescription(live) {
		c.Assert(described, qt.Contains, named,
			qt.Commentf("the description names role %q but does not define it", named))
	}

	// The positive half, so the invariant cannot be satisfied by describing
	// every role on the server: a role reachable only as the owner of a table
	// nobody has granted anything on is named nowhere and described nowhere.
	c.Assert(rolesNamedByDescription(live), qt.Not(qt.Contains), "ptah_ref_owner_137")
	c.Assert(described, qt.Not(qt.Contains), "ptah_ref_owner_137")
	// The control: a role an actual GRANT names is both named and described.
	c.Assert(rolesNamedByDescription(live), qt.Contains, "ptah_ref_granted_137")
	c.Assert(described, qt.Contains, "ptah_ref_granted_137")
}

// rolesNamedByDescription lists every role name the non-role parts of a
// description refer to: grant subjects, grantors, and the roles a row-level
// security policy applies to.
//
// The names a description never defines are dropped, because a description
// never refers to them either: PUBLIC is a keyword rather than a role, and the
// reserved pg_ roles and the bootstrap superuser are excluded from role
// reporting by readRoles. Leaving the bootstrap superuser in made this guard
// fail against any connection that IS the bootstrap superuser -- the grantor
// of every GRANT it runs -- while the document it produces names no such role
// and the pinned Atlas community binary v1.3.0 reads it at exit 0. That is a
// property of who connected, not of the description, and a guard that fails on
// a legitimate connection is a guard that gets switched off.
func rolesNamedByDescription(schema *dbschematypes.DBSchema) []string {
	var named []string
	for _, grant := range schema.Grants {
		named = append(named, grant.Role)
		if grant.GrantedBy != "" {
			named = append(named, grant.GrantedBy)
		}
	}
	for _, policy := range schema.RLSPolicies {
		for role := range strings.SplitSeq(policy.ToRoles, ",") {
			named = append(named, strings.TrimSpace(role))
		}
	}
	return slices.DeleteFunc(named, func(name string) bool {
		return name == "" || name == "PUBLIC" ||
			name == "postgres" || strings.HasPrefix(name, "pg_")
	})
}

// TestPostgreSQLRoleOutOfScopeIsPresentNotAbsentIntegration pins the other
// half of scoping roles to the inspected schemas (stokaro/ptah#1267): the
// reader may leave a role out of the description, but the comparator must not
// then conclude the role does not exist.
//
// PostgreSQL roles are cluster-wide. Scoping the description alone made every
// role outside the inspected schemas look absent, so a desired schema naming
// one planned CREATE ROLE and the server refused it at SQLSTATE 42710 --
// `role "admin_user" already exists`, which took stokaro/ptah#1273's
// integration job red three times. That is requirement 2 of
// stokaro/ptah#1276: "not described" and "not present" have to stay
// distinguishable in what the comparator consumes.
//
// The fixture holds both halves so neither can be satisfied by a blanket
// answer: a role that exists outside the scope must be planned as nothing,
// and a role that exists nowhere must still be planned as a CREATE.
//
// Say it plainly: without POSTGRES_TEST_DSN this test SKIPS and the run still
// reports ok, so on any job that does not set it the SQL in
// readRolesOutOfScope has no coverage from here at all. The white-box guard in
// internal/dbschema/postgres/reader_roles_internal_test.go covers the query
// text on every unit run, deliberately, because it is the only thing that runs
// when this does not -- and it is not a substitute for a live server: it
// cannot tell whether PostgreSQL agrees with what the predicate means.
//
// The reserved roles are outside this test as they are outside the reader: pg_
// names and the bootstrap superuser are in neither list, so a desired schema
// naming one is refused before anything is compared or planned rather than
// turned into a CREATE ROLE the server refuses. See
// compare.TestRolesReservedNameIsRefusedBeforeThisComparisonRunsAtAll and
// go.5x5.cz/ptah/internal/reservedrole.
func TestPostgreSQLRoleOutOfScopeIsPresentNotAbsentIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupScopedRolePresenceIntegration(c, db)
	c.Cleanup(func() { cleanupScopedRolePresenceIntegration(c, db) })

	// A role the cluster has and the inspected schema does not use: nothing
	// grants it anything and no policy names it.
	_, err = db.Exec("CREATE ROLE ptah_scope_outside_137 LOGIN")
	c.Assert(err, qt.IsNil)

	reader := postgres.NewPostgreSQLReader(db, "public")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	// The description leaves it out, which is what stokaro/ptah#1267 asked
	// for, and the reader still reports that it exists.
	c.Assert(integrationRoleNames(live.Roles), qt.Not(qt.Contains), "ptah_scope_outside_137")
	c.Assert(integrationRoleNames(live.RolesOutOfScope), qt.Contains, "ptah_scope_outside_137")

	desired := &goschema.Database{
		Roles: []goschema.Role{
			{Name: "ptah_scope_outside_137", Login: true, Inherit: true},
			{Name: "ptah_scope_absent_137", Login: true, Inherit: true},
		},
	}
	// Compare against the role facts alone: the rest of this shared database
	// belongs to other tests, and the decision under test is the role one.
	rolesOnly := &dbschematypes.DBSchema{
		Roles:           live.Roles,
		RolesOutOfScope: live.RolesOutOfScope,
	}
	diff := schemadiff.Compare(desired, rolesOnly)

	c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"ptah_scope_absent_137"})

	// And the plan applies. Before this fix the same plan carried
	// CREATE ROLE "ptah_scope_outside_137" and died on it.
	nodes, err := planner.GenerateSchemaDiffAST(diff, desired, "postgres")
	c.Assert(err, qt.IsNil)
	migrationSQL, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationSQL, qt.Not(qt.Contains), "ptah_scope_outside_137")
	for _, statement := range migrator.SplitSQLStatements(migrationSQL) {
		_, execErr := db.Exec(statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement failed: %s", statement))
	}
}

// integrationRoleNames lists the names of introspected roles.
func integrationRoleNames(roles []dbschematypes.DBRole) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

func cleanupScopedRolePresenceIntegration(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(`
DO $ptah_cleanup_scope_roles$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ptah_scope_outside_137', 'ptah_scope_absent_137'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE %I FROM %I', role_name, current_user);
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$ptah_cleanup_scope_roles$;`)
	c.Check(err, qt.IsNil)
}

func cleanupDescribedRolesIntegration(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec("DROP TABLE IF EXISTS ptah_ref_granted_tbl_137 CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec("DROP TABLE IF EXISTS ptah_ref_untouched_137 CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec(`
DO $ptah_cleanup_ref_roles$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ptah_ref_granted_137', 'ptah_ref_owner_137'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE %I FROM %I', role_name, current_user);
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$ptah_cleanup_ref_roles$;`)
	c.Check(err, qt.IsNil)
}

// TestPostgreSQLUndescribedRolesAreReportedAndRecoverableIntegration is the
// guard on the half of stokaro/ptah#1267 that is not the scoping.
//
// Scoping the description removed a capability: before it, `ptah db read` of a
// database emitted CREATE ROLE for every role Ptah manages on the server, and
// that output could be replayed into a DIFFERENT cluster to reproduce them.
// Measured on PostgreSQL 17.10 across two containers, on a database holding one
// table and one ungranted cluster role: 4 CREATE ROLE before, 0 after, and
// `ptah-compat schema apply --dry-run` against an empty database in the second
// cluster planned three of them before and none after. AGENTS.md
// ("Compatibility never removes a capability. Constitute it, do not discard
// it.") requires that to stay reachable on the same surface, and requires what
// the default leaves out to be reported rather than dropped in silence, so this
// asserts both halves at once and in both directions.
//
// Said plainly, the way the sibling guards say it: without POSTGRES_TEST_DSN
// this test SKIPS and the run still reports ok. The wiring it covers -- the
// reader honoring the variable, and `ptah db read` printing the note -- has
// unit coverage in internal/dbschema/postgres/reader_roles_internal_test.go and
// internal/rolescope, which is what runs when this does not; neither of those
// can show that a real server answers the widened read with the roles this one
// checks for.
func TestPostgreSQLUndescribedRolesAreReportedAndRecoverableIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupUndescribedRolesIntegration(c, db)
	c.Cleanup(func() { cleanupUndescribedRolesIntegration(c, db) })

	// A role the cluster has and the inspected schema does not use, plus a
	// pg-prefixed ordinary role: the reserved-prefix pattern is escaped, so
	// this one is MANAGED and has to come back with the others rather than
	// being swallowed by `pg_%` (stokaro/ptah#1291, stokaro/ptah#1292).
	_, err = db.Exec(`
CREATE SCHEMA ptah_undescribed_schema_137;
CREATE ROLE ptah_undescribed_outside_137 LOGIN;
CREATE ROLE pgbouncer_undescribed_137 LOGIN;`)
	c.Assert(err, qt.IsNil)

	scopedCmd := readdb.NewReadDBCommand()
	var scopedOut, scopedErr bytes.Buffer
	scopedCmd.SetContext(t.Context())
	scopedCmd.SetOut(&scopedOut)
	scopedCmd.SetErr(&scopedErr)
	scopedCmd.SetArgs([]string{"--db-url", dsn, "--schemas", "ptah_undescribed_schema_137"})

	c.Assert(scopedCmd.Execute(), qt.IsNil)

	// The default: neither role is described, and the operator is told so
	// without being told their names.
	c.Assert(scopedOut.String(), qt.Not(qt.Contains), "ptah_undescribed_outside_137")
	c.Assert(scopedOut.String(), qt.Not(qt.Contains), "pgbouncer_undescribed_137")
	c.Assert(scopedErr.String(), qt.Contains, "roles Ptah manages on this server are not described")
	c.Assert(scopedErr.String(), qt.Contains, "Set PTAH_POSTGRES_INSPECT_ALL_ROLES=1")
	c.Assert(scopedErr.String(), qt.Not(qt.Contains), "ptah_undescribed_outside_137")
	c.Assert(scopedOut.String(), qt.Not(qt.Contains), "PTAH_POSTGRES_INSPECT_ALL_ROLES",
		qt.Commentf("the note belongs on the diagnostics stream, never in the SQL"))

	// The compatibility surface owes the same two answers, and it is a
	// different call path: `ptah-compat schema inspect` renders through
	// atlasschema.Inspect and reports on its own diagnostics writer rather
	// than on a cobra stderr.
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	var inspectDiag bytes.Buffer
	inspected, err := atlasschema.Inspect(t.Context(), conn, atlasschema.InspectOptions{
		Schemas:     []string{"ptah_undescribed_schema_137"},
		Diagnostics: &inspectDiag,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(inspected, qt.Not(qt.Contains), "ptah_undescribed_outside_137")
	c.Assert(inspected, qt.Not(qt.Contains), "PTAH_POSTGRES_INSPECT_ALL_ROLES",
		qt.Commentf("the note belongs on the diagnostics stream, never in the document"))
	c.Assert(inspectDiag.String(), qt.Contains, "roles Ptah manages on this server are not described")
	c.Assert(inspectDiag.String(), qt.Contains, "Set PTAH_POSTGRES_INSPECT_ALL_ROLES=1")

	// The opt-in: the same read describes them again, and says nothing about
	// an omission because there is none.
	t.Setenv(rolescope.DescribeAllEnvVar, "1")
	fullCmd := readdb.NewReadDBCommand()
	var fullOut, fullErr bytes.Buffer
	fullCmd.SetContext(t.Context())
	fullCmd.SetOut(&fullOut)
	fullCmd.SetErr(&fullErr)
	fullCmd.SetArgs([]string{"--db-url", dsn, "--schemas", "ptah_undescribed_schema_137"})

	c.Assert(fullCmd.Execute(), qt.IsNil)

	c.Assert(fullOut.String(), qt.Contains, `CREATE ROLE "ptah_undescribed_outside_137"`)
	c.Assert(fullOut.String(), qt.Contains, `CREATE ROLE "pgbouncer_undescribed_137"`)
	c.Assert(fullErr.String(), qt.Not(qt.Contains), "are not described")

	// And the widened read is still a read of the roles Ptah MANAGES. The
	// reserved names stay out in both directions, so the opt-in can never plan
	// a statement PostgreSQL is guaranteed to reject.
	c.Assert(fullOut.String(), qt.Not(qt.Contains), `CREATE ROLE "postgres"`)
	c.Assert(fullOut.String(), qt.Not(qt.Contains), `CREATE ROLE "pg_`)

	// Both directions on the comparator: widening the description must not
	// change one planned statement. The role exists either way, so it is never
	// added; a role that exists nowhere still is.
	reader := postgres.NewPostgreSQLReader(db, "ptah_undescribed_schema_137")
	full, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(integrationRoleNames(full.Roles), qt.Contains, "ptah_undescribed_outside_137")
	c.Assert(full.RolesOutOfScope, qt.HasLen, 0)

	desired := &goschema.Database{
		Roles: []goschema.Role{
			{Name: "ptah_undescribed_outside_137", Login: true, Inherit: true},
			{Name: "ptah_undescribed_absent_137", Login: true, Inherit: true},
		},
	}
	diff := schemadiff.Compare(desired, &dbschematypes.DBSchema{
		Roles:           full.Roles,
		RolesOutOfScope: full.RolesOutOfScope,
	})
	c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"ptah_undescribed_absent_137"})
}

func cleanupUndescribedRolesIntegration(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec("DROP SCHEMA IF EXISTS ptah_undescribed_schema_137 CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec(`
DO $ptah_cleanup_undescribed_roles$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ptah_undescribed_outside_137', 'pgbouncer_undescribed_137', 'ptah_undescribed_absent_137'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE %I FROM %I', role_name, current_user);
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$ptah_cleanup_undescribed_roles$;`)
	c.Check(err, qt.IsNil)
}

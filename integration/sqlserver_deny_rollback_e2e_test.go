//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// sqlServerRollbackBase is the table every declaration of a rollback test
// keeps.
const sqlServerRollbackBase = "CREATE TABLE dbo.t (id INT PRIMARY KEY, a INT, b INT);\n"

// sqlServerRollbackApp is the schema app and its table, which the user
// rb_owner owns, as a declaration keeps them.
const sqlServerRollbackApp = "CREATE SCHEMA app;\nCREATE TABLE app.s (id INT PRIMARY KEY);\n"

// sqlServerRollbackFixture gives a scratch database the table of
// [sqlServerRollbackBase], the role rb_reader, which no declaration manages,
// and the schema app with its table, owned by rb_owner.
var sqlServerRollbackFixture = []string{
	"CREATE TABLE dbo.t (id INT PRIMARY KEY, a INT, b INT)",
	"CREATE ROLE rb_reader",
	"CREATE USER rb_owner WITHOUT LOGIN",
	"CREATE SCHEMA app AUTHORIZATION rb_owner",
	"CREATE TABLE app.s (id INT PRIMARY KEY)",
}

// sqlServerRowsOf reads every row of sys.database_permissions the grantee
// holds, as "STATE PERMISSION securable column grantor": the state is G, W or
// D, and a schema is named SCHEMA::name.
func sqlServerRowsOf(c *qt.C, db *sql.DB, grantee string) []string {
	c.Helper()
	// The parts are joined here rather than in the query: the catalog's names
	// and a scratch database's literals can carry different collations, which
	// the server refuses to concatenate.
	rows, err := db.QueryContext(c.Context(), `
		SELECT pe.state, pe.permission_name,
			CASE pe.class WHEN 3 THEN N'SCHEMA::' + SCHEMA_NAME(pe.major_id)
				ELSE OBJECT_SCHEMA_NAME(pe.major_id) + N'.' + OBJECT_NAME(pe.major_id) END,
			COALESCE(COL_NAME(pe.major_id, NULLIF(pe.minor_id, 0)), N'-'), grantor.name
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS grantee ON grantee.principal_id = pe.grantee_principal_id
		JOIN sys.database_principals AS grantor ON grantor.principal_id = pe.grantor_principal_id
		WHERE grantee.name = @p1 AND pe.class IN (1, 3)`, grantee)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var state, permission, securable, column, grantor string
		c.Assert(rows.Scan(&state, &permission, &securable, &column, &grantor), qt.IsNil)
		held = append(held, strings.Join([]string{state, permission, securable, column, grantor}, " "))
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(held)
	return held
}

// sqlServerRollbackDatabase is a scratch database holding
// [sqlServerRollbackFixture] and held.
func sqlServerRollbackDatabase(c *qt.C, held ...string) (string, *sql.DB) {
	c.Helper()
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, append(append([]string(nil), sqlServerRollbackFixture...), held...)...)
	return target, db
}

// A migration that replaces a row of a role the schema does not manage, on
// SQL Server 2025, is generated, run up and run down, and the role holds the
// row it held before: the same state (G, W or D), on the same securable, from
// the same grantor. The server keeps one row for a permission of a grantee on
// a securable, so a DENY takes the place of a GRANT and a GRANT the place of a
// DENY, and a down migration that only revokes leaves the role holding
// nothing.
//
// A row the owner of the securable gave is the one replaced, and the down
// migration, run as the same principal, records that grantor again: dbo on
// dbo.t, rb_owner on the schema app and its table, and the principal a
// procedure, a function or a table was given to on it. A row another principal
// gave from its grant option stays through the DENY and through the rollback.
func TestMigrationsDownSQLServerGivesBackTheReplacedRowE2E(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		before   []string
		afterUp  []string
	}{
		{
			name:     "an added object grant option",
			held:     []string{"GRANT SELECT ON dbo.t TO rb_reader"},
			declared: "GRANT SELECT ON dbo.t TO rb_reader WITH GRANT OPTION;\n",
			before:   []string{"G SELECT dbo.t - dbo"},
			afterUp:  []string{"W SELECT dbo.t - dbo"},
		},
		{
			name:     "an added column grant option",
			held:     []string{"GRANT SELECT (a) ON dbo.t TO rb_reader"},
			declared: "GRANT SELECT (a) ON dbo.t TO rb_reader WITH GRANT OPTION;\n",
			before:   []string{"G SELECT dbo.t a dbo"},
			afterUp:  []string{"W SELECT dbo.t a dbo"},
		},
		{
			name:     "an added schema grant option",
			held:     []string{"GRANT SELECT ON SCHEMA::dbo TO rb_reader"},
			declared: "GRANT SELECT ON SCHEMA::dbo TO rb_reader WITH GRANT OPTION;\n",
			before:   []string{"G SELECT SCHEMA::dbo - dbo"},
			afterUp:  []string{"W SELECT SCHEMA::dbo - dbo"},
		},
		{
			name: "an added option beside another grantor's plain row",
			held: []string{
				"CREATE USER rb_granter WITHOUT LOGIN",
				"GRANT SELECT ON dbo.t TO rb_granter WITH GRANT OPTION",
				"GRANT SELECT ON dbo.t TO rb_reader",
				"GRANT SELECT ON dbo.t TO rb_reader AS rb_granter",
			},
			declared: "GRANT SELECT ON dbo.t TO rb_reader WITH GRANT OPTION;\n",
			before:   []string{"G SELECT dbo.t - dbo", "G SELECT dbo.t - rb_granter"},
			afterUp:  []string{"G SELECT dbo.t - rb_granter", "W SELECT dbo.t - dbo"},
		},
		{
			name:     "a grant a deny replaces",
			held:     []string{"GRANT SELECT ON dbo.t TO rb_reader"},
			declared: "DENY SELECT ON dbo.t TO rb_reader;\n",
			before:   []string{"G SELECT dbo.t - dbo"},
			afterUp:  []string{"D SELECT dbo.t - dbo"},
		},
		{
			name:     "a grant with the grant option a deny replaces",
			held:     []string{"GRANT SELECT ON dbo.t TO rb_reader WITH GRANT OPTION"},
			declared: "DENY SELECT ON dbo.t TO rb_reader;\n",
			before:   []string{"W SELECT dbo.t - dbo"},
			afterUp:  []string{"D SELECT dbo.t - dbo"},
		},
		{
			name:     "a deny a grant replaces",
			held:     []string{"DENY SELECT ON dbo.t TO rb_reader"},
			declared: "GRANT SELECT ON dbo.t TO rb_reader;\n",
			before:   []string{"D SELECT dbo.t - dbo"},
			afterUp:  []string{"G SELECT dbo.t - dbo"},
		},
		{
			name:     "a deny a grant with the grant option replaces",
			held:     []string{"DENY SELECT ON dbo.t TO rb_reader"},
			declared: "GRANT SELECT ON dbo.t TO rb_reader WITH GRANT OPTION;\n",
			before:   []string{"D SELECT dbo.t - dbo"},
			afterUp:  []string{"W SELECT dbo.t - dbo"},
		},
		{
			name:     "a column grant a column deny replaces",
			held:     []string{"GRANT SELECT (a) ON dbo.t TO rb_reader"},
			declared: "DENY SELECT (a) ON dbo.t TO rb_reader;\n",
			before:   []string{"G SELECT dbo.t a dbo"},
			afterUp:  []string{"D SELECT dbo.t a dbo"},
		},
		{
			name:     "a column deny a column grant replaces",
			held:     []string{"DENY SELECT (a) ON dbo.t TO rb_reader"},
			declared: "GRANT SELECT (a) ON dbo.t TO rb_reader;\n",
			before:   []string{"D SELECT dbo.t a dbo"},
			afterUp:  []string{"G SELECT dbo.t a dbo"},
		},
		{
			name:     "a column grant and a column deny a deny on the table takes",
			held:     []string{"GRANT SELECT (a) ON dbo.t TO rb_reader", "DENY SELECT (b) ON dbo.t TO rb_reader"},
			declared: "DENY SELECT ON dbo.t TO rb_reader;\n",
			before:   []string{"D SELECT dbo.t b dbo", "G SELECT dbo.t a dbo"},
			afterUp:  []string{"D SELECT dbo.t - dbo"},
		},
		{
			name:     "a schema grant a schema deny replaces",
			held:     []string{"GRANT SELECT ON SCHEMA::dbo TO rb_reader"},
			declared: "DENY SELECT ON SCHEMA::dbo TO rb_reader;\n",
			before:   []string{"G SELECT SCHEMA::dbo - dbo"},
			afterUp:  []string{"D SELECT SCHEMA::dbo - dbo"},
		},
		{
			name:     "a schema deny a schema grant replaces",
			held:     []string{"DENY SELECT ON SCHEMA::dbo TO rb_reader"},
			declared: "GRANT SELECT ON SCHEMA::dbo TO rb_reader;\n",
			before:   []string{"D SELECT SCHEMA::dbo - dbo"},
			afterUp:  []string{"G SELECT SCHEMA::dbo - dbo"},
		},
		{
			name:     "a grant on a schema another principal owns",
			held:     []string{"GRANT SELECT ON SCHEMA::app TO rb_reader"},
			declared: sqlServerRollbackApp + "DENY SELECT ON SCHEMA::app TO rb_reader;\n",
			before:   []string{"G SELECT SCHEMA::app - rb_owner"},
			afterUp:  []string{"D SELECT SCHEMA::app - rb_owner"},
		},
		{
			name:     "a grantable grant on a table in a schema another principal owns",
			held:     []string{"GRANT SELECT ON app.s TO rb_reader WITH GRANT OPTION"},
			declared: sqlServerRollbackApp + "DENY SELECT ON app.s TO rb_reader;\n",
			before:   []string{"W SELECT app.s - rb_owner"},
			afterUp:  []string{"D SELECT app.s - rb_owner"},
		},
		{
			name: "a grant on a procedure given to another principal",
			held: []string{
				"CREATE USER rb_powner WITHOUT LOGIN",
				"CREATE PROCEDURE dbo.p AS BEGIN SELECT 1 AS one; END",
				"ALTER AUTHORIZATION ON OBJECT::dbo.p TO rb_powner",
				"GRANT EXECUTE ON dbo.p TO rb_reader",
			},
			declared: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1 AS one; END;\nGO\nDENY EXECUTE ON dbo.p TO rb_reader;\n",
			before:   []string{"G EXECUTE dbo.p - rb_powner"},
			afterUp:  []string{"D EXECUTE dbo.p - rb_powner"},
		},
		{
			name: "a grant on a function given to another principal",
			held: []string{
				"CREATE USER rb_fowner WITHOUT LOGIN",
				"CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN RETURN 1; END",
				"ALTER AUTHORIZATION ON OBJECT::dbo.f TO rb_fowner",
				"GRANT EXECUTE ON dbo.f TO rb_reader",
			},
			declared: "CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN RETURN 1; END;\nGO\nDENY EXECUTE ON dbo.f TO rb_reader;\n",
			before:   []string{"G EXECUTE dbo.f - rb_fowner"},
			afterUp:  []string{"D EXECUTE dbo.f - rb_fowner"},
		},
		{
			name: "a grant on a table given to another principal",
			held: []string{
				"CREATE USER rb_towner WITHOUT LOGIN",
				"CREATE TABLE dbo.u (id INT PRIMARY KEY)",
				"ALTER AUTHORIZATION ON OBJECT::dbo.u TO rb_towner",
				"GRANT SELECT ON dbo.u TO rb_reader",
			},
			declared: "CREATE TABLE dbo.u (id INT PRIMARY KEY);\nDENY SELECT ON dbo.u TO rb_reader;\n",
			before:   []string{"G SELECT dbo.u - rb_towner"},
			afterUp:  []string{"D SELECT dbo.u - rb_towner"},
		},
		{
			name: "a grant another principal gave from its grant option",
			held: []string{
				"CREATE USER rb_granter WITHOUT LOGIN",
				"GRANT SELECT ON dbo.t TO rb_granter WITH GRANT OPTION",
				"GRANT SELECT ON dbo.t TO rb_reader AS rb_granter",
			},
			declared: "DENY SELECT ON dbo.t TO rb_reader;\n",
			before:   []string{"G SELECT dbo.t - rb_granter"},
			afterUp:  []string{"D SELECT dbo.t - dbo", "G SELECT dbo.t - rb_granter"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerRollbackDatabase(c, test.held...)
			c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, test.before)
			dir := filepath.Join(c.TempDir(), "migrations")
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerRollbackBase+test.declared)

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file,
				"--migrations-dir", dir, "--name", "replace")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, test.afterUp)

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, test.before)
		})
	}
}

// A rollback derived with `migrations down --plan` cannot give back the row a
// DENY on a role the migrations do not manage replaced: the replay does not
// create the role, and the live database holds only the DENY. So the plan
// leaves the DENY, and says so, rather than reading as a complete rollback
// with no statement. The replay does not run the rolled-back migrations, so
// the line says it names every such DENY, not only theirs.
func TestMigrationsDownPlanSQLServerNamesTheDenyItLeavesE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	shadow, _ := declaredSQLServerDatabase(c)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_table.up.sql"), []byte(sqlServerRollbackBase), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_table.down.sql"), []byte("DROP TABLE dbo.t;\n"), 0o600), qt.IsNil)
	first := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	declaredSQLServerExec(c, db, "CREATE ROLE rb_reader", "GRANT SELECT ON dbo.t TO rb_reader")
	file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerRollbackBase+"DENY SELECT ON dbo.t TO rb_reader;\n")
	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir, "--name", "deny")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir,
		"--plan", "--shadow-db", shadow, "--target", "1", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(down.Stdout, qt.Contains, "Derived rollback plan: 0 statements")
	c.Assert(unmanagedLines(down.Stderr), qt.DeepEquals, []string{
		`UNMANAGED: the database denies SELECT on TABLE dbo.t to "rb_reader", and the target version does not declare the DENY, ` +
			"so the derived rollback leaves it and cannot give back a grant it replaced; this lists every such DENY, " +
			"not only the ones the rolled-back migrations added, so revoke it by hand if one of them added it",
	})
	c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, []string{"D SELECT dbo.t - dbo"})
}

// sqlServerGrantorPassword is the password of the login
// [TestMigrationsDownSQLServerRunByAGrantorWithControlE2E] creates.
//
// #nosec G101 -- the password of a login the test creates and drops on a test server.
const sqlServerGrantorPassword = "Rb_grantor_pw_1!"

// A migration run by a principal that is not the owner, holding CONTROL on the
// schema and a grant option of its own, records the owner as the grantor of
// the DENY it runs, and the DENY leaves the grant that principal gave from its
// grant option in place. So that grant is not a row the DENY replaced, and the
// down migration only takes the DENY away: a GRANT run with CONTROL would add a
// second row, from dbo, which the role did not hold before.
//
// The principal also holds CREATE TABLE, for the revision table, and VIEW
// DEFINITION on the database, without which the read does not see the role.
func TestMigrationsDownSQLServerRunByAGrantorWithControlE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	login := fmt.Sprintf("rv6706_%d", time.Now().UnixNano()%1_000_000_000)
	declaredSQLServerExec(c, db, "CREATE LOGIN ["+login+"] WITH PASSWORD = '"+sqlServerGrantorPassword+"', CHECK_POLICY = OFF")
	// The login is server-wide, so it is dropped on its own; the user it maps
	// to goes with the scratch database.
	c.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP LOGIN ["+login+"]")
		c.Check(err, qt.IsNil)
	})
	declaredSQLServerExec(c, db, append(append([]string(nil), sqlServerRollbackFixture...),
		"CREATE USER rb_x FOR LOGIN ["+login+"]",
		"GRANT SELECT ON dbo.t TO rb_x WITH GRANT OPTION",
		"GRANT CONTROL ON SCHEMA::dbo TO rb_x",
		"GRANT CREATE TABLE TO rb_x",
		"GRANT VIEW DEFINITION TO rb_x",
		"GRANT SELECT ON dbo.t TO rb_reader AS rb_x",
	)...)
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(login, sqlServerGrantorPassword)
	asGrantor := parsed.String()
	c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, []string{"G SELECT dbo.t - rb_x"})
	dir := filepath.Join(c.TempDir(), "migrations")
	file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerRollbackBase+"DENY SELECT ON dbo.t TO rb_reader;\n")

	generated := runPtahIn(c, "migrations", "generate", "--db-url", asGrantor, "--schema-file", file,
		"--migrations-dir", dir, "--name", "deny")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", asGrantor, "--migrations-dir", dir)
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, []string{"D SELECT dbo.t - dbo", "G SELECT dbo.t - rb_x"})

	down := runPtahIn(c, "migrations", "down", "--db-url", asGrantor, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(sqlServerRowsOf(c, db, "rb_reader"), qt.DeepEquals, []string{"G SELECT dbo.t - rb_x"})
}

//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // registers the ClickHouse driver the member session opens
	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"  // registers the pgx driver the CockroachDB setup and member session open
	_ "github.com/microsoft/go-mssqldb" // registers the SQL Server driver the setup and member session open
	_ "github.com/sijms/go-ora/v3"      // registers the Oracle driver the member session opens

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// revokeAllServer is a database holding the table t1, a role that holds
// privileges on it, and a session of a user the role is granted to.
type revokeAllServer struct {
	// url is the address ptah connects with.
	url  string
	role string
	// catalog runs heldQuery, which lists the role's privileges on t1 one row
	// each, with heldArgs.
	catalog   *sql.DB
	heldQuery string
	heldArgs  []any
	member    *sql.DB
}

// apply runs `ptah schema apply` with the desired schema, where {role} stands
// for the role, and the extra arguments.
func (s revokeAllServer) apply(c *qt.C, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(strings.ReplaceAll(desired, "{role}", s.role)), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute},
		append([]string{"schema", "apply", "--db-url", s.url, "--schema-file", file}, args...)...)
}

// privileges is what the catalog reports the role holds on t1, in order.
func (s revokeAllServer) privileges(c *qt.C) []string {
	c.Helper()
	rows, err := s.catalog.QueryContext(c.Context(), s.heldQuery, s.heldArgs...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// revokesEverything applies desired, which revokes ALL on t1 from the role, and
// asserts what the role holds afterwards, that the member is refused each
// statement with an error matching denied, and that the next plan is empty.
func (s revokeAllServer) revokesEverything(c *qt.C, desired string, want, refused []string, denied string) {
	c.Helper()
	applied := s.apply(c, desired, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(s.privileges(c), qt.DeepEquals, want)
	for _, statement := range refused {
		_, err := s.member.ExecContext(c.Context(), statement)
		c.Assert(err, qt.ErrorMatches, denied, qt.Commentf("%s", statement))
	}

	again := s.apply(c, desired, "--dry-run")

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// revokeAllStamp names the objects of one run apart from every other run.
func revokeAllStamp() string {
	return fmt.Sprintf("ptah_ra_%d", time.Now().UnixNano()%1_000_000_000)
}

// openRevokeAllMember opens a session of the member on the database address
// names, with the user replaced, and closes it when the test ends.
func openRevokeAllMember(c *qt.C, driver, address string, user *url.Userinfo) *sql.DB {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.User = user
	member, err := sql.Open(driver, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(member.Close(), qt.IsNil) })
	return member
}

// execRevokeAllSetup runs each statement on db, where {role} stands for the
// role and {user} for the member's user.
func execRevokeAllSetup(c *qt.C, db *sql.DB, role, user string, statements ...string) {
	c.Helper()
	names := strings.NewReplacer("{role}", role, "{user}", user)
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), names.Replace(statement))
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// sqlServerMemberPassword is the password of every member login this file
// creates.
//
// #nosec G101 -- the password of a login the test creates and drops on a test server.
const sqlServerMemberPassword = "Ptah_member_pw_1!"

// newSQLServerRevokeAllServer creates a database, the role, and a login and
// user the role is granted to, then runs held. The database and the login are
// dropped when the test ends.
func newSQLServerRevokeAllServer(c *qt.C, held string) revokeAllServer {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	admin, err := sql.Open("sqlserver", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	role := revokeAllStamp()
	user := role + "_u"
	execRevokeAllSetup(c, admin, role, user,
		"CREATE LOGIN [{user}] WITH PASSWORD = '"+sqlServerMemberPassword+"', CHECK_POLICY = OFF")
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP LOGIN ["+user+"]")
		c.Check(dropErr, qt.IsNil)
	})
	databaseURL, db := newSQLServerMigrationDatabase(c, c.Context(), adminURL, "")
	execRevokeAllSetup(c, db, role, user,
		"CREATE TABLE dbo.t1 (id INT PRIMARY KEY, name NVARCHAR(50))",
		"INSERT INTO dbo.t1 VALUES (1, 'a')",
		"CREATE ROLE [{role}]",
		"CREATE USER [{user}] FOR LOGIN [{user}]",
		"ALTER ROLE [{role}] ADD MEMBER [{user}]",
		held)
	return revokeAllServer{
		url:     databaseURL,
		role:    role,
		catalog: db,
		heldQuery: `SELECT p.permission_name
			FROM sys.database_permissions p
			JOIN sys.database_principals d ON d.principal_id = p.grantee_principal_id
			WHERE d.name = @p1 AND p.class = 1 AND p.major_id = OBJECT_ID('dbo.t1') AND p.state IN ('G', 'W')
			ORDER BY p.permission_name`,
		heldArgs: []any{role},
		member:   openRevokeAllMember(c, "sqlserver", databaseURL, url.UserPassword(user, sqlServerMemberPassword)),
	}
}

// A REVOKE ALL takes every permission the role holds on the table. SQL Server's
// own REVOKE ALL leaves ALTER, CONTROL and VIEW DEFINITION, measured on SQL
// Server 2025, and a member keeps reading, altering and dropping the table
// through CONTROL. With ALL read as PostgreSQL's table list, the plan for a role
// the schema does not create revokes DELETE, INSERT, REFERENCES, SELECT and
// UPDATE and leaves the same three, and the next plan reads as synced.
func TestSchemaApplySQLServerRevokeAllE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(50));\n"
	tests := []struct {
		name     string
		held     string
		declared string
	}{
		{
			name:     "a role the schema does not create",
			held:     "GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALTER, CONTROL, VIEW DEFINITION ON dbo.t1 TO [{role}]",
			declared: table + "REVOKE ALL ON t1 FROM [{role}];\n",
		},
		{
			name:     "a role the schema creates",
			held:     "GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALTER, CONTROL, VIEW DEFINITION ON dbo.t1 TO [{role}]",
			declared: table + "CREATE ROLE [{role}];\nREVOKE ALL ON t1 FROM [{role}];\n",
		},
		{
			name:     "a role holding what GRANT ALL gives",
			held:     "GRANT ALL ON dbo.t1 TO [{role}]",
			declared: table + "REVOKE ALL ON t1 FROM [{role}];\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newSQLServerRevokeAllServer(c, test.held)

			server.revokesEverything(c, test.declared, nil,
				[]string{"SELECT id FROM dbo.t1", "ALTER TABLE dbo.t1 ADD extra INT", "DROP TABLE dbo.t1"},
				`(?s).*(permission was denied|do not have permission).*`)
		})
	}
}

// newClickHouseRevokeAllServer creates a database, the role, and a user the
// role is granted by default, then runs held. The database, the user and the
// role are dropped when the test ends: a ClickHouse grant on a dropped table
// outlives it.
func newClickHouseRevokeAllServer(c *qt.C, held string) revokeAllServer {
	c.Helper()
	address := dbtarget.URL(c, dbtarget.ClickHouse)
	admin, err := sql.Open("clickhouse", address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	role := revokeAllStamp()
	user := role + "_u"
	c.Cleanup(func() {
		for _, statement := range []string{
			"DROP DATABASE IF EXISTS " + role + " SYNC",
			"DROP USER IF EXISTS " + user,
			"DROP ROLE IF EXISTS " + role,
		} {
			_, dropErr := admin.ExecContext(context.Background(), statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("%s", statement))
		}
	})
	execRevokeAllSetup(c, admin, role, user,
		"CREATE DATABASE {role}",
		"CREATE TABLE {role}.t1 (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO {role}.t1 VALUES (1, 'a')",
		"CREATE ROLE {role}",
		"CREATE USER {user} IDENTIFIED BY 'ptah_member_pw'",
		"GRANT {role} TO {user}",
		"SET DEFAULT ROLE {role} TO {user}",
		held)
	databaseURL, err := atlasurl.WithDatabaseName(address, role)
	c.Assert(err, qt.IsNil)
	return revokeAllServer{
		url:       databaseURL,
		role:      role,
		catalog:   admin,
		heldQuery: "SELECT access_type FROM system.grants WHERE role_name = ? AND database = ? ORDER BY access_type",
		heldArgs:  []any{role, role},
		member:    openRevokeAllMember(c, "clickhouse", databaseURL, url.UserPassword(user, "ptah_member_pw")),
	}
}

// A REVOKE ALL takes every access type the role holds on the table. With ALL
// read as PostgreSQL's table list, the plan revokes SELECT, INSERT and TRUNCATE
// and leaves DROP TABLE, ALTER UPDATE and the rest, a member drops the table,
// and the next plan reads as synced. Measured on ClickHouse 26.10.
func TestSchemaApplyClickHouseRevokeAllE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id UInt32, name String) ENGINE = MergeTree ORDER BY id;\n" +
		"REVOKE ALL ON t1 FROM {role};\n"
	tests := []struct {
		name string
		held string
	}{
		{name: "a role holding ALL", held: "GRANT ALL ON {role}.t1 TO {role}"},
		{name: "a role holding named access types", held: "GRANT SELECT, ALTER UPDATE, DROP TABLE, TRUNCATE ON {role}.t1 TO {role}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newClickHouseRevokeAllServer(c, test.held)

			server.revokesEverything(c, declared, nil,
				[]string{"SELECT id FROM t1", "ALTER TABLE t1 UPDATE name = 'b' WHERE id = 1", "TRUNCATE TABLE t1", "DROP TABLE t1"},
				`(?s).*code: 497, .*Not enough privileges.*`)
		})
	}
}

// newCockroachRevokeAllServer creates a database, the role, and a user the role
// is granted to, then runs held. The database, the user and the role are
// dropped when the test ends.
func newCockroachRevokeAllServer(c *qt.C, held string) revokeAllServer {
	c.Helper()
	fixture := newCockroachRoleFixture(c, c.Context(), "ptah_ra")
	user := fixture.role + "_u"
	execRevokeAllSetup(c, fixture.admin, fixture.role, user, "CREATE ROLE {role}", "CREATE USER {user}", "GRANT {role} TO {user}")
	c.Cleanup(func() {
		_, dropErr := fixture.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+user)
		c.Check(dropErr, qt.IsNil)
	})
	driverURL := postgresFamilyDriverURL(c, fixture.targetURL)
	db, err := sql.Open("pgx", driverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	execRevokeAllSetup(c, db, fixture.role, user,
		"CREATE TABLE t1 (id INT PRIMARY KEY, name STRING)",
		"INSERT INTO t1 VALUES (1, 'a')",
		held)
	return revokeAllServer{
		url:       fixture.targetURL,
		role:      fixture.role,
		catalog:   db,
		heldQuery: "SELECT privilege_type FROM [SHOW GRANTS ON TABLE t1] WHERE grantee = $1 ORDER BY privilege_type",
		heldArgs:  []any{fixture.role},
		member:    openRevokeAllMember(c, "pgx", driverURL, url.User(user)),
	}
}

// A REVOKE ALL takes every privilege the role holds on the table. With ALL
// read as PostgreSQL's table list, the plan for a role holding ALL revokes
// nothing, since the catalog reports that grant as one ALL row, and for a role
// holding named privileges it leaves CHANGEFEED, DROP and ZONECONFIG; a member
// drops the table, and the next plan reads as synced. Measured on CockroachDB
// 26.4.1.
func TestSchemaApplyCockroachDBRevokeAllE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY, name STRING);\nREVOKE ALL ON t1 FROM {role};\n"
	tests := []struct {
		name string
		held string
	}{
		{name: "a role holding ALL", held: "GRANT ALL ON t1 TO {role}"},
		{name: "a role holding named privileges", held: "GRANT SELECT, INSERT, DROP, ZONECONFIG, CHANGEFEED ON t1 TO {role}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newCockroachRevokeAllServer(c, test.held)

			server.revokesEverything(c, declared, nil,
				[]string{"SELECT id FROM t1", "ALTER TABLE t1 CONFIGURE ZONE USING gc.ttlseconds = 600", "DROP TABLE t1"},
				`(?s).*does not have .*privilege.*`)
		})
	}
}

// newOracleRevokeAllServer creates an account that owns t1 and may read the
// role catalog, the role, and a user the role is granted to, then runs held as
// the account, and returns the server and the account. The account, the user
// and the role are dropped when the test ends.
//
// The role is named in upper case, the way Oracle stores an unquoted name, so
// the declaration and the catalog spell it alike.
func newOracleRevokeAllServer(c *qt.C, held string) (revokeAllServer, string) {
	c.Helper()
	ctx := c.Context()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	account, role, user := "PTAH_RA_"+stamp, "PTAH_RA_ROLE_"+stamp, "PTAH_RA_M_"+stamp
	c.Cleanup(func() {
		dropOracleUser(context.Background(), c, admin, user)
		dropOracleUser(context.Background(), c, admin, account)
		dropOracleRole(context.Background(), c, admin, role)
	})
	createOracleUser(ctx, c, admin, account)
	execOracle(ctx, c, admin, "GRANT SELECT_CATALOG_ROLE TO "+account)
	execOracle(ctx, c, admin, "CREATE ROLE "+role)
	execOracle(ctx, c, admin, "CREATE USER "+user+" IDENTIFIED BY "+user)
	execOracle(ctx, c, admin, "GRANT CREATE SESSION, "+role+" TO "+user)

	accountURL := oracleURLAs(c, adminURL, account)
	driverURL := oracleRevokeAllDriverURL(c, accountURL)
	owner, err := sql.Open("oracle", driverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(owner.Close(), qt.IsNil) })
	execRevokeAllSetup(c, owner, role, user,
		"CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50))",
		"INSERT INTO t1 VALUES (1, 'a')",
		held)
	return revokeAllServer{
		url:       accountURL,
		role:      role,
		catalog:   owner,
		heldQuery: "SELECT privilege FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 AND table_name = 'T1' ORDER BY privilege",
		heldArgs:  []any{role, account},
		member:    openRevokeAllMember(c, "oracle", driverURL, url.UserPassword(user, user)),
	}, account
}

// oracleRevokeAllDriverURL is address for go-ora opened directly, with FAST
// LOGIN off as Ptah's connector sets it: with it on, the driver replays the
// first connection's login on the next one, Oracle 23 closes the socket, and
// every later connection in the process answers EOF.
func oracleRevokeAllDriverURL(c *qt.C, address string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("FAST LOGIN", "FALSE")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// A REVOKE ALL takes every privilege the role holds on the table. With ALL
// read as PostgreSQL's table list, the plan revokes DELETE, INSERT, SELECT and
// UPDATE and leaves ALTER, DEBUG, FLASHBACK, ON COMMIT REFRESH, QUERY REWRITE
// and READ; a member reads the table through READ and alters it, and the next
// plan reads as synced. Measured on Oracle 23.26.
func TestSchemaApplyOracleRevokeAllE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nREVOKE ALL ON t1 FROM {role};\n"
	tests := []struct {
		name string
		held string
	}{
		{name: "a role holding ALL", held: "GRANT ALL ON t1 TO {role}"},
		{name: "a role holding named privileges", held: "GRANT SELECT, ALTER, DEBUG ON t1 TO {role}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server, account := newOracleRevokeAllServer(c, test.held)

			server.revokesEverything(c, declared, nil,
				[]string{"SELECT id FROM " + account + ".t1", "ALTER TABLE " + account + ".t1 ADD (extra NUMBER)"},
				`(?s).*ORA-(00942|41900|01031).*`)
		})
	}
}

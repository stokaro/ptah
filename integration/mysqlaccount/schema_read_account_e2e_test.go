//go:build integration

// Package mysqlaccount_test holds what a MySQL or MariaDB schema read answers for
// an account that cannot see everything it reads. The answers come from the
// shipped binaries against a real server, and an account that may see everything
// is held to what the administrator's connection prints.
package mysqlaccount_test

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

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // the driver the administrator's connection opens with

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

const accountPassword = "ptah_account_password" // #nosec G101 -- the password of an account these tests create and drop

// everyDatabase is a privilege pattern matching both databases of a fixture and
// nothing a test does not own: `ptah_rt_` is the prefix the CI account is granted.
const everyDatabase = "`ptah\\_rt\\_acct\\__\\_%`"

// server is one engine under test.
type server struct {
	name  string
	admin dbtarget.Engine
	// test is the same server reached with the account CI runs as.
	test dbtarget.Engine
	dev  dbtarget.Engine
	// routineGrant shows routines, with {db} for a database it can apply to.
	routineGrant string
	// routineLine is how a refusal asks for it, with {db} for the database.
	routineLine string
	// setDefaultRole makes {role} active on login for {account}.
	setDefaultRole string
	mariaDB        bool
}

func servers() []server {
	return []server{
		{
			name:           "MySQL",
			admin:          dbtarget.MySQLAdmin,
			test:           dbtarget.MySQL,
			dev:            dbtarget.MySQLDevServer,
			routineGrant:   "GRANT SHOW_ROUTINE ON *.* TO {account}",
			routineLine:    "procedures and functions: SHOW_ROUTINE on *.*",
			setDefaultRole: "SET DEFAULT ROLE {role} TO {account}",
		},
		{
			name:           "MariaDB",
			admin:          dbtarget.MariaDBAdmin,
			test:           dbtarget.MariaDB,
			dev:            dbtarget.MariaDBDevServer,
			routineGrant:   "GRANT SHOW CREATE ROUTINE ON {db}.* TO {account}",
			routineLine:    "procedures and functions: SHOW CREATE ROUTINE on `{db}`.*",
			setDefaultRole: "SET DEFAULT ROLE {role} FOR {account}",
			mariaDB:        true,
		},
	}
}

// fixture is two databases that differ by one object of every kind a read needs
// a privilege to see.
type fixture struct {
	admin    *sql.DB
	adminURL string
	left     string
	right    string
	stamp    int64
}

// What both databases hold, what only the left one holds, and a procedure of the
// administrator's, which an account holding ALL PRIVILEGES on the database lists
// and on MySQL may not read.
var (
	sharedObjects = []string{
		"CREATE TABLE parent (id INT PRIMARY KEY)",
		"CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, amount INT)",
		"CREATE VIEW v_child AS SELECT id FROM child",
	}
	leftObjects = []string{
		"CREATE TABLE only_left (id INT PRIMARY KEY)",
		"CREATE INDEX child_amount_idx ON child (amount)",
		"ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)",
		"ALTER TABLE child ADD CONSTRAINT child_amount_ck CHECK (amount >= 0)",
		"CREATE OR REPLACE VIEW v_child AS SELECT id, amount FROM child",
		"CREATE TRIGGER child_trg BEFORE INSERT ON child FOR EACH ROW SET NEW.amount = 1",
	}
	leftProcedure = []string{"CREATE PROCEDURE p_noop() SELECT 1"}
)

func newFixture(c *qt.C, s server, extra ...string) fixture {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, s.admin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	f := fixture{
		admin:    admin,
		adminURL: dbtarget.URL(c, s.admin),
		left:     fmt.Sprintf("ptah_rt_acct_l_%d", stamp),
		right:    fmt.Sprintf("ptah_rt_acct_r_%d", stamp),
		stamp:    stamp,
	}
	for _, database := range []string{f.left, f.right} {
		f.exec(c, "CREATE DATABASE `"+database+"`")
		c.Cleanup(func() {
			_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+database+"`")
			c.Check(dropErr, qt.IsNil)
		})
		f.execIn(c, database, sharedObjects...)
	}
	f.execIn(c, f.left, leftObjects...)
	f.execIn(c, f.left, extra...)
	return f
}

func (f fixture) exec(c *qt.C, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// execIn runs statements on one connection with database selected.
func (f fixture) execIn(c *qt.C, database string, statements ...string) {
	c.Helper()
	conn, err := f.admin.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "USE `"+database+"`")
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// newAccount creates an account and runs grants for it, once per database when a
// grant names {db} or {literal}, and drops it when the test ends. {literal} spells
// the database with its underscores escaped: unescaped, a name holding `_` is a
// pattern to the server, and ranks below a pattern that matches more of it.
func (f fixture) newAccount(c *qt.C, name string, grants ...string) string {
	c.Helper()
	account := fmt.Sprintf("pa_%s_%d", name, f.stamp%1_000_000_000)
	quoted := "'" + account + "'@'%'"
	f.exec(c, "CREATE USER "+quoted+" IDENTIFIED BY '"+accountPassword+"'")
	c.Cleanup(func() {
		_, err := f.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+quoted)
		c.Check(err, qt.IsNil)
	})
	for _, grant := range grants {
		grant = strings.ReplaceAll(grant, "{account}", quoted)
		for _, database := range []string{f.left, f.right} {
			f.exec(c, strings.NewReplacer(
				"{db}", "`"+database+"`",
				"{literal}", "`"+strings.ReplaceAll(database, "_", `\_`)+"`",
			).Replace(grant))
		}
	}
	return account
}

// newRole creates a role and drops it when the test ends.
func (f fixture) newRole(c *qt.C, name string) string {
	c.Helper()
	role := fmt.Sprintf("pr_%s_%d", name, f.stamp%1_000_000_000)
	f.exec(c, "CREATE ROLE "+role)
	c.Cleanup(func() {
		_, err := f.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	return role
}

// url is the address of database as account; an empty account is the
// administrator, and an empty database the whole server.
func (f fixture) url(account, database string) string {
	address := f.adminURL[:strings.LastIndex(f.adminURL, "/")+1] + database
	if account == "" {
		return address
	}
	return asAccount(address, account, accountPassword)
}

// asAccount replaces the credentials of a URL, which CI spells in the driver's
// tcp(host:port) form that net/url cannot parse.
func asAccount(address, account, password string) string {
	scheme, rest, _ := strings.Cut(address, "://")
	_, host, _ := strings.Cut(rest, "@")
	return scheme + "://" + account + ":" + url.QueryEscape(password) + "@" + host
}

// diff runs schema diff from the left database to the right one as account.
func (f fixture) diff(c *qt.C, account string, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"schema", "diff", "--from", f.url(account, f.left), "--to", f.url(account, f.right)}, flags...)...)
}

// named fills {db} in lines with the database the refusal is about.
func named(database string, lines ...string) []string {
	filled := make([]string, 0, len(lines))
	for _, line := range lines {
		filled = append(filled, strings.ReplaceAll(line, "{db}", database))
	}
	return filled
}

// assertRefused holds a run to exitCode, no answer on standard output, and each
// line on standard error.
func assertRefused(c *qt.C, got clirun.Result, exitCode int, lines ...string) {
	c.Helper()
	comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
	c.Assert(got.ExitCode, qt.Equals, exitCode, comment)
	c.Assert(got.Stdout, qt.Not(qt.Contains), "synced", comment)
	c.Assert(got.Stderr, qt.Contains, "cannot see everything", comment)
	for _, line := range lines {
		c.Assert(got.Stderr, qt.Contains, line, comment)
	}
}

// assertSameAsAdministrator holds a run to the answer the administrator's
// connection gets: the expected value is read from the server, not restated.
func assertSameAsAdministrator(c *qt.C, got, administrator clirun.Result) {
	c.Helper()
	c.Assert(administrator.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", administrator.Stderr))
	c.Assert(administrator.Stdout, qt.Not(qt.Contains), "synced", qt.Commentf("the fixture differs, so this answer has to say so"))
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, administrator.Stdout)
}

// The lines of a refusal that do not depend on the engine.
const (
	lineListing     = "tables and views: SELECT on `{db}`.*"
	lineDefinitions = "SELECT and SHOW VIEW on `{db}`.*"
	lineTriggers    = "triggers: TRIGGER on `{db}`.*"
	lineConstraints = "primary keys, unique keys, foreign keys and CHECK constraints: a privilege beyond SELECT"
)

// constraintLines is the line MariaDB adds, and MySQL has no use for.
func constraintLines(s server) []string {
	if s.mariaDB {
		return []string{lineConstraints}
	}
	return nil
}

// TestSchemaDiffRefusesWhatTheAccountCannotSeeE2E is the table of
// stokaro/ptah#5497: each account that loses part of a comparison is refused at
// exit 2, naming what it lacks, where it was told `Schemas are synced`.
func TestSchemaDiffRefusesWhatTheAccountCannotSeeE2E(t *testing.T) {
	rows := []struct {
		name   string
		grants []string
		lines  func(server) []string
	}{
		{
			name:   "SELECT on one table",
			grants: []string{"GRANT SELECT ON {db}.child TO {account}"},
			lines: func(s server) []string {
				return append([]string{lineListing, lineDefinitions, lineTriggers, s.routineLine}, constraintLines(s)...)
			},
		},
		{
			name:   "SELECT on one column",
			grants: []string{"GRANT SELECT (id) ON {db}.child TO {account}"},
			lines: func(s server) []string {
				return append([]string{lineListing, lineTriggers, s.routineLine}, constraintLines(s)...)
			},
		},
		{
			name:   "SELECT on the database",
			grants: []string{"GRANT SELECT ON {db}.* TO {account}"},
			lines: func(s server) []string {
				return append([]string{lineDefinitions, lineTriggers, s.routineLine}, constraintLines(s)...)
			},
		},
		{
			name:   "SELECT, SHOW VIEW, TRIGGER on the database",
			grants: []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}"},
			lines:  func(s server) []string { return []string{s.routineLine} },
		},
		{
			name:   "an application account: SELECT, INSERT, UPDATE, DELETE on the database",
			grants: []string{"GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO {account}"},
			lines: func(s server) []string {
				return []string{lineDefinitions, lineTriggers, s.routineLine}
			},
		},
		{
			name:   "a wildcard grant a literal grant hides",
			grants: []string{"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE ON " + everyDatabase + ".* TO {account}", "GRANT SELECT ON {literal}.* TO {account}"},
			lines: func(s server) []string {
				return append([]string{lineDefinitions, lineTriggers, s.routineLine}, constraintLines(s)...)
			},
		},
		{
			name:   "TRIGGER on one table of several",
			grants: []string{"GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", "GRANT TRIGGER ON {db}.child TO {account}"},
			lines:  func(s server) []string { return []string{lineTriggers, s.routineLine} },
		},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			for i, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					account := f.newAccount(c, fmt.Sprintf("r%d", i), row.grants...)
					got := f.diff(c, account)
					assertRefused(c, got, 2, named(f.left, row.lines(s)...)...)
					c.Assert(got.Stdout, qt.Equals, "")
				})
			}
		})
	}
}

// TestSchemaDiffAnswersForAnAccountThatSeesEverythingE2E is the other half: each
// of these accounts gets, byte for byte, what the administrator gets.
func TestSchemaDiffAnswersForAnAccountThatSeesEverythingE2E(t *testing.T) {
	rows := []struct {
		name   string
		grants func(server, fixture) []string
	}{
		{
			name: "SELECT, SHOW VIEW, TRIGGER on the database and the routine privilege",
			grants: func(s server, _ fixture) []string {
				return []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant}
			},
		},
		{
			name: "those privileges globally",
			grants: func(server, fixture) []string {
				return []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON *.* TO {account}"}
			},
		},
		{
			name:   "ALL PRIVILEGES globally",
			grants: func(server, fixture) []string { return []string{"GRANT ALL PRIVILEGES ON *.* TO {account}"} },
		},
		{
			name: "a wildcard database grant and the routine privilege",
			grants: func(s server, _ fixture) []string {
				return []string{
					"GRANT SELECT, SHOW VIEW, TRIGGER ON " + everyDatabase + ".* TO {account}",
					strings.ReplaceAll(s.routineGrant, "{db}", everyDatabase),
				}
			},
		},
		{
			name: "TRIGGER granted table by table on every table",
			grants: func(s server, f fixture) []string {
				return []string{
					"GRANT SELECT, SHOW VIEW ON {db}.* TO {account}",
					"GRANT TRIGGER ON {db}.child TO {account}",
					"GRANT TRIGGER ON {db}.parent TO {account}",
					"GRANT TRIGGER ON `" + f.left + "`.only_left TO {account}",
					s.routineGrant,
				}
			},
		},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			administrator := f.diff(c, "")
			for i, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					account := f.newAccount(c, fmt.Sprintf("s%d", i), row.grants(s, f)...)
					assertSameAsAdministrator(c, f.diff(c, account), administrator)
				})
			}
		})
	}
}

// TestAllPrivilegesOnTheDatabaseE2E covers the account most deployments connect
// as. It holds everything a database-level grant can hold, and on MySQL that does
// not reach the body of another definer's routine, which SHOW_ROUTINE guards
// globally. MariaDB's ALL includes SHOW CREATE ROUTINE.
func TestAllPrivilegesOnTheDatabaseE2E(t *testing.T) {
	const all = "GRANT ALL PRIVILEGES ON {db}.* TO {account}"

	for _, s := range servers() {
		t.Run(s.name+", with no routine", func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			account := f.newAccount(c, "all", all)
			assertSameAsAdministrator(c, f.diff(c, account), f.diff(c, ""))
		})
	}

	t.Run("MySQL, with a routine of another definer", func(t *testing.T) {
		c := qt.New(t)
		s := servers()[0]
		f := newFixture(c, s, leftProcedure...)
		account := f.newAccount(c, "allr", all)
		assertRefused(c, f.diff(c, account), 2, "the bodies of routines p_noop, unless the account is their definer: SHOW_ROUTINE on *.*")
	})

	t.Run("MariaDB, with a routine of another definer", func(t *testing.T) {
		c := qt.New(t)
		s := servers()[1]
		f := newFixture(c, s, leftProcedure...)
		account := f.newAccount(c, "allr", all)
		assertSameAsAdministrator(c, f.diff(c, account), f.diff(c, ""))
	})
}

// TestTheContinuousIntegrationAccountAnswersE2E reads as the account the whole
// contour connects as, on databases its grant on `ptah\_rt\_%` covers. A refusal
// here would stop every live test that compares.
func TestTheContinuousIntegrationAccountAnswersE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			test := dbtarget.URL(c, s.test)
			from := test[:strings.LastIndex(test, "/")+1] + f.left
			to := test[:strings.LastIndex(test, "/")+1] + f.right
			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "schema", "diff", "--from", from, "--to", to)
			assertSameAsAdministrator(c, got, f.diff(c, ""))
		})
	}
}

// TestAGrantARoleCarriesCountsWhileTheRoleIsActiveE2E covers both engines' way of
// printing a role's privileges in SHOW GRANTS.
func TestAGrantARoleCarriesCountsWhileTheRoleIsActiveE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name+", a default role", func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			role := f.newRole(c, "read")
			f.exec(c, grantsOn(f, "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO "+role, roleGrant(s, role))...)
			account := f.newAccount(c, "dflt", "GRANT "+role+" TO {account}")
			f.exec(c, defaultRole(s, role, account))
			assertSameObjects(c, f.diff(c, account), f.diff(c, ""))
		})
		t.Run(s.name+", a role carrying a role", func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			inner := f.newRole(c, "inner")
			outer := f.newRole(c, "outer")
			f.exec(c, grantsOn(f, "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO "+inner, roleGrant(s, inner), "GRANT "+inner+" TO "+outer)...)
			account := f.newAccount(c, "nest", "GRANT "+outer+" TO {account}")
			f.exec(c, defaultRole(s, outer, account))
			assertSameObjects(c, f.diff(c, account), f.diff(c, ""))
		})
		t.Run(s.name+", a role that is not set", func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			role := f.newRole(c, "unset")
			f.exec(c, grantsOn(f, "GRANT TRIGGER ON {db}.* TO "+role)...)
			account := f.newAccount(c, "off",
				"GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant, "GRANT "+role+" TO {account}")
			assertRefused(c, f.diff(c, account), 2, named(f.left, lineTriggers)...)
		})
	}
}

// assertSameObjects is [assertSameAsAdministrator] for a fixture that grants to a
// role. The administrator reads those grants from mysql.user and sees that they
// name different databases; an account without SELECT on it records roles as not
// described, which is outside what these tests measure.
func assertSameObjects(c *qt.C, got, administrator clirun.Result) {
	c.Helper()
	administrator.Stdout = withoutGrants(administrator.Stdout)
	assertSameAsAdministrator(c, got, administrator)
}

func withoutGrants(stdout string) string {
	var kept strings.Builder
	for line := range strings.Lines(stdout) {
		if !strings.HasPrefix(line, "GRANT ") && !strings.HasPrefix(line, "REVOKE ") {
			kept.WriteString(line)
		}
	}
	return kept.String()
}

// roleGrant is the routine privilege for role.
func roleGrant(s server, role string) string {
	return strings.ReplaceAll(s.routineGrant, "{account}", role)
}

// grantsOn writes each of statements once per database of the fixture.
func grantsOn(f fixture, statements ...string) []string {
	var written []string
	for _, statement := range statements {
		written = append(written,
			strings.ReplaceAll(statement, "{db}", "`"+f.left+"`"),
			strings.ReplaceAll(statement, "{db}", "`"+f.right+"`"))
	}
	return written
}

// defaultRole is the engine's statement making role active when account logs in.
func defaultRole(s server, role, account string) string {
	statement := strings.ReplaceAll(s.setDefaultRole, "{role}", role)
	return strings.ReplaceAll(statement, "{account}", "'"+account+"'@'%'")
}

// TestEveryVerbThatReadsAnswersTheSameE2E runs each command that reads through the
// same path, refused as an account that cannot see and answering as one that can.
// A write is refused for the same reason: an apply that believes an object is
// absent plans from that belief.
func TestEveryVerbThatReadsAnswersTheSameE2E(t *testing.T) {
	type verb struct {
		name string
		args func(url, schemaFile, dir string) []string
	}
	verbs := []verb{
		{name: "schema compare", args: func(u, file, _ string) []string {
			return []string{"schema", "compare", "--db-url", u, "--schema-file", file}
		}},
		{name: "schema drift", args: func(u, file, _ string) []string {
			return []string{"schema", "drift", "--db-url", u, "--schema-file", file}
		}},
		{name: "schema drift as a document", args: func(u, file, _ string) []string {
			return []string{"schema", "drift", "--db-url", u, "--schema-file", file, "--format", "json"}
		}},
		{name: "schema inspect", args: func(u, _, _ string) []string {
			return []string{"schema", "inspect", "--db-url", u, "--format", "sql"}
		}},
		{name: "db read", args: func(u, _, _ string) []string { return []string{"db", "read", "--db-url", u} }},
		{name: "schema apply", args: func(u, file, _ string) []string {
			return []string{"schema", "apply", "--db-url", u, "--schema-file", file, "--dry-run"}
		}},
		{name: "schema plan", args: func(u, file, _ string) []string {
			return []string{"schema", "plan", "--db-url", u, "--schema-file", file, "--dry-run"}
		}},
		{name: "migrations generate", args: func(u, file, dir string) []string {
			return []string{"migrations", "generate", "--db-url", u, "--schema-file", file, "--migrations-dir", dir, "--name", "m"}
		}},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := filepath.Join(c.TempDir(), "desired.hcl")
			inspected := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "inspect", "--db-url", f.url("", f.left), "--format", "hcl")
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(os.WriteFile(file, []byte(inspected.Stdout), 0o600), qt.IsNil)
			blind := f.newAccount(c, "blind", "GRANT SELECT ON {db}.* TO {account}")
			sighted := f.newAccount(c, "sight", "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant)

			for _, verb := range verbs {
				t.Run(verb.name+", refused", func(t *testing.T) {
					c := qt.New(t)
					got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, verb.args(f.url(blind, f.left), file, c.TempDir())...)
					assertRefused(c, got, 2, named(f.left, lineTriggers, s.routineLine)...)
				})
				t.Run(verb.name+", answered", func(t *testing.T) {
					c := qt.New(t)
					got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, verb.args(f.url(sighted, f.left), file, c.TempDir())...)
					administrator := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, verb.args(f.url("", f.left), file, c.TempDir())...)
					c.Assert(got.ExitCode, qt.Equals, administrator.ExitCode, qt.Commentf("stderr:\n%s", got.Stderr))
				})
			}
		})
	}
}

// TestTheCompatibilitySurfaceRefusesTooE2E holds ptah-compat to the same answer at
// the exit code its surface has. The pinned community binary answers `Schemas are
// synced` at exit 0 for the same account; docs/conformance.md records the
// difference.
func TestTheCompatibilitySurfaceRefusesTooE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			account := f.newAccount(c, "compat", "GRANT SELECT ON {db}.* TO {account}")
			left, right := atlasURL(c, f, account, f.left), atlasURL(c, f, account, f.right)
			commands := []struct {
				args     []string
				database string
			}{
				{args: []string{"schema", "diff", "--from", left, "--to", right}, database: f.left},
				{args: []string{"schema", "inspect", "-u", left}, database: f.left},
				{args: []string{"schema", "apply", "-u", left, "--to", right, "--dry-run"}, database: f.right},
			}
			for _, command := range commands {
				t.Run(strings.Join(command.args[:2], " "), func(t *testing.T) {
					c := qt.New(t)
					got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, command.args...)
					assertRefused(c, got, 1, named(command.database, lineTriggers, s.routineLine)...)
				})
			}
		})
	}
}

// atlasURL is the fixture's address in the host:port form the Atlas URL takes.
func atlasURL(c *qt.C, f fixture, account, database string) string {
	c.Helper()
	address := f.url(account, database)
	opened, closed := strings.Index(address, "@tcp("), strings.LastIndex(address, ")/")
	c.Assert(opened >= 0 && closed > opened, qt.IsTrue, qt.Commentf("%s", address))
	return address[:opened+1] + address[opened+len("@tcp("):closed] + address[closed+1:]
}

// TestARunThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E covers a scoped run. It
// depends on nothing the account cannot see, so it gets what the administrator
// gets from the same selector, and an account that cannot see the rest is still
// refused.
func TestARunThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E(t *testing.T) {
	rows := []struct {
		name    string
		grants  func(server) []string
		exclude string
	}{
		{
			name: "no TRIGGER, every trigger excluded",
			grants: func(s server) []string {
				return []string{"GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant}
			},
			exclude: "*[type=trigger]",
		},
		{
			name: "no SHOW VIEW, every view excluded",
			grants: func(s server) []string {
				return []string{"GRANT SELECT, INSERT, TRIGGER ON {db}.* TO {account}", s.routineGrant}
			},
			exclude: "*[type=view]",
		},
		{
			name: "no routine privilege, every routine excluded",
			grants: func(server) []string {
				return []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}"}
			},
			exclude: "*[type=function]",
		},
		{
			name:    "SELECT alone, every kind it cannot see excluded",
			grants:  func(server) []string { return []string{"GRANT SELECT ON {db}.* TO {account}"} },
			exclude: "*[type=trigger|view|function|constraint]",
		},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			for i, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					account := f.newAccount(c, fmt.Sprintf("x%d", i), row.grants(s)...)
					assertSameAsAdministrator(c, f.diff(c, account, "--exclude", row.exclude), f.diff(c, "", "--exclude", row.exclude))
				})
			}
			t.Run("a selector naming one trigger vouches for no other", func(t *testing.T) {
				c := qt.New(t)
				account := f.newAccount(c, "named", "GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant)
				assertRefused(c, f.diff(c, account, "--exclude", "child_trg*[type=trigger]"), 2, named(f.left, lineTriggers)...)
			})
			t.Run("diff.skip leaves the kind in the comparison", func(t *testing.T) {
				c := qt.New(t)
				account := f.newAccount(c, "skip", "GRANT SELECT ON {db}.* TO {account}")
				config := filepath.Join(c.TempDir(), "ptah.yaml")
				c.Assert(os.WriteFile(config, []byte("diff:\n  skip: [drop_table, drop_column, drop_index, drop_enum]\n"), 0o600), qt.IsNil)
				assertRefused(c, f.diff(c, account, "--config", config), 2, named(f.left, lineTriggers)...)
			})
		})
	}
}

// TestAWholeServerReadAnswersForTheDatabasesItSeesE2E covers a URL that names no
// database. It covers the databases the account can see, and each of those is held
// to the same answer.
func TestAWholeServerReadAnswersForTheDatabasesItSeesE2E(t *testing.T) {
	inspect := func(c *qt.C, address string, flags ...string) clirun.Result {
		c.Helper()
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
			append([]string{"schema", "inspect", "--db-url", address, "--format", "sql"}, flags...)...)
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)

			t.Run("an account that sees all of what it lists", func(t *testing.T) {
				c := qt.New(t)
				account := f.newAccount(c, "server", "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant)
				got := inspect(c, f.url(account, ""))
				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "child_trg")
				c.Assert(got.Stdout, qt.Contains, f.right)
			})
			t.Run("an account that does not", func(t *testing.T) {
				c := qt.New(t)
				account := f.newAccount(c, "partial", "GRANT SELECT ON {db}.* TO {account}")
				assertRefused(c, inspect(c, f.url(account, "")), 2, named(f.left, lineTriggers, s.routineLine)...)
			})
			t.Run("the database it names, leaving out the one it cannot see all of", func(t *testing.T) {
				c := qt.New(t)
				left, right := "`"+f.left+"`", "`"+f.right+"`"
				account := f.newAccount(c, "named",
					"GRANT SELECT, SHOW VIEW, TRIGGER ON "+left+".* TO {account}",
					strings.ReplaceAll(s.routineGrant, "{db}", left),
					"GRANT SELECT ON "+right+".* TO {account}")
				got := inspect(c, f.url(account, ""), "--schemas", f.left)
				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "child_trg")
				assertRefused(c, inspect(c, f.url(account, "")), 2, named(f.right, lineTriggers)...)
			})
		})
	}
}

// TestADevReadAnswersForWhatTheRunCreatedE2E covers the dev database. What a read
// finds there, the account created in the same run, so a privilege that shows
// someone else's trigger or routine proves nothing. An account lacking TRIGGER and
// SHOW VIEW and holding no routine privilege gets the administrator's answer on a
// dev server, where it holds no global cleanup privilege either.
func TestADevReadAnswersForWhatTheRunCreatedE2E(t *testing.T) {
	const desired = `schema "ptah_rt_acct_dev" {}

table "t" {
  schema = schema.ptah_rt_acct_dev
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, s.dev))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			f := fixture{admin: admin, stamp: time.Now().UnixNano()}
			account := f.newAccount(c, "dev", "GRANT SELECT, CREATE, DROP, ALTER, INDEX, REFERENCES, INSERT ON *.* TO {account}")

			file := filepath.Join(c.TempDir(), "desired.hcl")
			c.Assert(os.WriteFile(file, []byte(desired), 0o600), qt.IsNil)
			read := func(c *qt.C, devURL string) clirun.Result {
				c.Helper()
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "inspect", "--schema-file", file, "--dev-url", devURL, "--format", "sql")
			}
			administrator := read(c, dbtarget.URL(c, s.dev))
			got := read(c, asAccount(dbtarget.URL(c, s.dev), account, accountPassword))

			c.Assert(administrator.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", administrator.Stderr))
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(got.Stdout, qt.Equals, administrator.Stdout)
		})
	}
}

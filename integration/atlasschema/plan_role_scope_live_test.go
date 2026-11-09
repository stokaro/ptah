//go:build integration

package atlasschema_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// A saved plan's source fingerprint covers the role memberships the plan
// depends on and no others. The direction a fake cannot be wrong about is the
// one a server decides: which edges its own catalog reports after a GRANT, and
// whether the two reads of that catalog still agree. Every row therefore changes
// the graph on a live server and asks VerifyPlanTarget.
//
// roleScopeEdge names a membership by the fixture's short role names.
type roleScopeEdge struct{ group, member string }

// roleScopeRow is one change to the graph. The change is undone after the row,
// so the rows of one engine share one fixture and one plan.
type roleScopeRow struct {
	name string
	// users are accounts the change creates, and removes again.
	users []string
	// grant and revoke are the memberships it adds and takes away.
	grant, revoke []roleScopeEdge
}

// The fixture's graph is:
//
//	run -> rung -> runt   the role the plan runs as, and what it holds
//	sib                   a role that holds nothing
//	hold                  holds SELECT on the table t
//	pol                   a row-level security policy on t names this role
//	own                   owns the table o (PostgreSQL family)
//	low -> mid -> named   members of the role the plan grants to, and that role
//	un, ung               two roles with no tie to the plan
//
// siblingRows pin the sideways step: a new member of a group the running role
// belongs to changes nothing the running role holds.
var (
	unrelatedRows = []roleScopeRow{
		{name: "a membership between two roles with no tie to the plan", grant: []roleScopeEdge{{"ung", "un"}}},
		{
			name:  "a rotation that creates a user and grants it a group with no tie",
			users: []string{"rot"},
			grant: []roleScopeEdge{{"ung", "rot"}},
		},
	}
	siblingRows = []roleScopeRow{
		{name: "another member joining a group the running role belongs to", grant: []roleScopeEdge{{"rung", "sib"}}},
	}
	planNamedRows = []roleScopeRow{
		{name: "a new member of the role the plan grants to", grant: []roleScopeEdge{{"named", "un"}}},
		{name: "the role the plan grants to joining a group", grant: []roleScopeEdge{{"ung", "named"}}},
		{name: "a new member through a group under the role the plan grants to", grant: []roleScopeEdge{{"mid", "un"}}},
		{name: "a new member through two groups under the role the plan grants to", grant: []roleScopeEdge{{"low", "un"}}},
		{name: "a member leaving the role the plan grants to", revoke: []roleScopeEdge{{"named", "mid"}}},
		{name: "a new member of a role holding a privilege in the read", grant: []roleScopeEdge{{"hold", "un"}}},
		{name: "a role holding a privilege in the read joining a group", grant: []roleScopeEdge{{"ung", "hold"}}},
	}
	runningRoleRows = []roleScopeRow{
		{name: "the running role joining a group", grant: []roleScopeEdge{{"ung", "run"}}},
		{name: "the running role leaving a group", revoke: []roleScopeEdge{{"rung", "run"}}},
		{name: "a group of the running role joining another", grant: []roleScopeEdge{{"ung", "rung"}}},
		{name: "a chain above the running role losing a link", revoke: []roleScopeEdge{{"runt", "rung"}}},
		{name: "the top of that chain joining another group", grant: []roleScopeEdge{{"ung", "runt"}}},
	}
	ownerRows = []roleScopeRow{
		{name: "a new member of an object's owner", grant: []roleScopeEdge{{"own", "un"}}},
		{name: "an object's owner joining a group", grant: []roleScopeEdge{{"ung", "own"}}},
		{name: "a new member of a role only a policy names", grant: []roleScopeEdge{{"pol", "un"}}},
	}
)

// roleScopeEngine is what differs between the engines: the spelling of each
// statement, and how a database and the role that reads it are reached. The
// templates take the fixture's full role names.
type roleScopeEngine struct {
	name string
	// server is the administrator's address.
	server dbtarget.Engine
	// reach returns the addresses of a database for the administrator and for
	// the role the plan runs as.
	reach func(c *qt.C, server, database, login string) (admin, running string)
	// dropDatabase removes the plan's database.
	dropDatabase string
	// createRole, createUser and createRunner make a role with no login, an
	// account, and the login the plan runs as.
	createRole, createUser, createRunner string
	// dropRole, dropUser and dropRunner remove them again.
	dropRole, dropUser, dropRunner string
	// grant and revoke take the group and then the member.
	grant, revoke string
	// edges are the memberships the fixture starts with.
	edges []roleScopeEdge
	// setup runs in the database, as the administrator, with the role names
	// available by their short names.
	setup []string
	// desired is the plan's desired state, with the same names.
	desired string
	// unchanged and changed are the rows this engine answers.
	unchanged, changed []roleScopeRow
}

// #nosec G101 -- the password of a login one test creates and drops.
const roleScopeRunnerPassword = "ptah-role-scope"

func roleScopeEngines() []roleScopeEngine {
	unchanged := slices.Concat(unrelatedRows, siblingRows)
	withRunner := []roleScopeEdge{{"named", "mid"}, {"mid", "low"}, {"rung", "run"}, {"runt", "rung"}}
	postgresFamily := roleScopeEngine{
		reach:        roleScopeReachPostgresFamily,
		dropDatabase: "DROP DATABASE IF EXISTS %s WITH (FORCE)",
		createRole:   "CREATE ROLE %s",
		createUser:   "CREATE ROLE %s LOGIN",
		createRunner: "CREATE ROLE %s LOGIN PASSWORD '" + roleScopeRunnerPassword + "'",
		dropRole:     "DROP ROLE IF EXISTS %s",
		dropUser:     "DROP ROLE IF EXISTS %s",
		dropRunner:   "DROP ROLE IF EXISTS %s",
		grant:        "GRANT %s TO %s",
		revoke:       "REVOKE %s FROM %s",
		edges:        withRunner,
		setup: []string{
			"CREATE TABLE t (id integer PRIMARY KEY)",
			"GRANT SELECT ON t TO {hold}",
			"CREATE TABLE o (id integer PRIMARY KEY)",
			"ALTER TABLE o OWNER TO {own}",
			"ALTER TABLE t ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY p ON t TO {pol} USING (true)",
		},
		desired: `CREATE TABLE t (id integer PRIMARY KEY);
CREATE TABLE o (id integer PRIMARY KEY);
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON t TO {pol} USING (true);
CREATE TABLE a (id integer PRIMARY KEY);
GRANT SELECT ON t TO {hold};
GRANT SELECT ON t TO {named};
`,
		unchanged: unchanged,
		changed:   slices.Concat(planNamedRows, runningRoleRows, ownerRows),
	}
	mysqlFamily := roleScopeEngine{
		reach:        roleScopeReachMySQLFamily,
		dropDatabase: "DROP DATABASE IF EXISTS %s",
		createRole:   "CREATE ROLE %s",
		createUser:   "CREATE USER %s",
		createRunner: "CREATE USER %s IDENTIFIED BY '" + roleScopeRunnerPassword + "'",
		dropRole:     "DROP ROLE IF EXISTS %s",
		dropUser:     "DROP USER IF EXISTS %s",
		dropRunner:   "DROP USER IF EXISTS %s",
		grant:        "GRANT %s TO %s",
		revoke:       "REVOKE %s FROM %s",
		edges:        withRunner,
		setup: []string{
			"CREATE TABLE t (id INT PRIMARY KEY)",
			"GRANT ALL ON {database}.* TO {run}",
			"GRANT SELECT ON mysql.* TO {run}",
			"GRANT SELECT ON {database}.t TO {hold}",
		},
		desired: `CREATE TABLE t (id INT PRIMARY KEY);
CREATE TABLE a (id INT PRIMARY KEY);
GRANT SELECT ON t TO {hold};
GRANT SELECT ON t TO {named};
`,
		unchanged: unchanged,
		changed:   slices.Concat(planNamedRows, runningRoleRows),
	}

	postgres := postgresFamily
	postgres.name, postgres.server = "postgresql", dbtarget.PostgreSQL

	yugabyte := postgresFamily
	yugabyte.name, yugabyte.server = "yugabytedb", dbtarget.YugabyteDB

	// CockroachDB refuses a password on an insecure server, keeps WITH (FORCE)
	// to itself, and wants a new owner able to create in the schema.
	cockroach := postgresFamily
	cockroach.name, cockroach.server = "cockroachdb", dbtarget.CockroachDB
	cockroach.createRunner = "CREATE ROLE %s LOGIN"
	cockroach.dropDatabase = "DROP DATABASE IF EXISTS %s CASCADE"
	cockroach.setup = slices.Concat([]string{"GRANT CREATE ON SCHEMA public TO {own}"}, postgresFamily.setup)

	mysql := mysqlFamily
	mysql.name, mysql.server = "mysql", dbtarget.MySQLAdmin

	mariadb := mysqlFamily
	mariadb.name, mariadb.server = "mariadb", dbtarget.MariaDBAdmin

	sqlserver := roleScopeEngine{
		name:         "sqlserver",
		server:       dbtarget.SQLServer,
		reach:        roleScopeReachSQLServer,
		dropDatabase: "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s",
		createRole:   "CREATE ROLE %s",
		createUser:   "CREATE USER %s WITHOUT LOGIN",
		createRunner: "CREATE ROLE %s",
		dropRole:     "DROP ROLE IF EXISTS %s",
		dropUser:     "DROP USER IF EXISTS %s",
		dropRunner:   "DROP ROLE IF EXISTS %s",
		grant:        "ALTER ROLE %s ADD MEMBER %s",
		revoke:       "ALTER ROLE %s DROP MEMBER %s",
		edges:        []roleScopeEdge{{"named", "mid"}, {"mid", "low"}},
		setup: []string{
			"CREATE TABLE t (id INT PRIMARY KEY)",
			"GRANT SELECT ON t TO {hold}",
		},
		desired: `CREATE TABLE t (id INT PRIMARY KEY);
CREATE TABLE a (id INT PRIMARY KEY);
GRANT SELECT ON t TO {hold};
GRANT SELECT ON t TO {named};
`,
		unchanged: unrelatedRows,
		changed:   planNamedRows,
	}
	return []roleScopeEngine{postgres, yugabyte, cockroach, mysql, mariadb, sqlserver}
}

// roleScopeReachPostgresFamily replaces the database in the address, and the
// user for the role the plan runs as.
func roleScopeReachPostgresFamily(c *qt.C, server, database, login string) (admin, running string) {
	c.Helper()
	parsed, err := url.Parse(server)
	c.Assert(err, qt.IsNil)
	parsed.Path, parsed.RawPath = "/"+database, ""
	admin = parsed.String()
	parsed.User = url.UserPassword(login, roleScopeRunnerPassword)
	return admin, parsed.String()
}

// roleScopeReachMySQLFamily is the same over a driver DSN, which url.Parse
// refuses.
func roleScopeReachMySQLFamily(c *qt.C, server, database, login string) (admin, running string) {
	c.Helper()
	scheme, dsn, found := strings.Cut(server, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = database
	admin = scheme + "://" + config.FormatDSN()
	config.User, config.Passwd = login, roleScopeRunnerPassword
	return admin, scheme + "://" + config.FormatDSN()
}

// roleScopeReachSQLServer names the database in the query. A database role lives
// in the database and has no login of its own, so the plan reads as the
// administrator, whose database user is dbo and holds no membership.
func roleScopeReachSQLServer(c *qt.C, server, database, _ string) (admin, running string) {
	c.Helper()
	parsed, err := url.Parse(server)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String(), parsed.String()
}

// roleScopeFixture is one engine's graph, database and saved plan.
type roleScopeFixture struct {
	engine roleScopeEngine
	prefix string
	admin  *dbschema.DatabaseConnection
	conn   *dbschema.DatabaseConnection
	plan   atlasschema.PlanFile
}

// roleScopeShortNames are the fixture's roles other than the login.
var roleScopeShortNames = []string{"rung", "runt", "sib", "own", "hold", "pol", "named", "mid", "low", "un", "ung"}

// newRoleScopeFixture builds the graph, plans against it as the login, and
// asserts that the plan names the role it grants to and verifies as saved.
//
// A role belongs to a server several tests share, so every name is unique to
// this run, and the database is its own. A MySQL account name stops at 32
// characters, which leaves room for the prefix and the longest short name.
//
// Cleanups run last to first, so the one that drops the database and the roles
// is registered first and runs once the connections have closed. A server
// refuses to drop a role that still owns something, so the database goes first.
func newRoleScopeFixture(c *qt.C, ctx context.Context, engine roleScopeEngine) *roleScopeFixture {
	c.Helper()
	f := &roleScopeFixture{engine: engine, prefix: "rs" + strconv.FormatInt(time.Now().UnixNano(), 36)}
	database := f.prefix + "_db"

	address := dbtarget.URL(c, engine.server)
	// A connection of its own: YugabyteDB refuses DROP DATABASE on a session
	// whose catalog snapshot the fixture's role changes moved (SQLSTATE 40001).
	c.Cleanup(func() {
		server, err := dbschema.ConnectToDatabase(context.Background(), address)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(server)
		statements := []string{fmt.Sprintf(engine.dropDatabase, database), fmt.Sprintf(engine.dropRunner, f.name("run"))}
		for _, short := range roleScopeShortNames {
			statements = append(statements, fmt.Sprintf(engine.dropRole, f.name(short)))
		}
		for _, statement := range statements {
			_, err := server.ExecContext(context.Background(), statement)
			c.Check(err, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
	})
	server, err := dbschema.ConnectToDatabase(ctx, address)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(server)
	_, err = server.ExecContext(ctx, "CREATE DATABASE "+database)
	c.Assert(err, qt.IsNil)

	adminURL, runningURL := engine.reach(c, address, database, f.name("run"))
	f.admin, err = dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(f.admin) })

	for _, short := range roleScopeShortNames {
		c.Assert(f.exec(ctx, fmt.Sprintf(engine.createRole, f.name(short))), qt.IsNil)
	}
	c.Assert(f.exec(ctx, fmt.Sprintf(engine.createRunner, f.name("run"))), qt.IsNil)
	for _, edge := range engine.edges {
		c.Assert(f.exec(ctx, f.grant(edge)), qt.IsNil)
	}
	for _, statement := range engine.setup {
		statement = f.expand(statement, database)
		c.Assert(f.exec(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	f.conn, err = dbschema.ConnectToDatabase(ctx, runningURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(f.conn) })

	path := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(path, []byte(f.expand(engine.desired, database)), 0o600), qt.IsNil)
	f.plan, err = atlasschema.PreparePlanFile(ctx, f.conn, atlasschema.PlanFileOptions{
		ToURLs:  []string{"file://" + path},
		Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	statements := strings.Join(f.plan.StatementSQL(), "\n")
	c.Assert(statements, qt.Contains, f.name("named"),
		qt.Commentf("a plan that does not grant to the role does not name it"))
	c.Assert(statements, qt.Not(qt.Contains), f.name("pol"),
		qt.Commentf("a statement naming the role would hide what the read's own list does"))
	c.Assert(f.verify(ctx), qt.IsNil, qt.Commentf("a plan stale as saved cannot show a change being seen"))
	return f
}

func (f *roleScopeFixture) name(short string) string { return f.prefix + "_" + short }

func (f *roleScopeFixture) grant(edge roleScopeEdge) string {
	return fmt.Sprintf(f.engine.grant, f.name(edge.group), f.name(edge.member))
}

func (f *roleScopeFixture) revoke(edge roleScopeEdge) string {
	return fmt.Sprintf(f.engine.revoke, f.name(edge.group), f.name(edge.member))
}

func (f *roleScopeFixture) exec(ctx context.Context, statement string) error {
	_, err := f.admin.ExecContext(ctx, statement)
	return err
}

// expand replaces each {short} in text with the fixture's role of that name.
func (f *roleScopeFixture) expand(text, database string) string {
	text = strings.ReplaceAll(text, "{database}", database)
	for _, short := range slices.Concat([]string{"run"}, roleScopeShortNames) {
		text = strings.ReplaceAll(text, "{"+short+"}", f.name(short))
	}
	return text
}

func (f *roleScopeFixture) verify(ctx context.Context) error {
	return atlasschema.VerifyPlanTarget(ctx, f.conn, f.plan, must.Must(builtin.New()))
}

// wholeFingerprint fingerprints a read with every membership the server
// reports, which is what a change to the graph moves.
func (f *roleScopeFixture) wholeFingerprint(c *qt.C, ctx context.Context) string {
	c.Helper()
	current, err := dbschema.ReadSchemaWithSchemasContext(ctx, f.conn, nil)
	c.Assert(err, qt.IsNil)
	fingerprint, err := atlasschema.SchemaFingerprint(ctx, current, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return fingerprint
}

// change applies a row through the administrator and undoes it when the subtest
// ends, so a row that fails still leaves the next one its graph.
func (f *roleScopeFixture) change(c *qt.C, ctx context.Context, row roleScopeRow) {
	c.Helper()
	undo := func(statement string) {
		c.Cleanup(func() { c.Check(f.exec(context.Background(), statement), qt.IsNil) })
	}
	for _, user := range row.users {
		c.Assert(f.exec(ctx, fmt.Sprintf(f.engine.createUser, f.name(user))), qt.IsNil)
		undo(fmt.Sprintf(f.engine.dropUser, f.name(user)))
	}
	for _, edge := range row.grant {
		c.Assert(f.exec(ctx, f.grant(edge)), qt.IsNil)
		undo(f.revoke(edge))
	}
	for _, edge := range row.revoke {
		c.Assert(f.exec(ctx, f.revoke(edge)), qt.IsNil)
		undo(f.grant(edge))
	}
}

func TestPlanRoleScopeLive_AnUnrelatedMembershipKeepsThePlan(t *testing.T) {
	ctx := context.Background()
	for _, engine := range roleScopeEngines() {
		t.Run(engine.name, func(t *testing.T) {
			f := newRoleScopeFixture(qt.New(t), ctx, engine)
			for _, row := range engine.unchanged {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					before := f.wholeFingerprint(c, ctx)

					f.change(c, ctx, row)

					// The control: the read reports the change, so the plan stays
					// valid because of the scoping and not because nothing moved.
					c.Assert(f.wholeFingerprint(c, ctx), qt.Not(qt.Equals), before)
					c.Assert(f.verify(ctx), qt.IsNil)
				})
			}
		})
	}
}

// A server-wide role read includes accounts from other databases. Creating
// one must not replace a plan that neither names nor reaches that role.
func TestPlanRoleScopeLive_AnUnrelatedRoleKeepsThePlan(t *testing.T) {
	ctx := context.Background()
	for _, engine := range roleScopeEngines()[3:5] {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := newRoleScopeFixture(c, ctx, engine)
			before := f.wholeFingerprint(c, ctx)
			name := f.name("fresh")
			c.Assert(f.exec(ctx, fmt.Sprintf(engine.createRole, name)), qt.IsNil)
			c.Cleanup(func() { c.Check(f.exec(context.Background(), fmt.Sprintf(engine.dropRole, name)), qt.IsNil) })
			c.Assert(f.wholeFingerprint(c, ctx), qt.Not(qt.Equals), before)
			c.Assert(f.verify(ctx), qt.IsNil)
		})
	}
}

// PostgreSQL grants the creator ADMIN on a new role without giving it access
// to that role. The plan remains valid unless one of its statements names it.
func TestPlanRoleScopeLive_AdminOnlyCreatorMembershipKeepsThePlan(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	engine := roleScopePostgres()
	engine.createRunner = "CREATE ROLE %s LOGIN CREATEROLE PASSWORD '" + roleScopeRunnerPassword + "'"
	f := newRoleScopeFixture(c, ctx, engine)
	name := f.name("fresh")
	_, err := f.conn.ExecContext(ctx, "CREATE ROLE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(f.exec(context.Background(), fmt.Sprintf(engine.dropRole, name)), qt.IsNil) })
	current, err := dbschema.ReadSchemaWithSchemasContext(ctx, f.conn, nil)
	c.Assert(err, qt.IsNil)
	edgeAt := slices.IndexFunc(current.RoleMemberships, func(edge catalog.RoleMembership) bool {
		return edge.Role == name && edge.Member == f.name("run")
	})
	c.Assert(edgeAt >= 0, qt.IsTrue)
	edge := current.RoleMemberships[edgeAt]
	c.Assert(edge.AdminOption, qt.IsTrue)
	c.Assert(edge.InheritOption, qt.DeepEquals, new(false))
	c.Assert(edge.SetOption, qt.DeepEquals, new(false))
	c.Assert(f.verify(ctx), qt.IsNil)
}

// A change to the inheritance option changes the privileges available to the
// role that will apply the plan, even when the two role names stay the same.
func TestPlanRoleScopeLive_InheritOptionReplacesThePlan(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	f := newRoleScopeFixture(c, ctx, roleScopePostgres())
	grant := fmt.Sprintf("GRANT %s TO %s WITH INHERIT FALSE", f.name("rung"), f.name("run"))
	c.Assert(f.exec(ctx, grant), qt.IsNil)
	c.Cleanup(func() {
		c.Check(f.exec(context.Background(), fmt.Sprintf("GRANT %s TO %s WITH INHERIT TRUE", f.name("rung"), f.name("run"))), qt.IsNil)
	})
	var stale *atlasschema.StalePlanError
	c.Assert(f.verify(ctx), qt.ErrorAs, &stale)
}

func TestPlanRoleScopeLive_ARelatedMembershipReplacesThePlan(t *testing.T) {
	ctx := context.Background()
	for _, engine := range roleScopeEngines() {
		t.Run(engine.name, func(t *testing.T) {
			f := newRoleScopeFixture(qt.New(t), ctx, engine)
			for _, row := range engine.changed {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					// Undoing the row before this one brought the plan back, which
					// is what a REVOKE after a stale plan does.
					c.Assert(f.verify(ctx), qt.IsNil)

					f.change(c, ctx, row)

					var stale *atlasschema.StalePlanError
					c.Assert(f.verify(ctx), qt.ErrorAs, &stale)
				})
			}
		})
	}
}

// roleScopePostgres is the engine the tests below ask about one property of the
// plan rather than one engine's answer, on the server every run has.
func roleScopePostgres() roleScopeEngine {
	return roleScopeEngines()[0]
}

// A role belongs to the server and a grant to one database, so a group tied to
// one database moves the plans of that database and leaves those of another.
func TestPlanRoleScopeLive_AGroupTiedToOneDatabaseLeavesThePlansOfAnother(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	tied := newRoleScopeFixture(c, ctx, roleScopePostgres())
	other := newRoleScopeFixture(c, ctx, roleScopePostgres())

	tied.change(c, ctx, roleScopeRow{grant: []roleScopeEdge{{"hold", "un"}}})

	var stale *atlasschema.StalePlanError
	c.Assert(tied.verify(ctx), qt.ErrorAs, &stale)
	c.Assert(other.verify(ctx), qt.IsNil)
}

// The `.plan.hcl` shape records neither the statements' roles nor anything else
// about them, so its verification recomputes them from the statements. The
// writer adds a comment above each one, and the two halves have to agree anyway.
func TestPlanRoleScopeLive_AnAtlasFormatPlanRecomputesTheSameScope(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	f := newRoleScopeFixture(c, ctx, roleScopePostgres())

	document, err := atlasschema.MarshalPlanFileAs(f.plan, atlasschema.PlanFormatHCL)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "plan"+atlasschema.PlanFileSuffixHCL)
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	f.plan, _, err = atlasschema.ReadPlanDocument(ctx, must.Must(builtin.New()), path)
	c.Assert(err, qt.IsNil)
	c.Assert(f.verify(ctx), qt.IsNil)

	t.Run("an unrelated membership keeps it", func(t *testing.T) {
		c := qt.New(t)
		f.change(c, ctx, unrelatedRows[0])
		c.Assert(f.verify(ctx), qt.IsNil)
	})
	t.Run("a member of the role it grants to replaces it", func(t *testing.T) {
		c := qt.New(t)
		f.change(c, ctx, planNamedRows[0])
		var stale *atlasschema.StalePlanError
		c.Assert(f.verify(ctx), qt.ErrorAs, &stale)
	})
}

// A filter copies the description field by field, and a copy that loses the
// connected role reads as a read that cannot be narrowed.
func TestPlanRoleScopeLive_APlanWithAnExclusionIsScopedToo(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	f := newRoleScopeFixture(c, ctx, roleScopePostgres())

	path := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(path, []byte(f.expand(f.engine.desired, "")), 0o600), qt.IsNil)
	var err error
	f.plan, err = atlasschema.PreparePlanFile(ctx, f.conn, atlasschema.PlanFileOptions{
		ToURLs:  []string{"file://" + path},
		Exclude: []string{"public.left_out"},
		Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(f.verify(ctx), qt.IsNil)

	f.change(c, ctx, unrelatedRows[0])
	c.Assert(f.verify(ctx), qt.IsNil)
}

// An edit can name a role the plan did not. Its fingerprint then has to cover
// that role, or the edited plan refuses a database nothing moved in, and it has
// to stay what the planner recorded when the database did move.
func TestPlanRoleScopeLive_AnEditedPlanCoversTheRolesItNames(t *testing.T) {
	ctx := context.Background()
	runtime := must.Must(builtin.New())

	edit := func(c *qt.C, f *roleScopeFixture, statement string) atlasschema.PlanFile {
		c.Helper()
		edited, err := f.plan.WithStatementsFromSQL(ctx, runtime, f.plan.SQL()+statement)
		c.Assert(err, qt.IsNil)
		return edited
	}
	verify := func(f *roleScopeFixture, plan atlasschema.PlanFile) error {
		return atlasschema.VerifyPlanTarget(ctx, f.conn, plan, runtime)
	}
	var stale *atlasschema.StalePlanError

	t.Run("a new name is covered", func(t *testing.T) {
		c := qt.New(t)
		f := newRoleScopeFixture(c, ctx, roleScopePostgres())
		f.change(c, ctx, unrelatedRows[0])

		edited := edit(c, f, "GRANT SELECT ON t TO "+f.name("un")+";\n")
		c.Assert(verify(f, edited), qt.ErrorAs, &stale, qt.Commentf("without the rescoping the edit reads as a move"))

		rescoped, err := atlasschema.RescopeEditedPlan(ctx, f.conn, f.plan, edited, runtime)
		c.Assert(err, qt.IsNil)
		c.Assert(rescoped.FromFingerprint, qt.Not(qt.Equals), f.plan.FromFingerprint)
		c.Assert(rescoped.StatementSQL(), qt.DeepEquals, edited.StatementSQL())
		c.Assert(verify(f, rescoped), qt.IsNil)

		f.change(c, ctx, roleScopeRow{grant: []roleScopeEdge{{"un", "sib"}}})
		c.Assert(verify(f, rescoped), qt.ErrorAs, &stale)
	})

	t.Run("a removed name stops being covered", func(t *testing.T) {
		c := qt.New(t)
		f := newRoleScopeFixture(c, ctx, roleScopePostgres())

		edited, err := f.plan.WithStatementsFromSQL(ctx, runtime, f.plan.StatementSQL()[0]+";\n")
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Join(edited.StatementSQL(), "\n"), qt.Not(qt.Contains), f.name("named"))

		rescoped, err := atlasschema.RescopeEditedPlan(ctx, f.conn, f.plan, edited, runtime)
		c.Assert(err, qt.IsNil)
		c.Assert(verify(f, rescoped), qt.IsNil)

		f.change(c, ctx, planNamedRows[0])
		c.Assert(verify(f, rescoped), qt.IsNil)
	})

	t.Run("an edit naming no new role keeps the fingerprint", func(t *testing.T) {
		c := qt.New(t)
		f := newRoleScopeFixture(c, ctx, roleScopePostgres())

		edited := edit(c, f, "SELECT 1;\n")
		rescoped, err := atlasschema.RescopeEditedPlan(ctx, f.conn, f.plan, edited, runtime)
		c.Assert(err, qt.IsNil)
		c.Assert(rescoped.FromFingerprint, qt.Equals, f.plan.FromFingerprint)
	})

	t.Run("a comment naming a role names nothing", func(t *testing.T) {
		c := qt.New(t)
		f := newRoleScopeFixture(c, ctx, roleScopePostgres())
		f.change(c, ctx, unrelatedRows[0])

		edited, err := f.plan.WithStatementsFromSQL(ctx, runtime, "-- about "+f.name("un")+"\n"+f.plan.SQL())
		c.Assert(err, qt.IsNil)
		c.Assert(verify(f, edited), qt.IsNil)
	})

	t.Run("a database that moved during the edit stays refused", func(t *testing.T) {
		c := qt.New(t)
		f := newRoleScopeFixture(c, ctx, roleScopePostgres())
		f.change(c, ctx, planNamedRows[0])

		rescoped, err := atlasschema.RescopeEditedPlan(ctx, f.conn, f.plan, edit(c, f, "SELECT 1;\n"), runtime)
		c.Assert(err, qt.IsNil)
		c.Assert(rescoped.FromFingerprint, qt.Equals, f.plan.FromFingerprint)
		c.Assert(verify(f, rescoped), qt.ErrorAs, &stale)
	})
}

// A plan covers the memberships of the role it was computed as. Applied as
// another role, it verifies only when neither role holds a membership the rest
// of the plan does not already cover, and the refusal says who is running it,
// because the database may not have changed at all.
func TestPlanRoleScopeLive_AnotherRoleApplyingThePlan(t *testing.T) {
	ctx := context.Background()
	c := qt.New(t)
	f := newRoleScopeFixture(c, ctx, roleScopePostgres())
	address := dbtarget.URL(c, f.engine.server)
	database := f.prefix + "_db"

	login := func(c *qt.C, short string) *dbschema.DatabaseConnection {
		c.Helper()
		c.Assert(f.exec(ctx, fmt.Sprintf(f.engine.createRunner, f.name(short))), qt.IsNil)
		c.Cleanup(func() {
			c.Check(f.exec(context.Background(), fmt.Sprintf(f.engine.dropRunner, f.name(short))), qt.IsNil)
		})
		_, running := f.engine.reach(c, address, database, f.name(short))
		conn, err := dbschema.ConnectToDatabase(ctx, running)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
		return conn
	}

	// Planned by a role that holds nothing, so the plan's fingerprint covers
	// only what any reader of this database would.
	planner := login(c, "pln")
	path := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(path, []byte(f.expand(f.engine.desired, database)), 0o600), qt.IsNil)
	plan, err := atlasschema.PreparePlanFile(ctx, planner, atlasschema.PlanFileOptions{
		ToURLs:  []string{"file://" + path},
		Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)

	t.Run("a role that holds nothing the plan does not cover verifies it", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(atlasschema.VerifyPlanTarget(ctx, login(c, "app"), plan, must.Must(builtin.New())), qt.IsNil)
	})

	t.Run("a role holding a group of its own is refused, by name", func(t *testing.T) {
		c := qt.New(t)
		applier := login(c, "apg")
		f.change(c, ctx, roleScopeRow{grant: []roleScopeEdge{{"ung", "apg"}}})

		var stale *atlasschema.StalePlanError
		c.Assert(atlasschema.VerifyPlanTarget(ctx, applier, plan, must.Must(builtin.New())), qt.ErrorAs, &stale)
		c.Assert(stale.ConnectedRole, qt.Equals, f.name("apg"))
		c.Assert(stale.Error(), qt.Contains, `a role other than "`+f.name("apg")+`"`)
	})
}

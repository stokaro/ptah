//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlServerAccount is a whole server as one account sees it. The account
// holds privileges on the databases in names and on nothing else, so a run
// through url sees those that exist and no database another test keeps. The
// migrate verbs judge whether a server is clean by every database they can
// see, which is why they are driven through an account rather than through the
// shared server's administrator.
type mysqlServerAccount struct {
	url   string
	names map[string]string
	admin *sql.DB
}

// newMySQLServerAccount grants a new account every privilege on a database per
// role, each named uniquely, and creates the roles in existing. Before it
// returns it asserts the account sees exactly those, so a run that finds the
// server clean or dirty finds it so for the reason the test gives.
func newMySQLServerAccount(c *qt.C, engine dbtarget.Engine, roles, existing []string) mysqlServerAccount {
	c.Helper()
	family := newMySQLFamilyServer(c, engine)
	suffix := time.Now().UnixNano() % 1_000_000_000
	account := fmt.Sprintf("ptah_mig_%d", suffix)
	password := account + "_pw"
	createMySQLUser(c, c.Context(), family.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, account) })
	names := make(map[string]string, len(roles))
	for _, role := range roles {
		name := fmt.Sprintf("ptah_mig_%s_%d", role, suffix)
		names[role] = name
		_, err := family.admin.ExecContext(c.Context(),
			fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%%'", name, account))
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), family.admin, name) })
	}
	visible := make([]string, 0, len(existing))
	for _, role := range existing {
		createMySQLDatabase(c, c.Context(), family.admin, names[role])
		visible = append(visible, names[role])
	}
	server := mysqlServerAccount{
		url:   (&url.URL{Scheme: "mysql", User: url.UserPassword(account, password), Host: family.config.Addr, Path: "/"}).String(),
		names: names,
		admin: family.admin,
	}
	slices.Sort(visible)
	c.Assert(server.databasesSeenBy(c, account, password, family), qt.Equals, strings.Join(visible, ","))
	return server
}

// databasesSeenBy answers the user databases the account sees, in name order.
func (s mysqlServerAccount) databasesSeenBy(c *qt.C, account, password string, family mysqlFamilyServer) string {
	c.Helper()
	config := family.config.Clone()
	config.User, config.Passwd, config.DBName = account, password, ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var visible string
	err = db.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(SCHEMA_NAME ORDER BY SCHEMA_NAME), '')
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`).Scan(&visible)
	c.Assert(err, qt.IsNil)
	return visible
}

// exists reports whether the database a role names exists.
func (s mysqlServerAccount) exists(c *qt.C, role string) bool {
	c.Helper()
	var count int
	err := s.admin.QueryRowContext(c.Context(),
		`SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?`, s.names[role]).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

// tables answers the tables of the database a role names, in name order.
func (s mysqlServerAccount) tables(c *qt.C, role string) string {
	c.Helper()
	var tables string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME), '')
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`, s.names[role]).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

// revisions answers the versions the revision table in the database a role
// names records, in order.
func (s mysqlServerAccount) revisions(c *qt.C, role string) string {
	c.Helper()
	var versions string
	err := s.admin.QueryRowContext(c.Context(), fmt.Sprintf(
		"SELECT COALESCE(GROUP_CONCAT(version ORDER BY version), '') FROM `%s`.atlas_schema_revisions", s.names[role],
	)).Scan(&versions)
	c.Assert(err, qt.IsNil)
	return versions
}

// serverMigrationDir writes and hashes a migration directory for a whole
// server: the first file creates the app database and a table in it, outside a
// transaction, as a database cannot be created inside the one a file runs in;
// the second adds a table to it.
func serverMigrationDir(c *qt.C, app string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	files := map[string]string{
		"20260101000001_init.sql": "-- atlas:txmode none\n\nCREATE DATABASE `" + app + "`;\nCREATE TABLE `" + app + "`.t (id int PRIMARY KEY);\n",
		"20260101000002_more.sql": "CREATE TABLE `" + app + "`.u (id int PRIMARY KEY);\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// TestCompatMigrateApplyKeepsAServersRevisionsInADatabaseOfItsOwnE2E applies
// a migration directory to a whole server, as the pinned community binary
// v1.3.0 applies one, measured on MySQL 8.4.11 and MariaDB 11.8.9: the run
// creates the databases the files create, and records its revisions in a
// database of its own (stokaro/ptah#3789). `migrate status` reads that history
// back, and `migrate set` rewrites it. The revisions database is named with
// --revisions-schema, because the default, atlas_schema_revisions, is one name
// on a server every test shares.
func TestCompatMigrateApplyKeepsAServersRevisionsInADatabaseOfItsOwnE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "revs"}, nil)
			dir := serverMigrationDir(c, server.names["app"])
			args := []string{"--url", server.url, "--dir", "file://" + dir, "--revisions-schema", server.names["revs"]}

			pending, pendingErr := runCompatVerb(slices.Concat([]string{"migrate", "status"}, args)...)
			applied, appliedErr := runCompatVerb(slices.Concat([]string{"migrate", "apply"}, args)...)
			current, currentErr := runCompatVerb(slices.Concat([]string{"migrate", "status"}, args)...)

			c.Assert(pendingErr, qt.IsNil, qt.Commentf("%s", pending))
			c.Assert(appliedErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(currentErr, qt.IsNil, qt.Commentf("%s", current))
			c.Assert(pending, qt.Contains, "Migration Status: PENDING")
			c.Assert(current, qt.Contains, "Migration Status: OK")
			c.Assert(server.tables(c, "app"), qt.Equals, "t,u")
			c.Assert(server.revisions(c, "revs"), qt.Equals, "20260101000001,20260101000002")

			set, setErr := runCompatVerb(slices.Concat([]string{"migrate", "set", "20260101000001"}, args)...)

			c.Assert(setErr, qt.IsNil, qt.Commentf("%s", set))
			c.Assert(server.revisions(c, "revs"), qt.Equals, "20260101000001")
		})
	}
}

// TestCompatMigrateApplyAdoptsADirtyServerOnRequestE2E is the control for the
// refusals below: `--allow-dirty` applies to a server that holds another
// database, as it does on the pinned community binary.
func TestCompatMigrateApplyAdoptsADirtyServerOnRequestE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "revs", "other"}, []string{"other"})
			dir := serverMigrationDir(c, server.names["app"])

			out, err := runCompatVerb("migrate", "apply", "--url", server.url, "--dir", "file://"+dir,
				"--revisions-schema", server.names["revs"], "--allow-dirty")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(server.tables(c, "app"), qt.Equals, "t,u")
		})
	}
}

// mysqlServersNotClean are servers `migrate apply` refuses to adopt without
// --baseline or --allow-dirty. The pinned community binary v1.3.0 counts the
// bookkeeping database it creates before it looks, measured on MySQL 8.4.11
// and MariaDB 11.8.9: one other database reads `found multiple schemas: 2`,
// two read 3, and the bookkeeping database alone holding a table besides the
// revision table reads `found multiple tables: 2`.
var mysqlServersNotClean = []struct {
	name     string
	existing []string
	// tables are tables to create, each `role.table`.
	tables  []string
	wantErr string
}{
	{name: "one other database", existing: []string{"other"}, wantErr: "found multiple schemas: 2"},
	{name: "two other databases", existing: []string{"other", "second"}, wantErr: "found multiple schemas: 3"},
	{
		name:     "a bookkeeping database holding another table",
		existing: []string{"revs"},
		tables:   []string{"revs.junk"},
		wantErr:  "found multiple tables: 2",
	},
}

// TestCompatMigrateApplyRefusesAServerThatIsNotCleanE2E refuses each server
// above before any migration runs: the database the directory creates does
// not exist after the refusal.
func TestCompatMigrateApplyRefusesAServerThatIsNotCleanE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range mysqlServersNotClean {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				server := newMySQLServerAccount(c, engine.admin, []string{"app", "revs", "other", "second"}, test.existing)
				for _, table := range test.tables {
					role, name, _ := strings.Cut(table, ".")
					_, err := server.admin.ExecContext(c.Context(),
						fmt.Sprintf("CREATE TABLE `%s`.`%s` (id int)", server.names[role], name))
					c.Assert(err, qt.IsNil)
				}
				dir := serverMigrationDir(c, server.names["app"])

				out, err := runCompatVerb("migrate", "apply", "--url", server.url, "--dir", "file://"+dir,
					"--revisions-schema", server.names["revs"])

				c.Assert(err, qt.ErrorMatches, `(?s).*sql/migrate: connected database is not clean: `+
					regexp.QuoteMeta(test.wantErr)+`\. baseline version or allow-dirty is required.*`,
					qt.Commentf("%s", out))
				c.Assert(server.exists(c, "app"), qt.Equals, false)
			})
		}
	}
}

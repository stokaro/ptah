//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// devServerMigrationDir writes and hashes a migration directory for a whole
// dev server: the first file creates the app and second databases outside a
// transaction, as a database cannot be created inside the one a file runs
// in, and the second gives second a table with a foreign key into app. The
// key is what makes the order of the cleanup matter.
func devServerMigrationDir(c *qt.C, app, second string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	files := map[string]string{
		"20260101000001_init.sql": "-- atlas:txmode none\n\nCREATE DATABASE `" + app + "`;\n" +
			"CREATE TABLE `" + app + "`.t (id int PRIMARY KEY);\nCREATE DATABASE `" + second + "`;\n",
		"20260101000002_more.sql": "CREATE TABLE `" + second + "`.x (id int PRIMARY KEY, t_id int, " +
			"CONSTRAINT x_t FOREIGN KEY (t_id) REFERENCES `" + app + "`.t (id));\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// devServerMigrationDirWithThird is [devServerMigrationDir] with a third file
// that creates the third database and a table in it, for a --to that is a
// migration directory replayed on the dev server.
func devServerMigrationDirWithThird(c *qt.C, app, second, third string) string {
	c.Helper()
	dir := devServerMigrationDir(c, app, second)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000003_third.sql"), []byte(
		"-- atlas:txmode none\n\nCREATE DATABASE `"+third+"`;\nCREATE TABLE `"+third+"`.w (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// readMigrationNamed reads the one migration file in dir named name.
func readMigrationNamed(c *qt.C, dir, name string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*_"+name+".sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	body, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(body)
}

// devServerRealm declares app with its table, and third, which the directory
// does not create; second is left out.
func devServerRealm(c *qt.C, app, third string) string {
	c.Helper()
	realm := filepath.Join(c.TempDir(), "realm.hcl")
	c.Assert(os.WriteFile(realm, []byte(fmt.Sprintf(`schema %[1]q {}
schema %[2]q {}
table "t" {
  schema = schema.%[1]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "w" {
  schema = schema.%[2]s
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`, app, third)), 0o600), qt.IsNil)
	return realm
}

// TestCompatMigrateVerbsReplayOnADevServerE2E replays a directory on a whole
// dev server, a --dev-url naming no database, as the pinned community binary
// v1.3.0 replays one: measured on MySQL 8.4.11 and MariaDB 11.8.9, `migrate
// validate`, `migrate lint` and `migrate diff` run at exit 0 against an empty
// dev server and leave it empty (stokaro/ptah#3789). The directory keeps a
// foreign key from one database into another, where the binary's cleanup
// stops with error 3730 and leaves the dev server dirty. The dev server is
// the shared CI server seen through an account that sees no database, which
// is what an empty dev server is to the run.
func TestCompatMigrateVerbsReplayOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "second", "third"}, nil)
			dir := devServerMigrationDir(c, server.names["app"], server.names["second"])
			realm := devServerRealm(c, server.names["app"], server.names["third"])

			validated, validateErr := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", server.url)
			linted, lintErr := runCompatVerb("migrate", "lint", "--dir", "file://"+dir, "--dev-url", server.url, "--latest", "1")
			diffed, diffErr := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+realm,
				"--dev-url", server.url, "planned")
			// The first diff wrote into dir, so the second starts from a fresh one.
			fresh := devServerMigrationDir(c, server.names["app"], server.names["second"])
			replayed, replayedErr := runCompatVerb("migrate", "diff", "--dir", "file://"+fresh,
				"--to", "file://"+devServerMigrationDirWithThird(c, server.names["app"], server.names["second"], server.names["third"]),
				"--dev-url", server.url, "replayed")

			c.Assert(validateErr, qt.IsNil, qt.Commentf("%s", validated))
			c.Assert(lintErr, qt.IsNil, qt.Commentf("%s", linted))
			c.Assert(diffErr, qt.IsNil, qt.Commentf("%s", diffed))
			c.Assert(replayedErr, qt.IsNil, qt.Commentf("%s", replayed))
			planned := readMigrationNamed(c, dir, "planned")
			c.Assert(planned, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+server.names["third"]+"`")
			c.Assert(planned, qt.Contains, "DROP DATABASE `"+server.names["second"]+"`")
			c.Assert(readMigrationNamed(c, fresh, "replayed"), qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+server.names["third"]+"`")
			c.Assert(server.exists(c, "app"), qt.Equals, false)
			c.Assert(server.exists(c, "second"), qt.Equals, false)
		})
	}
}

// devServerNotClean are the verbs that refuse a dev server holding a user
// database, each with the prefix the pinned community binary v1.3.0 puts in
// front of the refusal, measured on MySQL 8.4.11 and MariaDB 11.8.9: the
// first database in byte order, empty or not, is named.
var devServerNotClean = []struct {
	name    string
	args    func(dir, dev, realm string) []string
	wantErr string
}{
	{
		name: "migrate validate",
		args: func(dir, dev, _ string) []string {
			return []string{"migrate", "validate", "--dir", "file://" + dir, "--dev-url", dev}
		},
		wantErr: `replaying the migration directory: sql/migrate: taking database snapshot: sql/migrate: connected database is not clean: found schema %q`,
	},
	{
		name: "migrate lint",
		args: func(dir, dev, _ string) []string {
			return []string{"migrate", "lint", "--dir", "file://" + dir, "--dev-url", dev, "--latest", "1"}
		},
		wantErr: `taking database snapshot: sql/migrate: connected database is not clean: found schema %q`,
	},
	{
		name: "migrate diff",
		args: func(dir, dev, realm string) []string {
			return []string{"migrate", "diff", "--dir", "file://" + dir, "--to", "file://" + realm, "--dev-url", dev, "refused"}
		},
		wantErr: `sql/migrate: taking database snapshot: sql/migrate: connected database is not clean: found schema %q`,
	},
}

// TestCompatMigrateVerbsRefuseADevServerThatIsNotCleanE2E refuses a dev
// server that holds a user database, and leaves the database where it is.
func TestCompatMigrateVerbsRefuseADevServerThatIsNotCleanE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range devServerNotClean {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				server := newMySQLServerAccount(c, engine.admin, []string{"app", "second", "third", "other"}, []string{"other"})
				dir := devServerMigrationDir(c, server.names["app"], server.names["second"])
				realm := devServerRealm(c, server.names["app"], server.names["third"])

				out, err := runCompatVerb(test.args(dir, server.url, realm)...)

				c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(fmt.Sprintf(test.wantErr, server.names["other"]))+`.*`,
					qt.Commentf("%s", out))
				c.Assert(server.exists(c, "other"), qt.Equals, true)
				c.Assert(server.exists(c, "app"), qt.Equals, false)
			})
		}
	}
}

// TestCompatMigrateDiffRefusesADevServerThatIsTheDesiredServerE2E refuses a
// dev server that is the server --to reads, before anything is reset. The two
// URLs log in as different accounts, so they share no database name; the
// server identity, its UUID on MySQL and its host name, port and data
// directory on MariaDB, says they are one server. The pinned community binary
// v1.3.0 compares no identity, and replays onto such a server when the
// account it uses sees it empty.
func TestCompatMigrateDiffRefusesADevServerThatIsTheDesiredServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newMySQLServerAccount(c, engine.admin, []string{"app", "second"}, nil)
			desired := newMySQLServerAccount(c, engine.admin, []string{"want"}, []string{"want"})
			dir := devServerMigrationDir(c, dev.names["app"], dev.names["second"])

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", desired.url,
				"--dev-url", dev.url, "refused")

			c.Assert(err, qt.ErrorMatches,
				`(?s).*--to database must differ from --dev-url because the dev database is reset during planning.*`,
				qt.Commentf("%s", out))
			c.Assert(desired.exists(c, "want"), qt.Equals, true)
			c.Assert(dev.exists(c, "app"), qt.Equals, false)
		})
	}
}

// TestCompatMigrateDiffRefusesOneDatabaseBesideADevServerE2E refuses a --to
// naming one database beside a dev server: the directory replays on the dev
// server as a whole server, and the one database would be compared with it as
// if the scopes were one.
func TestCompatMigrateDiffRefusesOneDatabaseBesideADevServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newMySQLServerAccount(c, engine.admin, []string{"app", "second"}, nil)
			desired := newMySQLFamilyScratch(c, engine.admin)
			name, url := desired.database(c, "want")
			dir := devServerMigrationDir(c, dev.names["app"], dev.names["second"])

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", url, "--dev-url", dev.url, "refused")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(fmt.Sprintf("cannot diff a schema %q with a database connection", name))+`.*`,
				qt.Commentf("%s", out))
		})
	}
}

// TestCompatMigrateValidateRefusesAUserOnADevServerE2E refuses a directory
// that creates a user on a dev server: the cleanup drops databases, and a
// user outlives it. The dev server is left empty.
func TestCompatMigrateValidateRefusesAUserOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app"}, nil)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20260101000001_init.sql"), []byte(
				"-- atlas:txmode none\n\nCREATE DATABASE `"+server.names["app"]+"`;\nCREATE USER 'ptah_replay_user'@'%';\n"), 0o600), qt.IsNil)
			hashed, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))

			out, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", server.url)

			c.Assert(err, qt.ErrorMatches, `(?s).*migration replay rejects CREATE USER.*`, qt.Commentf("%s", out))
			c.Assert(server.exists(c, "app"), qt.Equals, false)
		})
	}
}

// TestSchemaVerbsRefuseADevServerE2E refuses a dev server on schema diff and
// schema apply, which rehearse and materialize on one dev database and do not
// take a whole server yet (stokaro/ptah#3789). The refusal comes before
// anything is contacted, so a dev server holding a database gets this
// refusal and not the one for a dev database that is not clean.
func TestSchemaVerbsRefuseADevServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newMySQLServerAccount(c, engine.admin, []string{"app", "second", "third", "other"}, []string{"other"})
			target := newMySQLFamilyScratch(c, engine.admin)
			_, url := target.database(c, "target")
			dir := devServerMigrationDir(c, dev.names["app"], dev.names["second"])
			realm := devServerRealm(c, dev.names["app"], dev.names["third"])

			diffed, diffErr := runCompatVerb("schema", "diff", "--from", "file://"+dir, "--to", "file://"+realm, "--dev-url", dev.url)
			applied, applyErr := runCompatVerb("schema", "apply", "--url", url, "--to", "file://"+realm, "--dev-url", dev.url, "--dry-run")

			for _, err := range []error{diffErr, applyErr} {
				c.Assert(err, qt.ErrorMatches,
					`(?s).*a --dev-url naming no MySQL or MariaDB database is a whole dev server, which schema diff and schema apply do not take yet.*`,
					qt.Commentf("%s\n%s", diffed, applied))
			}
			c.Assert(dev.exists(c, "app"), qt.Equals, false)
		})
	}
}

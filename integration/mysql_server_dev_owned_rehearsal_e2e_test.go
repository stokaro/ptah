//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// schema apply rehearses a plan for a whole MySQL or MariaDB server on a whole
// dev server, a --dev-url naming no database, under the replay guard. On a dev
// server the operator named, the guard refuses what the reset of that server
// leaves behind, a stored body among it. On a server the run owns -- declared
// with PTAH_DEV_SERVER_DISPOSABLE=1, or started by a docker URL -- it runs
// what a migration replay there runs (stokaro/ptah#4060).

// ownedRehearsalEngines are the engines below, each with the image a
// `docker+` URL naming no database starts as a whole dev server. The images
// are the ones the integration job already runs as services.
var ownedRehearsalEngines = []struct {
	name, dialect string
	admin, dev    dbtarget.Engine
	dockerDevURL  string
}{
	{name: "MySQL", dialect: "mysql", admin: dbtarget.MySQLAdmin, dev: dbtarget.MySQLDevServer, dockerDevURL: "docker+mysql://_/mysql:26.7"},
	{name: "MariaDB", dialect: "mariadb", admin: dbtarget.MariaDBAdmin, dev: dbtarget.MariaDBDevServer, dockerDevURL: "docker+mariadb://_/mariadb:12.3"},
}

// storedBodyRealm writes a realm creating app with a table and a procedure
// that writes to it, and returns its path.
func storedBodyRealm(c *qt.C, app string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "realm.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE DATABASE `"+app+"`;\n"+
		"CREATE TABLE `"+app+"`.t (id int PRIMARY KEY);\n"+
		"CREATE PROCEDURE `"+app+"`.add_t(IN v int) BEGIN INSERT INTO `"+app+"`.t (id) VALUES (v); END;\n"), 0o600), qt.IsNil)
	return path
}

// storedRoutinesIn counts the routines of database on the server behind server.
func storedRoutinesIn(c *qt.C, server mysqlServerAccount, database string) int {
	c.Helper()
	var routines int
	c.Assert(server.admin.QueryRowContext(c.Context(),
		"SELECT count(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?", database).Scan(&routines), qt.IsNil)
	return routines
}

// TestSchemaApplyRefusesAStoredBodyOnANamedDevServerE2E rehearses the realm
// on a whole dev server the operator named. Both binaries refuse the
// procedure before the target is touched, and say how to make the server the
// run's own.
func TestSchemaApplyRefusesAStoredBodyOnANamedDevServerE2E(t *testing.T) {
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	for _, engine := range ownedRehearsalEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app"}, nil)
			app := server.names["app"]
			realm := storedBodyRealm(c, app)
			dev := dbtarget.URL(c, engine.dev)

			compat, compatErr := runCompatVerb("schema", "apply", "--url", server.url, "--to", "file://"+realm,
				"--dev-url", dev, "--auto-approve")
			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", server.url,
				"--schema-file", realm, "--dev-url", dev, "--auto-approve")

			refusal := `(?s).*statement \d+ cannot be rehearsed on a whole dev server: ` + engine.dialect +
				` migration replay rejects CREATE executable stored body because its effects cannot be confined to the ` +
				`disposable database realm; if nothing else uses this server, declare it disposable with ` +
				`PTAH_DEV_SERVER_DISPOSABLE=1, or use a docker:// or docker\+<driver>:// dev URL.*`
			c.Assert(compatErr, qt.ErrorMatches, refusal, qt.Commentf("%s", compat))
			c.Assert(nativeErr, qt.ErrorMatches, refusal, qt.Commentf("%s", native))
			c.Assert(databasesOn(c, engine.admin, app), qt.Equals, 0)
			c.Assert(databasesOn(c, engine.dev, app), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyRehearsesAStoredBodyOnADeclaredDevServerE2E is the control:
// the same dev server declared the run's own rehearses the procedure, and each
// binary applies it to a target of its own. The reset leaves the dev server
// empty.
func TestSchemaApplyRehearsesAStoredBodyOnADeclaredDevServerE2E(t *testing.T) {
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)
	for _, engine := range ownedRehearsalEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			compatServer := newMySQLServerAccount(c, engine.admin, []string{"app"}, nil)
			nativeServer := newMySQLServerAccount(c, engine.admin, []string{"app"}, nil)
			compatApp, nativeApp := compatServer.names["app"], nativeServer.names["app"]
			dev := dbtarget.URL(c, engine.dev)

			compat, compatErr := runCompatVerb("schema", "apply", "--url", compatServer.url,
				"--to", "file://"+storedBodyRealm(c, compatApp), "--dev-url", dev, "--auto-approve")
			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", nativeServer.url,
				"--schema-file", storedBodyRealm(c, nativeApp), "--dev-url", dev, "--auto-approve")

			c.Assert(compatErr, qt.IsNil, qt.Commentf("%s", compat))
			c.Assert(nativeErr, qt.IsNil, qt.Commentf("%s", native))
			c.Assert(storedRoutinesIn(c, compatServer, compatApp), qt.Equals, 1)
			c.Assert(storedRoutinesIn(c, nativeServer, nativeApp), qt.Equals, 1)
			c.Assert(databasesOn(c, engine.dev, compatApp, nativeApp), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyRehearsesAStoredBodyOnAProvisionedDevServerE2E starts the
// whole dev server from a `docker+` URL that names no database, which the
// pinned community binary v1.3.0 also plans a realm beside. The server is the
// run's own without the declaration, and the procedure is rehearsed and
// applied.
func TestSchemaApplyRehearsesAStoredBodyOnAProvisionedDevServerE2E(t *testing.T) {
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	for _, engine := range ownedRehearsalEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app"}, nil)
			app := server.names["app"]

			out, err := runCompatVerb("schema", "apply", "--url", server.url, "--to", "file://"+storedBodyRealm(c, app),
				"--dev-url", engine.dockerDevURL, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(storedRoutinesIn(c, server, app), qt.Equals, 1)
		})
	}
}

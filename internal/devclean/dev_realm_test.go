package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
)

// declareDisposable records rawURL as a server the operator declared the
// run's own, as a command does when PTAH_DEV_SERVER_DISPOSABLE is set, until
// the test ends.
func declareDisposable(c *qt.C, rawURL string) {
	c.Helper()
	_, release, err := devdocker.Resolve(c.Context(), rawURL, devdocker.Options{DeclaredDisposable: true})
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
}

// wholeMySQLDevServer and wholeMariaDBDevServer are whole dev servers, each
// reached by a URL of its own.
var (
	wholeMySQLDevServer   = catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true, URL: "mysql://root@dev-realm-mysql-server:3306/"}
	wholeMariaDBDevServer = catalog.ServerInfo{Dialect: platform.MariaDB, WholeServer: true, URL: "mariadb://root@dev-realm-mariadb-server:3306/"}
)

// devRealmServers are dev databases and whole dev servers, each reached by a
// URL of its own.
var devRealmServers = []struct {
	name string
	info catalog.ServerInfo
	// named is the realm on a server the operator named.
	named devclean.ReplayRealm
}{
	{
		name:  "PostgreSQL dev database",
		info:  catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://root@dev-realm-pg:5432/dev"},
		named: devclean.ReplayRealmDatabase,
	},
	{
		name:  "MySQL dev database",
		info:  catalog.ServerInfo{Dialect: platform.MySQL, Schema: "dev", URL: "mysql://root@dev-realm-mysql:3306/dev"},
		named: devclean.ReplayRealmDatabase,
	},
	{name: "whole MySQL dev server", info: wholeMySQLDevServer, named: devclean.ReplayRealmServerDatabases},
	{name: "whole MariaDB dev server", info: wholeMariaDBDevServer, named: devclean.ReplayRealmServerDatabases},
}

func TestDevReplayRealm_NamedServer(t *testing.T) {
	for _, test := range devRealmServers {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devclean.DevReplayRealm(test.info), qt.Equals, test.named)
		})
	}
}

// TestDevReplayRealm_DeclaredServer is every server above declared the run's
// own: the realm is the whole server, as it is on one a docker URL started,
// which the same record answers.
func TestDevReplayRealm_DeclaredServer(t *testing.T) {
	for _, test := range devRealmServers {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declareDisposable(c, test.info.URL)
			c.Assert(devclean.DevReplayRealm(test.info), qt.Equals, devclean.ReplayRealmServer)
		})
	}
}

// devGuardStoredBody is a statement the realm of a named whole server refuses
// and the server realm runs.
const devGuardStoredBody = "CREATE PROCEDURE `app`.`refresh`() BEGIN SELECT 1; END"

// TestNewDevReplayGuard_NamedServerNamesTheRemedy refuses a stored body on a
// whole server the operator named, and ends the refusal with the two ways to
// make the server the run's own.
func TestNewDevReplayGuard_NamedServerNamesTheRemedy(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewDevReplayGuard(wholeMySQLDevServer)

	err := guard.ValidateStatement(devGuardStoredBody)

	c.Assert(err, qt.ErrorMatches, `mysql migration replay rejects CREATE executable stored body because its effects `+
		`cannot be confined to the disposable database realm; if nothing else uses this server, declare it disposable `+
		`with PTAH_DEV_SERVER_DISPOSABLE=1, or use a docker:// or docker\+<driver>:// dev URL`)
}

// TestNewDevReplayGuard_DeclaredServerRunsWhatTheServerRealmRuns is the
// control: the same server declared the run's own runs the stored body, and
// still refuses a scheduled event, which the server realm keeps refusing.
func TestNewDevReplayGuard_DeclaredServerRunsWhatTheServerRealmRuns(t *testing.T) {
	c := qt.New(t)
	declareDisposable(c, wholeMariaDBDevServer.URL)
	guard := devclean.NewDevReplayGuard(wholeMariaDBDevServer)

	c.Assert(guard.ValidateStatement(devGuardStoredBody), qt.IsNil)
	c.Assert(guard.ValidateStatement("CREATE EVENT purge ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM app.t"),
		qt.ErrorMatches, `mariadb migration replay rejects CREATE executable stored body because its effects `+
			`cannot be confined to the disposable database realm`)
}

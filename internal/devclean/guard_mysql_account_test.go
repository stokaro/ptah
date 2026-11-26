package devclean_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// reachesTheAccount names a statement that reaches the account the run
// connects as, and accountRemedy is the one remedy its refusal ends with.
const (
	reachesTheAccount = " that reaches the account the run connects as"
	accountRemedy     = "; connect the dev URL as an account the history does not name"
)

// currentAccountStatements reach the account the run connects as by a
// spelling that names it whoever the session runs as, the operation each
// refusal names and the remedy it ends with. internal/devclean/mysqlreplay
// pins the accounts a session read names; these hold the refusal to the
// realms and remedies devclean gives it.
var currentAccountStatements = []struct {
	name, statement, operation, remedy string
}{
	{name: "DROP USER CURRENT_USER", statement: "DROP USER CURRENT_USER", operation: "DROP USER" + reachesTheAccount, remedy: accountRemedy},
	{name: "DROP USER CURRENT_USER()", statement: "DROP USER CURRENT_USER()", operation: "DROP USER" + reachesTheAccount, remedy: accountRemedy},
	{name: "DROP USER USER()", statement: "DROP USER USER()", operation: "DROP USER" + reachesTheAccount, remedy: accountRemedy},
	{name: "DROP USER in a list", statement: "DROP USER IF EXISTS 'app'@'%', current_user", operation: "DROP USER" + reachesTheAccount, remedy: accountRemedy},
	{name: "RENAME USER", statement: "RENAME USER CURRENT_USER TO 'moved'@'%'", operation: "RENAME USER"},
	{name: "REVOKE every privilege", statement: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER", operation: "REVOKE" + reachesTheAccount, remedy: accountRemedy},
	{name: "REVOKE global privileges", statement: "REVOKE ALL ON *.* FROM CURRENT_USER", operation: "REVOKE" + reachesTheAccount, remedy: accountRemedy},
	{name: "REVOKE a role", statement: "REVOKE 'app_role' FROM CURRENT_USER()", operation: "REVOKE" + reachesTheAccount, remedy: accountRemedy},
	{name: "REVOKE PROXY", statement: "REVOKE PROXY ON CURRENT_USER FROM 'app'@'%'", operation: "REVOKE" + reachesTheAccount, remedy: accountRemedy},
	{name: "REVOKE from PUBLIC", statement: "REVOKE SELECT ON *.* FROM PUBLIC", operation: "REVOKE" + reachesTheAccount, remedy: accountRemedy},
	{name: "DROP ROLE CURRENT_ROLE", statement: "DROP ROLE CURRENT_ROLE", operation: "DROP ROLE" + reachesTheAccount, remedy: accountRemedy},
	{name: "GRANT IDENTIFIED", statement: "GRANT USAGE ON *.* TO CURRENT_USER IDENTIFIED BY 'changed'", operation: "GRANT" + reachesTheAccount, remedy: accountRemedy},
	{name: "GRANT REQUIRE", statement: "GRANT USAGE ON *.* TO CURRENT_USER REQUIRE X509", operation: "GRANT" + reachesTheAccount, remedy: accountRemedy},
	{name: "GRANT a resource limit", statement: "GRANT USAGE ON *.* TO CURRENT_USER WITH MAX_USER_CONNECTIONS 1", operation: "GRANT" + reachesTheAccount, remedy: accountRemedy},
	{name: "ALTER USER", statement: "ALTER USER CURRENT_USER() IDENTIFIED BY 'changed'", operation: "ALTER USER"},
	{name: "SET PASSWORD", statement: "SET PASSWORD = 'changed'", operation: "SET PASSWORD"},
	{name: "SET DEFAULT ROLE", statement: "SET DEFAULT ROLE NONE FOR CURRENT_USER", operation: "SET DEFAULT ROLE"},
	{name: "account table written", statement: "DELETE FROM mysql.user WHERE User = 'app'", operation: `write to account table "mysql.user"`},
	{name: "MariaDB account table written", statement: "UPDATE mysql.global_priv SET Priv = '{}'", operation: `write to account table "mysql.global_priv"`},
	{name: "procedure body", statement: "CREATE PROCEDURE p() DROP USER CURRENT_USER", operation: "DROP USER" + reachesTheAccount, remedy: accountRemedy},
}

// TestReplayGuard_MySQLCurrentAccount_FailurePath pins that no realm lifts the
// refusal, and that none offers to own the server: owning it lifts nothing
// here.
func TestReplayGuard_MySQLCurrentAccount_FailurePath(t *testing.T) {
	realms := []struct {
		name  string
		realm devclean.ReplayRealm
	}{
		{name: "database", realm: devclean.ReplayRealmDatabase},
		{name: "server databases", realm: devclean.ReplayRealmServerDatabases},
		{name: "server", realm: devclean.ReplayRealmServer},
		{name: "provisioned server", realm: devclean.ReplayRealmProvisionedServer},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, realm := range realms {
			for _, test := range currentAccountStatements {
				t.Run(dialect+"/"+realm.name+"/"+test.name, func(t *testing.T) {
					c := qt.New(t)
					err := mysqlGuard(c, dialect, realm.realm).WithServerRealmRemedy(replayRemedy).ValidateStatement(test.statement)
					c.Assert(err, qt.ErrorMatches, reachRefusal(dialect, test.operation)+regexp.QuoteMeta(test.remedy))
				})
			}
		}
	}
}

// TestDevReplayGuard_MySQLCurrentAccount_FailurePathTrusted pins that trust
// keeps the refusal on every dev server an operator names, and that the
// refusal offers neither to trust the SQL nor to own the server, which lift
// nothing here. ErrorMatches anchors the whole message, so another remedy
// appended fails the row.
func TestDevReplayGuard_MySQLCurrentAccount_FailurePathTrusted(t *testing.T) {
	servers := []struct {
		name     string
		info     catalog.ServerInfo
		declared bool
		realm    devclean.ReplayRealm
	}{
		{
			name:  "dev database",
			info:  catalog.ServerInfo{Schema: "ptah_dev", URL: "mysql://dev@account-database:3306/ptah_dev"},
			realm: devclean.ReplayRealmDatabase,
		},
		{
			name:  "whole server",
			info:  catalog.ServerInfo{WholeServer: true, URL: "mysql://dev@account-whole-server:3306/"},
			realm: devclean.ReplayRealmServerDatabases,
		},
		{
			name:     "declared server",
			info:     catalog.ServerInfo{Schema: "ptah_dev", URL: "mysql://dev@account-declared:3306/ptah_dev"},
			declared: true,
			realm:    devclean.ReplayRealmServer,
		},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, server := range servers {
			t.Run(dialect+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				info := server.info
				info.Dialect = dialect
				_, release, err := devdocker.Resolve(replaytrust.With(c.Context(), true), info.URL,
					devdocker.Options{DeclaredDisposable: server.declared})
				c.Assert(err, qt.IsNil)
				c.Cleanup(release)
				c.Assert(devclean.DevReplayRealm(info), qt.Equals, server.realm)
				guard := forSession(c, devclean.NewDevReplayGuard(info))

				for _, test := range currentAccountStatements {
					t.Run(test.name, func(t *testing.T) {
						c := qt.New(t)
						c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches,
							reachRefusal(dialect, test.operation)+regexp.QuoteMeta(test.remedy))
					})
				}
			})
		}
	}
}

// TestDevReplayGuard_MySQLCurrentAccount_FailurePathUntrusted is the same on a
// dev server whose run does not trust the SQL: neither trust nor ownership is
// offered, since neither would lift the refusal.
func TestDevReplayGuard_MySQLCurrentAccount_FailurePathUntrusted(t *testing.T) {
	untrusted := []catalog.ServerInfo{
		{Dialect: platform.MySQL, Schema: "ptah_dev", URL: "mysql://dev@account-untrusted-mysql:3306/ptah_dev"},
		{Dialect: platform.MariaDB, Schema: "ptah_dev", URL: "mariadb://dev@account-untrusted-mariadb:3306/ptah_dev"},
	}
	for _, info := range untrusted {
		for _, test := range currentAccountStatements {
			t.Run(info.Dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				err := forSession(c, devclean.NewDevReplayGuard(info)).ValidateStatement(test.statement)
				c.Assert(err, qt.ErrorMatches, reachRefusal(info.Dialect, test.operation)+regexp.QuoteMeta(test.remedy))
			})
		}
	}
}

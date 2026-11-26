package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// currentAccountStatements reach the account the run connects as by a
// spelling that names it whoever the session runs as, and the operation each
// refusal names. internal/devclean/mysqlreplay pins the accounts a session
// read names; these hold the refusal to the realms and remedies devclean
// gives it.
var currentAccountStatements = []struct {
	name, statement, operation string
}{
	{name: "DROP USER CURRENT_USER", statement: "DROP USER CURRENT_USER", operation: "DROP USER that reaches the account the run connects as"},
	{name: "DROP USER CURRENT_USER()", statement: "DROP USER CURRENT_USER()", operation: "DROP USER that reaches the account the run connects as"},
	{name: "DROP USER USER()", statement: "DROP USER USER()", operation: "DROP USER that reaches the account the run connects as"},
	{name: "DROP USER in a list", statement: "DROP USER IF EXISTS 'app'@'%', current_user", operation: "DROP USER that reaches the account the run connects as"},
	{name: "RENAME USER", statement: "RENAME USER CURRENT_USER TO 'moved'@'%'", operation: "RENAME USER"},
	{name: "REVOKE every privilege", statement: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER", operation: "REVOKE that reaches the account the run connects as"},
	{name: "REVOKE global privileges", statement: "REVOKE ALL ON *.* FROM CURRENT_USER", operation: "REVOKE that reaches the account the run connects as"},
	{name: "REVOKE a role", statement: "REVOKE 'app_role' FROM CURRENT_USER()", operation: "REVOKE that reaches the account the run connects as"},
	{name: "REVOKE PROXY", statement: "REVOKE PROXY ON CURRENT_USER FROM 'app'@'%'", operation: "REVOKE that reaches the account the run connects as"},
	{name: "REVOKE from PUBLIC", statement: "REVOKE SELECT ON *.* FROM PUBLIC", operation: "REVOKE that reaches the account the run connects as"},
	{name: "DROP ROLE CURRENT_ROLE", statement: "DROP ROLE CURRENT_ROLE", operation: "DROP ROLE that reaches the account the run connects as"},
	{name: "GRANT IDENTIFIED", statement: "GRANT USAGE ON *.* TO CURRENT_USER IDENTIFIED BY 'changed'", operation: "GRANT that reaches the account the run connects as"},
	{name: "GRANT REQUIRE", statement: "GRANT USAGE ON *.* TO CURRENT_USER REQUIRE X509", operation: "GRANT that reaches the account the run connects as"},
	{name: "GRANT a resource limit", statement: "GRANT USAGE ON *.* TO CURRENT_USER WITH MAX_USER_CONNECTIONS 1", operation: "GRANT that reaches the account the run connects as"},
	{name: "ALTER USER", statement: "ALTER USER CURRENT_USER() IDENTIFIED BY 'changed'", operation: "ALTER USER"},
	{name: "SET PASSWORD", statement: "SET PASSWORD = 'changed'", operation: "SET PASSWORD"},
	{name: "SET DEFAULT ROLE", statement: "SET DEFAULT ROLE NONE FOR CURRENT_USER", operation: "SET DEFAULT ROLE"},
}

// TestReplayGuard_MySQLCurrentAccount_FailurePath pins that no realm lifts the
// refusal, and that none offers a remedy: owning the server does not.
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
					err := mysqlGuard(dialect, realm.realm).ValidateStatement(test.statement)
					c.Assert(err, qt.ErrorMatches, reachRefusal(dialect, test.operation))
				})
			}
		}
	}
}

// TestDevReplayGuard_MySQLCurrentAccount_FailurePathTrusted pins that trust
// keeps the refusal on every dev server an operator names, and that the
// refusal offers neither remedy: trusting the SQL and owning the server lift
// nothing here. ErrorMatches anchors the whole message, so a remedy appended
// fails the row.
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
				guard := devclean.NewDevReplayGuard(info)

				for _, test := range currentAccountStatements {
					t.Run(test.name, func(t *testing.T) {
						c := qt.New(t)
						c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, reachRefusal(dialect, test.operation))
					})
				}
			})
		}
	}
}

// TestDevReplayGuard_MySQLCurrentAccount_FailurePathUntrusted is the same on a
// dev server whose run does not trust the SQL: neither remedy is offered,
// since neither would lift the refusal.
func TestDevReplayGuard_MySQLCurrentAccount_FailurePathUntrusted(t *testing.T) {
	untrusted := []catalog.ServerInfo{
		{Dialect: platform.MySQL, Schema: "ptah_dev", URL: "mysql://dev@account-untrusted-mysql:3306/ptah_dev"},
		{Dialect: platform.MariaDB, Schema: "ptah_dev", URL: "mariadb://dev@account-untrusted-mariadb:3306/ptah_dev"},
	}
	for _, info := range untrusted {
		for _, test := range currentAccountStatements {
			t.Run(info.Dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				err := devclean.NewDevReplayGuard(info).ValidateStatement(test.statement)
				c.Assert(err, qt.ErrorMatches, reachRefusal(info.Dialect, test.operation))
			})
		}
	}
}

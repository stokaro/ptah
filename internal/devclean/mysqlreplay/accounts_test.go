package mysqlreplay_test

import (
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// namedScope is a realm a run replays in, named for a subtest.
type namedScope struct {
	name  string
	scope replay.Scope
}

// refusingScopes are the realms that refuse an account statement naming
// another account, and liftingScopes the realms and trust that lift one.
// accountScopes are both: every realm, each without and with trust.
var (
	refusingScopes = []namedScope{
		{name: "database", scope: replay.Scope{Database: "ptah_dev"}},
		{name: "server databases", scope: replay.Scope{Databases: true}},
	}
	liftingScopes = []namedScope{
		{name: "server", scope: replay.Scope{Database: "ptah_dev", Server: true}},
		{name: "provisioned server", scope: replay.Scope{Database: "ptah_dev", Server: true, Provisioned: true}},
		{name: "trusted database", scope: replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
		{name: "trusted server databases", scope: replay.Scope{Databases: true, TrustedSQL: true}},
		{name: "trusted server", scope: replay.Scope{Database: "ptah_dev", Server: true, TrustedSQL: true}},
		{name: "trusted provisioned server", scope: replay.Scope{Database: "ptah_dev", Server: true, Provisioned: true, TrustedSQL: true}},
	}
	accountScopes = slices.Concat(refusingScopes, liftingScopes)
)

// devAccount is the session the rows below run on: dev@%, holding the role
// reader and, through it, the role nested.
const (
	devAccount = "dev@%"
	devRoles   = `["reader", "nested"]`
)

// refusal is the whole refusal of a statement by dialect's guard, where
// operation is all of it after the verb.
func refusal(dialect, operation string) string {
	return dialect + ` migration replay rejects ` + regexp.QuoteMeta(operation)
}

// confined ends every refusal, reaches ends the refusal of a statement that
// reaches the account the run connects as, with its remedy, and unreadable
// ends the refusal of one whose accounts the guard cannot tell from it.
const (
	confined   = " because its effects cannot be confined to the disposable database realm"
	reaches    = " that reaches the account the run connects as" + confined + "; connect as an account the history does not name"
	unreadable = " whose accounts cannot be read" + confined
)

// connectingAccountStatements each reach dev@% or a role it holds, and the
// refusal each is given. The spellings of the account are those MySQL 8.4.13
// and MariaDB 11.8.9 take as dev@%: measured there, each DROP USER below drops
// it, and each other statement leaves a new connection of dev refused or
// unable to drop the dev database.
var connectingAccountStatements = []struct {
	name, statement, operation string
}{
	// The spellings of the account, in a DROP USER.
	{name: "CURRENT_USER", statement: "DROP USER CURRENT_USER", operation: "DROP USER" + reaches},
	{name: "CURRENT_USER()", statement: "DROP USER CURRENT_USER()", operation: "DROP USER" + reaches},
	{name: "lower-case current_user()", statement: "DROP USER current_user()", operation: "DROP USER" + reaches},
	{name: "USER()", statement: "DROP USER USER()", operation: "DROP USER" + reaches},
	{name: "IF EXISTS", statement: "DROP USER IF EXISTS CURRENT_USER", operation: "DROP USER" + reaches},
	{name: "quoted user and host", statement: "DROP USER 'dev'@'%'", operation: "DROP USER" + reaches},
	{name: "bare user", statement: "DROP USER dev@'%'", operation: "DROP USER" + reaches},
	{name: "backticks", statement: "DROP USER `dev`@`%`", operation: "DROP USER" + reaches},
	{name: "double quotes", statement: `DROP USER "dev"@"%"`, operation: "DROP USER" + reaches},
	{name: "host left out", statement: "DROP USER dev", operation: "DROP USER" + reaches},
	{name: "quoted user, host left out", statement: "DROP USER 'dev'", operation: "DROP USER" + reaches},
	{name: "space before the at sign", statement: "DROP USER 'dev' @'%'", operation: "DROP USER" + reaches},
	{name: "empty host", statement: "DROP USER 'dev'@''", operation: "DROP USER" + reaches},
	{name: "backslash escape", statement: `DROP USER 'd\ev'@'%'`, operation: "DROP USER" + reaches},
	{name: "after another account", statement: "DROP USER 'other'@'%', CURRENT_USER", operation: "DROP USER" + reaches},
	{name: "after an unquoted host", statement: "DROP USER other@127.0.0.1, dev", operation: "DROP USER" + reaches},
	{name: "inside SET STATEMENT", statement: "SET STATEMENT max_statement_time = 1 FOR DROP USER CURRENT_USER", operation: "DROP USER" + reaches},

	// Names padded with what a server trims: MySQL trims space, tab, newline,
	// vertical tab, form feed and carriage return from both ends of a user
	// or a host, and MariaDB trims spaces from the end when it writes the
	// grant tables.
	{name: "user padded with a space", statement: "DROP USER 'dev '@'%'", operation: "DROP USER" + reaches},
	{name: "user after a space", statement: "DROP USER ' dev'@'%'", operation: "DROP USER" + reaches},
	{name: "user padded with a tab", statement: "DROP USER 'dev\t'@'%'", operation: "DROP USER" + reaches},
	{name: "user padded with a tab escape", statement: `DROP USER 'dev\t'@'%'`, operation: "DROP USER" + reaches},
	{name: "user padded with a newline", statement: "DROP USER 'dev\n'", operation: "DROP USER" + reaches},
	{name: "user after a vertical tab", statement: "DROP USER '\vdev'", operation: "DROP USER" + reaches},
	{name: "user padded with a form feed", statement: "DROP USER 'dev\f'", operation: "DROP USER" + reaches},
	{name: "user padded with a carriage return", statement: "DROP USER 'dev\r'", operation: "DROP USER" + reaches},
	{name: "host padded with a space", statement: "DROP USER 'dev'@'% '", operation: "DROP USER" + reaches},
	{name: "host of spaces", statement: "DROP USER 'dev'@'  '", operation: "DROP USER" + reaches},
	{name: "padded in backticks", statement: "DROP USER `dev `@`% `", operation: "DROP USER" + reaches},
	{name: "padded in double quotes", statement: `DROP USER "dev "@" %"`, operation: "DROP USER" + reaches},
	{name: "padded user revoked from", statement: "REVOKE ALL ON *.* FROM 'dev '@'%'", operation: "REVOKE" + reaches},
	{name: "padded held role dropped", statement: "DROP ROLE 'reader '", operation: "DROP ROLE" + reaches},
	{name: "revoked from a padded held role", statement: "REVOKE ALL ON *.* FROM ' nested'", operation: "REVOKE" + reaches},
	{name: "padded account on a narrower host", statement: "CREATE USER 'dev '@'127.0.0.1'", operation: "CREATE USER" + reaches},
	{name: "account on a padded narrower host", statement: "CREATE USER 'dev'@' 127.0.0.1'", operation: "CREATE USER" + reaches},
	{name: "anonymous account of a space", statement: "CREATE USER ' '@'127.0.0.1'", operation: "CREATE USER" + reaches},
	{name: "grant creating an anonymous account of a space", statement: "GRANT USAGE ON *.* TO '\t'@'127.0.0.1'", operation: "GRANT" + reaches},

	// A role the account holds, which it loses the privileges of.
	{name: "held role dropped", statement: "DROP ROLE reader", operation: "DROP ROLE" + reaches},
	{name: "nested role dropped", statement: "DROP ROLE IF EXISTS 'nested'", operation: "DROP ROLE" + reaches},
	{name: "current role dropped", statement: "DROP ROLE CURRENT_ROLE", operation: "DROP ROLE" + reaches},
	{name: "held role dropped as a user", statement: "DROP USER reader", operation: "DROP USER" + reaches},

	// REVOKE from the account, from a role it holds, or a proxy on it.
	{name: "every privilege revoked", statement: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER", operation: "REVOKE" + reaches},
	{name: "global privileges revoked", statement: "REVOKE ALL ON *.* FROM CURRENT_USER()", operation: "REVOKE" + reaches},
	{name: "database privilege revoked", statement: "REVOKE SELECT ON ptah_dev.* FROM 'dev'@'%'", operation: "REVOKE" + reaches},
	{name: "column privilege revoked", statement: "REVOKE SELECT (id) ON ptah_dev.t FROM dev", operation: "REVOKE" + reaches},
	{name: "revoked from a list", statement: "REVOKE DROP ON *.* FROM 'other'@'%', dev", operation: "REVOKE" + reaches},
	{name: "revoked ignoring unknown users", statement: "REVOKE IF EXISTS SELECT ON *.* FROM CURRENT_USER IGNORE UNKNOWN USER", operation: "REVOKE" + reaches},
	{name: "held role revoked", statement: "REVOKE 'reader' FROM CURRENT_USER", operation: "REVOKE" + reaches},
	{name: "revoked from a held role", statement: "REVOKE ALL ON *.* FROM reader", operation: "REVOKE" + reaches},
	{name: "revoked from the current role", statement: "REVOKE ALL ON *.* FROM CURRENT_ROLE", operation: "REVOKE" + reaches},
	{name: "revoked from PUBLIC", statement: "REVOKE ALL ON *.* FROM PUBLIC", operation: "REVOKE" + reaches},
	{name: "proxy on the account revoked", statement: "REVOKE PROXY ON CURRENT_USER FROM 'other'@'%'", operation: "REVOKE" + reaches},
	{name: "proxy revoked from the account", statement: "REVOKE PROXY ON 'other'@'%' FROM CURRENT_USER", operation: "REVOKE" + reaches},

	// MariaDB's CREATE OR REPLACE, and an account that shadows the session's.
	{name: "account replaced", statement: "CREATE OR REPLACE USER 'dev'@'%' IDENTIFIED BY 'changed'", operation: "CREATE USER" + reaches},
	{name: "held role replaced", statement: "CREATE OR REPLACE ROLE reader", operation: "CREATE ROLE" + reaches},
	{name: "account on a narrower host", statement: "CREATE USER 'dev'@'127.0.0.1' IDENTIFIED BY 'shadow'", operation: "CREATE USER" + reaches},
	{name: "account on a narrower host if new", statement: "CREATE USER IF NOT EXISTS 'dev'@'localhost'", operation: "CREATE USER" + reaches},
	{name: "account on a narrower host in a list", statement: "CREATE USER 'other'@'%', 'dev'@'10.%'", operation: "CREATE USER" + reaches},
	{name: "account replaced on a narrower host", statement: "CREATE OR REPLACE USER 'dev'@'10.0.0.1'", operation: "CREATE USER" + reaches},
	{name: "anonymous account on a narrower host", statement: "CREATE USER ''@'127.0.0.1'", operation: "CREATE USER" + reaches},
	{name: "role on a narrower host", statement: "CREATE ROLE 'dev'@'127.0.0.1'", operation: "CREATE ROLE" + reaches},

	// A GRANT that locks the account out, or creates one that shadows it.
	{name: "password granted", statement: "GRANT USAGE ON *.* TO CURRENT_USER IDENTIFIED BY 'changed'", operation: "GRANT" + reaches},
	{name: "certificate required", statement: "GRANT USAGE ON *.* TO 'dev'@'%' REQUIRE X509", operation: "GRANT" + reaches},
	{name: "query limit", statement: "GRANT USAGE ON *.* TO dev WITH MAX_QUERIES_PER_HOUR 1", operation: "GRANT" + reaches},
	{name: "statement time limit", statement: "GRANT USAGE ON *.* TO dev WITH MAX_STATEMENT_TIME 1", operation: "GRANT" + reaches},
	{name: "password granted to a list", statement: "GRANT USAGE ON *.* TO 'other'@'%' IDENTIFIED BY 'x', CURRENT_USER", operation: "GRANT" + reaches},
	{name: "grant creating a narrower host", statement: "GRANT USAGE ON *.* TO 'dev'@'127.0.0.1'", operation: "GRANT" + reaches},
	{name: "grant creating an anonymous account", statement: "GRANT USAGE ON *.* TO ''@'127.0.0.1'", operation: "GRANT" + reaches},

	// Refused whatever account they name.
	{name: "another account altered", statement: "ALTER USER 'other'@'%' ACCOUNT LOCK", operation: "ALTER USER" + confined},
	{name: "the account altered", statement: "ALTER USER CURRENT_USER() IDENTIFIED BY 'changed'", operation: "ALTER USER" + confined},
	{name: "the account renamed", statement: "RENAME USER CURRENT_USER TO 'moved'@'%'", operation: "RENAME USER" + confined},
	{name: "another account renamed", statement: "RENAME USER 'other'@'%' TO 'moved'@'%'", operation: "RENAME USER" + confined},
	{name: "own password set", statement: "SET PASSWORD = 'changed'", operation: "SET PASSWORD" + confined},
	{name: "another password set", statement: "SET PASSWORD FOR 'other'@'%' = 'changed'", operation: "SET PASSWORD" + confined},
	{name: "password set by a later item", statement: "SET @a = 1, PASSWORD FOR 'other'@'%' = PASSWORD('changed')", operation: "SET PASSWORD" + confined},
	{name: "default role set", statement: "SET DEFAULT ROLE NONE FOR CURRENT_USER", operation: "SET DEFAULT ROLE" + confined},

	// Accounts the guard cannot read, and so cannot tell from the run's.
	{name: "parenthesized account", statement: "DROP USER (dev)", operation: "DROP USER" + unreadable},
	{name: "variable as an account", statement: "REVOKE ALL ON *.* FROM @v", operation: "REVOKE" + unreadable},
	{name: "REVOKE without FROM", statement: "REVOKE ALL ON *.*", operation: "REVOKE" + unreadable},
}

// TestGuard_ConnectingAccount_FailurePath pins the rule: a statement that
// reaches the account the session runs as is refused in every realm and
// under trust, on either dialect.
func TestGuard_ConnectingAccount_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range connectingAccountStatements {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					err := guard.Validate(test.statement, scope.scope)

					var refused *replay.Refusal
					c.Assert(err, qt.ErrorAs, &refused)
					c.Assert(err, qt.ErrorMatches, refusal(dialect, test.operation))
				})
			}
		}
	}
}

// hostSessionStatements name dev@localhost by its host, which the server
// compares in either case.
var hostSessionStatements = []struct {
	name, statement string
}{
	{name: "host in another case", statement: "DROP USER 'dev'@'LocalHost'"},
	{name: "bare host", statement: "DROP USER dev@localhost"},
	{name: "host padded with a space", statement: "DROP USER 'dev'@'localhost '"},
}

func TestGuard_ConnectingAccount_FailurePathHost(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range hostSessionStatements {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session("dev@localhost", "[]"))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.ErrorMatches, refusal(dialect, "DROP USER"+reaches))
				})
			}
		}
	}
}

// anotherAccountStatements are the controls for connectingAccountStatements:
// each names an account dev@% is not, a role it does not hold, or adds to
// the account without taking anything away. operation is the refusal the
// database realm gives each, which is the answer it has without the rule.
var anotherAccountStatements = []struct {
	name, statement, operation string
}{
	{name: "another account dropped", statement: "DROP USER 'other'@'%'", operation: "DROP USER"},
	{name: "user name in another case", statement: "DROP USER 'DEV'@'%'", operation: "DROP USER"},
	{name: "host the session's does not match", statement: "DROP USER 'dev'@'10.%'", operation: "DROP USER"},
	{name: "list of other accounts", statement: "DROP USER IF EXISTS 'other'@'%', 'other'@'localhost'", operation: "DROP USER"},
	{name: "role not held dropped", statement: "DROP ROLE writer", operation: "DROP ROLE"},
	{name: "revoked from another account", statement: "REVOKE ALL ON *.* FROM 'other'@'%'", operation: "privilege or role mutation"},
	{name: "held role revoked from another account", statement: "REVOKE 'reader' FROM 'other'@'%'", operation: "privilege or role mutation"},
	{name: "revoked from another account ignoring unknown users", statement: "REVOKE SELECT ON ptah_dev.* FROM 'other'@'%' IGNORE UNKNOWN USER", operation: "privilege or role mutation"},
	{name: "privilege granted to the account", statement: "GRANT SELECT ON ptah_dev.* TO CURRENT_USER", operation: "privilege or role mutation"},
	{name: "role granted to the account", statement: "GRANT 'reader' TO CURRENT_USER", operation: "privilege or role mutation"},
	{name: "privilege granted to the account by name", statement: "GRANT SELECT ON ptah_dev.* TO 'dev'@'%' WITH GRANT OPTION", operation: "privilege or role mutation"},
	{name: "password granted to another account", statement: "GRANT USAGE ON *.* TO 'other'@'%' IDENTIFIED BY 'x'", operation: "privilege or role mutation"},
	{name: "grant to another account on a narrower host", statement: "GRANT USAGE ON *.* TO 'other'@'127.0.0.1'", operation: "privilege or role mutation"},
	{name: "another account created", statement: "CREATE USER 'other'@'%' IDENTIFIED BY 'x'", operation: "CREATE USER"},
	{name: "account of a longer user name", statement: "CREATE USER 'developer'@'127.0.0.1'", operation: "CREATE USER"},
	{name: "anonymous account on the session's host", statement: "CREATE USER ''@'%'", operation: "CREATE USER"},
	{name: "account with default roles", statement: "CREATE USER 'app'@'%' DEFAULT ROLE writer, 'dev'@'127.0.0.1'", operation: "CREATE USER"},
	{name: "grant as another account", statement: "GRANT SELECT ON ptah_dev.* TO 'app'@'%' AS 'admin'@'%' WITH ROLE writer, 'dev'@'127.0.0.1'", operation: "privilege or role mutation"},
	{name: "role not held created", statement: "CREATE ROLE writer", operation: "CREATE ROLE"},
	{name: "another account replaced", statement: "CREATE OR REPLACE USER 'other'@'%'", operation: "CREATE USER"},

	// What neither server trims: a no-break space, an ideographic space and
	// a space inside a name.
	{name: "user padded with a no-break space", statement: "DROP USER 'dev\u00a0'@'%'", operation: "DROP USER"},
	{name: "user padded with an ideographic space", statement: "DROP USER 'dev\u3000'@'%'", operation: "DROP USER"},
	{name: "space inside the user", statement: "DROP USER 'd ev'@'%'", operation: "DROP USER"},
	{name: "space inside a held role", statement: "DROP ROLE 'read er'", operation: "DROP ROLE"},
}

// TestGuard_AnotherAccount_HappyPath is the control for the failure path
// above: an account statement that does not reach dev@% runs on a server the
// run owns and under trust, as it does without the rule.
func TestGuard_AnotherAccount_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range anotherAccountStatements {
			for _, scope := range liftingScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.IsNil)
				})
			}
		}
	}
}

// TestGuard_AnotherAccount_FailurePath pins that the realms that refuse an
// account statement refuse it as they do without the rule.
func TestGuard_AnotherAccount_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range anotherAccountStatements {
			for _, scope := range refusingScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.ErrorMatches, refusal(dialect, test.operation+confined))
				})
			}
		}
	}
}

// hostSessionControls name a dev account other than dev@localhost, or one a
// connection is matched to only after it. A DROP USER or a REVOKE names one
// account exactly, so a host pattern that covers localhost names another
// account there.
var hostSessionControls = []struct {
	name, statement string
}{
	{name: "another host dropped", statement: "DROP USER 'dev'@'127.0.0.1'"},
	{name: "host pattern dropped", statement: "DROP USER 'dev'@'local%'"},
	{name: "host with a one-character wildcard dropped", statement: "DROP USER 'dev'@'l_calhost'"},
	{name: "any host dropped", statement: "DROP USER 'dev'@'%'"},
	{name: "revoked from any host", statement: "REVOKE ALL ON *.* FROM 'dev'"},
	{name: "wider host created", statement: "CREATE USER 'dev'@'%'"},
	{name: "anonymous account on the session's host", statement: "CREATE USER ''@'localhost'"},
}

func TestGuard_AnotherAccount_HappyPathHost(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range hostSessionControls {
			for _, scope := range liftingScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session("dev@localhost", "[]"))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.IsNil)
				})
			}
		}
	}
}

// A session reads 'd\ev' as dev under the default rules and as d\ev with
// NO_BACKSLASH_ESCAPES. The guard does not know the session's mode, so it
// refuses the statement for an account of either name.
func TestGuard_ConnectingAccount_FailurePathBackslashReadings(t *testing.T) {
	sessions := []struct {
		name, account string
	}{
		{name: "account the default rules read", account: `dev@%`},
		{name: "account NO_BACKSLASH_ESCAPES reads", account: `d\ev@%`},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, session := range sessions {
			t.Run(dialect+"/"+session.name, func(t *testing.T) {
				c := qt.New(t)
				guard := boundGuard(c, dialect, utf8Session(session.account, "[]"))

				c.Assert(guard.Validate(`DROP USER 'd\ev'@'%'`, replay.Scope{Database: "ptah_dev", Server: true}), qt.ErrorMatches,
					refusal(dialect, "DROP USER"+reaches))
			})
		}
	}
}

// A guard that read no session knows the spellings that name the session's
// account whoever runs it, and cannot tell any account spelled out from the
// session's, so it refuses a statement that names one. Every run's guard
// reads its session first; this pins what the guard does before it does.
func TestGuard_ConnectingAccount_FailurePathUnreadSession(t *testing.T) {
	tests := []struct {
		name, statement, operation string
	}{
		{name: "CURRENT_USER", statement: "DROP USER CURRENT_USER", operation: "DROP USER" + reaches},
		{name: "PUBLIC", statement: "REVOKE ALL ON *.* FROM PUBLIC", operation: "REVOKE" + reaches},
		{name: "ALTER USER", statement: "ALTER USER 'other'@'%' ACCOUNT LOCK", operation: "ALTER USER" + confined},
		{name: "account dropped by name", statement: "DROP USER 'dev'@'%'", operation: "DROP USER" + unreadable},
		{name: "role dropped by name", statement: "DROP ROLE writer", operation: "DROP ROLE" + unreadable},
		{name: "account created", statement: "CREATE USER 'app'@'%'", operation: "CREATE USER" + unreadable},
		{name: "privilege granted by name", statement: "GRANT SELECT ON ptah_dev.* TO 'app'@'%'", operation: "GRANT" + unreadable},
		{name: "revoked by name", statement: "REVOKE ALL ON *.* FROM 'app'@'%'", operation: "REVOKE" + unreadable},
		{name: "proxy on an account revoked", statement: "REVOKE PROXY ON 'other'@'%' FROM CURRENT_USER", operation: "REVOKE" + unreadable},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				err := guardFor(c, dialect).Validate(test.statement, replay.Scope{Database: "ptah_dev", Server: true})
				c.Assert(err, qt.ErrorMatches, refusal(dialect, test.operation))
			})
		}
	}
}

// A guard that read no session lifts what names no account it cannot tell
// from the session's: a GRANT that takes nothing from the run's own account,
// and a write to a table of the mysql schema that holds no account.
func TestGuard_ConnectingAccount_HappyPathUnreadSession(t *testing.T) {
	statements := []struct {
		name, statement string
	}{
		{name: "privilege granted to the account", statement: "GRANT SELECT ON ptah_dev.* TO CURRENT_USER"},
		{name: "time zone table written", statement: "DELETE FROM mysql.time_zone_name"},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range statements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				err := guardFor(c, dialect).Validate(test.statement, replay.Scope{Database: "ptah_dev", Server: true})
				c.Assert(err, qt.IsNil)
			})
		}
	}
}

// CURRENT_USER() names a user whose quoted name holds an at sign with the
// host after the last one.
func TestGuard_ConnectingAccount_FailurePathUserWithAnAtSign(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := boundGuard(c, dialect, utf8Session("ci@corp@%", "[]"))

			err := guard.Validate("DROP USER 'ci@corp'@'%'", replay.Scope{Database: "ptah_dev", Server: true})

			c.Assert(err, qt.ErrorMatches, refusal(dialect, "DROP USER"+reaches))
		})
	}
}

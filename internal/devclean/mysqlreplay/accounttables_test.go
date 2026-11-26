package mysqlreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// accountTableStatements each write a table of the mysql schema that holds
// accounts, which the server reads as a change to them once it reloads the
// tables, and the refusal each is given.
var accountTableStatements = []struct {
	name, statement, operation string
}{
	{name: "rows deleted", statement: "DELETE FROM mysql.user WHERE User = 'other'", operation: `write to account table "mysql.user"`},
	{name: "account locked", statement: "UPDATE mysql.user SET account_locked = 'Y' WHERE User = 'dev'", operation: `write to account table "mysql.user"`},
	{name: "rows copied in", statement: "INSERT INTO mysql.db SELECT * FROM ptah_dev.db", operation: `write to account table "mysql.db"`},
	{name: "row replaced", statement: "REPLACE INTO mysql.tables_priv VALUES ('%', 'ptah_dev', 'dev', 't', '', NOW(), 'Select', '')", operation: `write to account table "mysql.tables_priv"`},
	{name: "table truncated", statement: "TRUNCATE TABLE mysql.columns_priv", operation: `write to account table "mysql.columns_priv"`},
	{name: "table dropped", statement: "DROP TABLE IF EXISTS mysql.procs_priv", operation: `write to account table "mysql.procs_priv"`},
	{name: "table altered", statement: "ALTER TABLE mysql.proxies_priv ENGINE = InnoDB", operation: `write to account table "mysql.proxies_priv"`},
	{name: "table renamed", statement: "RENAME TABLE mysql.role_edges TO ptah_dev.role_edges", operation: `write to account table "mysql.role_edges"`},
	{name: "table locked", statement: "LOCK TABLES mysql.default_roles WRITE", operation: `write to account table "mysql.default_roles"`},
	{name: "rows loaded", statement: "LOAD DATA INFILE '/tmp/grants' INTO TABLE mysql.global_grants", operation: `write to account table "mysql.global_grants"`},
	{name: "password history deleted", statement: "DELETE FROM mysql.password_history", operation: `write to account table "mysql.password_history"`},
	{name: "MariaDB account rewritten", statement: "UPDATE mysql.global_priv SET Priv = '{}' WHERE User = 'dev'", operation: `write to account table "mysql.global_priv"`},
	{name: "MariaDB role mapping deleted", statement: "DELETE FROM mysql.roles_mapping", operation: `write to account table "mysql.roles_mapping"`},
	{name: "index created", statement: "CREATE INDEX by_user ON mysql.user (User)", operation: `write to account table "mysql.user"`},
	{name: "names in upper case", statement: "DELETE FROM MYSQL.USER", operation: `write to account table "MYSQL.USER"`},
	{name: "quoted names", statement: "DELETE FROM `mysql`.`user`", operation: `write to account table "mysql.user"`},
	{name: "after a WITH clause", statement: "WITH gone AS (SELECT 1) DELETE FROM mysql.user", operation: `write to account table "mysql.user"`},
	{name: "joined in a multi-table UPDATE", statement: "UPDATE ptah_dev.t JOIN mysql.user AS u ON u.User = t.name SET u.account_locked = 'Y'", operation: `write to account table "mysql.user"`},
	{name: "inside SET STATEMENT", statement: "SET STATEMENT max_statement_time = 1 FOR DELETE FROM mysql.user", operation: `write to account table "mysql.user"`},
	{name: "view over one", statement: "CREATE VIEW ptah_dev.accounts AS SELECT * FROM mysql.user", operation: `view "ptah_dev.accounts" over account table "mysql.user"`},

	// A server whose lower_case_table_names is set lowercases a name by its
	// Unicode simple mapping, so a dotted capital I (U+0130) is i.
	{name: "tables_priv with a dotted capital I", statement: "DELETE FROM mysql.tables_prİv", operation: `write to account table "mysql.tables_prİv"`},
	{name: "columns_priv with a dotted capital I", statement: "DELETE FROM mysql.columns_prİv", operation: `write to account table "mysql.columns_prİv"`},
	{name: "procs_priv with a dotted capital I", statement: "DELETE FROM mysql.procs_prİv", operation: `write to account table "mysql.procs_prİv"`},
	{name: "proxies_priv with a dotted capital I", statement: "DELETE FROM mysql.proxİes_priv", operation: `write to account table "mysql.proxİes_priv"`},
	{name: "password_history with a dotted capital I", statement: "DELETE FROM mysql.password_hİstory", operation: `write to account table "mysql.password_hİstory"`},
	{name: "global_priv with a dotted capital I", statement: "DELETE FROM mysql.global_prİv", operation: `write to account table "mysql.global_prİv"`},
	{name: "roles_mapping with a dotted capital I", statement: "DELETE FROM mysql.roles_mappİng", operation: `write to account table "mysql.roles_mappİng"`},
}

// TestGuard_AccountTable_FailurePath pins that a write to an account table
// is refused in every realm and under trust, whoever the session runs as.
func TestGuard_AccountTable_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range accountTableStatements {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.ErrorMatches, refusal(dialect, test.operation+confined))
				})
			}
		}
	}
}

// An unqualified name is in the database the statement runs in.
func TestGuard_AccountTable_FailurePathUnqualified(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

			err := guard.Validate("DELETE FROM user", replay.Scope{Database: "mysql", Server: true, TrustedSQL: true})

			c.Assert(err, qt.ErrorMatches, refusal(dialect, `write to account table "mysql.user"`+confined))
		})
	}
}

// accountTableControls are the controls for accountTableStatements: a table
// of the mysql schema that holds no account, a table of the same name in
// another database, a read of an account table, and names the server does
// not lowercase to an account table's: a long s (U+017F) and a dotless i
// (U+0131) stay what they are, and the table or database does not exist
// (1146 or 1049 on MySQL 26.10.0 and MariaDB 12.3.3 with
// --lower-case-table-names=1). Each is lifted where it is lifted without the
// rule.
var accountTableControls = []struct {
	name, statement string
}{
	{name: "time zone table written", statement: "DELETE FROM mysql.time_zone_name"},
	{name: "table of the same name in the dev database", statement: "DELETE FROM ptah_dev.user"},
	{name: "unqualified table of the same name", statement: "UPDATE user SET name = 'x'"},
	{name: "account table read", statement: "SELECT User FROM mysql.user"},
	{name: "account table read into the dev database", statement: "INSERT INTO ptah_dev.accounts SELECT User FROM mysql.user"},
	{name: "user with a long s", statement: "DELETE FROM mysql.uſer"},
	{name: "schema with a long s", statement: "DELETE FROM myſql.user"},
	{name: "global_priv with a dotless i", statement: "UPDATE mysql.global_prıv SET Priv = '{}'"},
}

func TestGuard_AccountTable_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range accountTableControls {
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

// The database realm refuses a write to a table of the mysql schema that
// holds no account as a write to another database, as it does without the
// rule.
func TestGuard_AccountTable_FailurePathAnotherTable(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

			err := guard.Validate("DELETE FROM mysql.time_zone_name", replay.Scope{Database: "ptah_dev"})

			c.Assert(err, qt.ErrorMatches, refusal(dialect, `cross-database target "mysql.time_zone_name"`+confined))
		})
	}
}

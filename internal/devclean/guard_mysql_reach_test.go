package devclean_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// The rows below spell, the way MySQL 8.4.13 and MariaDB 11.8.9 take them, a
// write that lands outside the dev database: an INSERT or REPLACE without
// INTO, a multi-table DELETE, the tables an ALTER TABLE or a MERGE table
// reaches, a view that writes through to another database, and a function or
// sequence of another database. Each was measured writing a table of another
// database on both servers.

// reachRefusal is the refusal of a statement whose effect reaches what want
// names, as dialect's guard words it in the database realm.
func reachRefusal(dialect, want string) string {
	return dialect + ` migration replay rejects ` + regexp.QuoteMeta(want) +
		` because its effects cannot be confined to the disposable database realm`
}

// trustedMariaDBDev is the MariaDB counterpart of trustedMySQLDev.
var trustedMariaDBDev = catalog.ServerInfo{
	Dialect: platform.MariaDB,
	Schema:  "ptah_dev",
	URL:     "mariadb://dev@trusted-mariadb:3306/ptah_dev",
}

// TestReplayGuardMySQLFamilyReach_HappyPath is the control for the failure
// path below: each spelling it refuses runs when every name it reaches is in
// the dev database, or names a column, an index or a function argument rather
// than a table.
func TestReplayGuardMySQLFamilyReach_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "insert without INTO", statement: "INSERT users VALUES (1)"},
		{name: "insert with modifiers", statement: "INSERT LOW_PRIORITY IGNORE ptah_dev.users VALUES (1)"},
		{name: "replace without INTO", statement: "REPLACE users VALUES (1)"},
		{name: "delete reading the dev database in a subquery", statement: "DELETE FROM users WHERE id IN (SELECT user_id FROM ptah_dev.audit)"},
		{name: "multi-table delete in the dev database", statement: "DELETE u FROM users AS u JOIN ptah_dev.audit AS a ON a.user_id = u.id"},
		{name: "delete with ORDER BY and LIMIT", statement: "DELETE FROM users ORDER BY users.id, users.name LIMIT 1"},
		{name: "delete returning qualified columns", statement: "DELETE FROM users WHERE id = 1 RETURNING users.id, users.name"},
		{name: "table maintenance in the dev database", statement: "OPTIMIZE TABLE users, ptah_dev.audit"},
		{name: "renamed column", statement: "ALTER TABLE users RENAME COLUMN name TO full_name"},
		{name: "renamed index", statement: "ALTER TABLE users RENAME INDEX users_email TO users_mail"},
		{name: "rename without TO in the dev database", statement: "ALTER TABLE users RENAME ptah_dev.members"},
		{name: "partition exchanged in the dev database", statement: "ALTER TABLE users EXCHANGE PARTITION p0 WITH TABLE ptah_dev.users_archive"},
		{name: "merge table over the dev database", statement: "CREATE TABLE all_users (id bigint) ENGINE=MERGE UNION=(users, ptah_dev.audit)"},
		{name: "table from a UNION query", statement: "CREATE TABLE union_users AS SELECT id FROM users UNION SELECT id FROM audit"},
		{
			name:      "table from a UNION reading another database",
			statement: "CREATE TABLE union_users AS SELECT id FROM users UNION (SELECT id FROM production.audit)",
		},
		{
			name: "view whose functions take FROM and FOR",
			statement: "CREATE VIEW order_parts AS SELECT EXTRACT(YEAR FROM o.created_at) AS y, " +
				"TRIM(BOTH ' ' FROM o.name) AS n, SUBSTRING(o.name FROM 1 FOR 2) AS s FROM orders AS o",
		},
		{
			name: "view over the dev database",
			statement: "CREATE VIEW paid AS SELECT o.id FROM orders o JOIN ptah_dev.items i ON i.order_id = o.id " +
				"WHERE o.id IN (SELECT order_id FROM refunds) GROUP BY o.id",
		},
		{name: "view with a window and a cast", statement: "CREATE VIEW ranked AS SELECT COUNT(*) OVER (PARTITION BY a ORDER BY b) AS c, CAST(a AS CHAR(10)) AS d FROM t"},
		{
			name: "view whose clauses list qualified columns",
			statement: "CREATE VIEW grouped AS SELECT u.id, COUNT(*) AS n FROM users AS u WHERE u.active = 1 " +
				"GROUP BY u.id, u.name ORDER BY u.id, u.name",
		},
		{name: "function of the dev database", statement: "SELECT ptah_dev.add_one(1)"},
		{name: "function of a package of the dev database", statement: "SELECT ptah_dev.billing.bump()"},
		{name: "function of the dev database in an update", statement: "UPDATE users SET score = `ptah_dev`.`add_one`(score)"},
		{
			// The parent of a foreign key is read, never written.
			name:      "foreign key into another database",
			statement: "CREATE TABLE orders (id bigint, user_id bigint, FOREIGN KEY (user_id) REFERENCES production.users (id))",
		},
		{name: "index on a qualified table", statement: "CREATE INDEX users_email ON ptah_dev.users (email)"},
		{name: "insert naming its columns", statement: "INSERT INTO ptah_dev.users (id, name) VALUES (1, 'a')"},
		{name: "sequence of the dev database", statement: "SELECT NEXTVAL(ptah_dev.order_ids), NEXT VALUE FOR order_ids"},
		{name: "locking read", statement: "SELECT id FROM users WHERE active = 1 FOR UPDATE"},
		{name: "transaction start", statement: "BEGIN"},
		{name: "transaction start with WORK", statement: "BEGIN WORK"},
		{name: "transaction start with START", statement: "START TRANSACTION"},
		{name: "own backup lock released", statement: "UNLOCK INSTANCE"},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "ptah_dev"}, devclean.ReplayRealmDatabase)
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
			})
		}
	}
}

// TestReplayGuardMySQLFamilyReach_FailurePath pins each spelling of a write to
// another database that the database realm refuses, on both servers.
func TestReplayGuardMySQLFamilyReach_FailurePath(t *testing.T) {
	const users = `cross-database target "production.users"`
	const throughV = `view "v" over cross-database table "production.users"`
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "insert without INTO", statement: "INSERT production.users VALUES (1)", want: users},
		{name: "insert with a TABLE modifier", statement: "INSERT INTO TABLE production.users VALUES (1)", want: users},
		{name: "insert with modifiers and no INTO", statement: "INSERT LOW_PRIORITY IGNORE production.users VALUES (1)", want: users},
		{name: "delayed insert without INTO", statement: "INSERT DELAYED production.users VALUES (1)", want: users},
		{name: "insert into a database named like a modifier", statement: "INSERT `ignore`.users VALUES (1)", want: `cross-database target "ignore.users"`},
		{name: "replace without INTO", statement: "REPLACE production.users VALUES (1)", want: users},
		{name: "replace with a modifier and no INTO", statement: "REPLACE LOW_PRIORITY production.users VALUES (1)", want: users},
		{name: "delete through USING and an alias", statement: "DELETE FROM u USING production.users AS u", want: users},
		{
			name:      "multi-table delete naming its target by alias",
			statement: "DELETE u, a FROM ptah_dev.audit AS a JOIN production.users AS u ON u.id = a.user_id",
			want:      users,
		},
		{name: "multi-table delete after modifiers", statement: "DELETE LOW_PRIORITY QUICK IGNORE u FROM production.users AS u", want: users},
		{name: "multi-table delete with a wildcard", statement: "DELETE u.* FROM production.users AS u", want: users},
		{
			name:      "multi-table delete after a leading CTE",
			statement: "WITH gone AS (SELECT 1 AS id) DELETE u FROM production.users AS u JOIN gone ON gone.id = u.id",
			want:      users,
		},
		{name: "second table of an OPTIMIZE", statement: "OPTIMIZE TABLE ptah_dev.users, production.users", want: users},
		{name: "ANALYZE after NO_WRITE_TO_BINLOG", statement: "ANALYZE NO_WRITE_TO_BINLOG TABLE production.users", want: users},
		{name: "second table of a REPAIR after LOCAL", statement: "REPAIR LOCAL TABLE ptah_dev.users, production.users", want: users},
		{name: "rename without TO", statement: "ALTER TABLE users RENAME production.users", want: users},
		{name: "rename with AS", statement: "ALTER TABLE users RENAME AS production.users", want: users},
		{name: "partition exchanged with another database", statement: "ALTER TABLE users EXCHANGE PARTITION p0 WITH TABLE production.users", want: users},
		{
			name:      "merge table over another database",
			statement: "CREATE TABLE all_users (id bigint) ENGINE=MERGE UNION=(users, production.users) INSERT_METHOD=LAST",
			want:      users,
		},
		{name: "merge table moved onto another database", statement: "ALTER TABLE all_users UNION = (production.users)", want: users},
		{
			name:      "view over another database",
			statement: "CREATE VIEW active_users AS SELECT id, UPPER(name) AS n FROM production.users WHERE active = 1",
			want:      `view "active_users" over cross-database table "production.users"`,
		},
		{
			name:      "view joining another database",
			statement: "CREATE OR REPLACE ALGORITHM=MERGE VIEW v AS SELECT a.id FROM audit AS a JOIN production.users AS u ON u.id = a.user_id",
			want:      throughV,
		},
		{name: "view over a derived table", statement: "CREATE VIEW v AS SELECT * FROM (SELECT id FROM production.users) AS d", want: throughV},
		{name: "view over a FROM list", statement: "CREATE VIEW v (id) AS SELECT a.id FROM audit AS a, production.users AS u", want: throughV},
		{name: "view over a join in parentheses", statement: "CREATE VIEW v AS SELECT a.id FROM (audit AS a JOIN production.users AS u ON u.id = a.id)", want: throughV},
		{name: "view over a TABLE statement", statement: "CREATE VIEW v AS TABLE production.users", want: throughV},
		{
			name:      "changed view",
			statement: "ALTER VIEW ptah_dev.active_users AS SELECT id FROM production.users",
			want:      `view "ptah_dev.active_users" over cross-database table "production.users"`,
		},
		{name: "view in another database", statement: "CREATE VIEW production.v AS SELECT id FROM users", want: `cross-database target "production.v"`},
		{name: "function of another database", statement: "SELECT production.bump()", want: `cross-database target "production.bump"`},
		{name: "function of another database in DO", statement: "DO production.bump (1)", want: `cross-database target "production.bump"`},
		{name: "function of another database in SET", statement: "SET @x = `production`.`bump`()", want: `cross-database target "production.bump"`},
		{
			name:      "function of another database in a CTE",
			statement: "WITH x AS (SELECT production.bump() AS v) SELECT v FROM x",
			want:      `cross-database target "production.bump"`,
		},
		{name: "function of another database in VALUES", statement: "INSERT INTO users VALUES (production.bump())", want: `cross-database target "production.bump"`},
		{name: "function of another database in an update", statement: "UPDATE users SET score = production.bump(score)", want: `cross-database target "production.bump"`},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "ptah_dev"}, devclean.ReplayRealmDatabase)
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, reachRefusal(dialect, test.want))
			})
		}
	}
}

// TestReplayGuardMariaDBReach_FailurePath pins the MariaDB spellings: a
// sequence of another database advanced or set, a compound statement, whose
// body the guard cannot read before the server runs it, and a SPIDER table.
func TestReplayGuardMariaDBReach_FailurePath(t *testing.T) {
	const sequence = `cross-database target "production.order_ids"`
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "sequence advanced by NEXTVAL", statement: "SELECT NEXTVAL(production.order_ids)", want: sequence},
		{name: "sequence set by SETVAL", statement: "SELECT SETVAL(production.order_ids, 500)", want: sequence},
		{name: "sequence advanced by NEXT VALUE FOR", statement: "SELECT NEXT VALUE FOR production.order_ids", want: sequence},
		{name: "sequence advanced in Oracle mode", statement: "SELECT production.order_ids.nextval", want: sequence},
		{name: "sequence advanced by a column default", statement: "CREATE TABLE orders (id bigint DEFAULT NEXTVAL(production.order_ids))", want: sequence},
		{name: "function of a package of another database", statement: "SELECT production.billing.bump()", want: `cross-database target "production.billing.bump"`},
		{name: "anonymous block", statement: "BEGIN NOT ATOMIC DELETE FROM production.users WHERE 1; END", want: "compound statement"},
		{name: "IF statement", statement: "IF 1 THEN DELETE FROM production.users; END IF", want: "compound statement"},
		{name: "CASE statement", statement: "CASE WHEN 1 THEN DELETE FROM production.users; END CASE", want: "compound statement"},
		{name: "LOOP statement", statement: "LOOP DELETE FROM production.users; LEAVE; END LOOP", want: "compound statement"},
		{name: "REPEAT statement", statement: "REPEAT DELETE FROM production.users; UNTIL 1 END REPEAT", want: "compound statement"},
		{name: "WHILE statement", statement: "while 0 do delete from production.users; end while", want: "compound statement"},
		{name: "FOR statement", statement: "FOR i IN 1..1 DO DELETE FROM production.users; END FOR", want: "compound statement"},
		{
			name:      "SPIDER table",
			statement: `CREATE TABLE remote_users (id bigint) ENGINE=SPIDER COMMENT='wrapper "mysql", host "127.0.0.1", database "production", table "users"'`,
			want:      "SPIDER storage engine",
		},
	}
	guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.MariaDB, Schema: "ptah_dev"}, devclean.ReplayRealmDatabase)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, reachRefusal(platform.MariaDB, test.want))
		})
	}
}

// mysqlEveryRealmStatements are the server statements no realm lifts, with
// the operation each refusal names. Each acts on the server or on the
// identity the run connects as, and the run needs both for the rest of the
// command whoever owns the server.
var mysqlEveryRealmStatements = []struct {
	name      string
	statement string
	operation string
}{
	{name: "server stopped", statement: "SHUTDOWN", operation: "SHUTDOWN"},
	{name: "server restarted", statement: "RESTART", operation: "RESTART"},
	{name: "session ended", statement: "KILL 42", operation: "KILL"},
	{name: "statement ended", statement: "kill query 42", operation: "KILL"},
	{name: "binary log events replayed", statement: "BINLOG 'AAAA'", operation: "BINLOG"},
	{name: "binary logs purged", statement: "PURGE BINARY LOGS TO 'binlog.000002'", operation: "binary log or replication change"},
	{name: "replication source changed", statement: "CHANGE REPLICATION SOURCE TO SOURCE_HOST = 'elsewhere'", operation: "binary log or replication change"},
	{name: "replication master changed", statement: "CHANGE MASTER TO MASTER_HOST = 'elsewhere'", operation: "binary log or replication change"},
	{name: "replication filter changed", statement: "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (dev)", operation: "binary log or replication change"},
	{name: "replica started", statement: "START REPLICA", operation: "binary log or replication change"},
	{name: "replica stopped", statement: "STOP REPLICA", operation: "binary log or replication change"},
	{name: "slave started", statement: "START SLAVE", operation: "binary log or replication change"},
	{name: "slave stopped", statement: "STOP SLAVE", operation: "binary log or replication change"},
	{name: "every slave started", statement: "START ALL SLAVES", operation: "binary log or replication change"},
	{name: "every slave stopped", statement: "STOP ALL SLAVES", operation: "binary log or replication change"},
	{name: "group replication started", statement: "START GROUP_REPLICATION", operation: "binary log or replication change"},
	{name: "group replication stopped", statement: "STOP GROUP_REPLICATION", operation: "binary log or replication change"},
	{name: "data directory cloned", statement: "CLONE LOCAL DATA DIRECTORY = '/tmp/copy'", operation: "CLONE"},
	{name: "instance locked for backup", statement: "LOCK INSTANCE FOR BACKUP", operation: "server-wide backup lock"},
	{name: "backup stage started", statement: "BACKUP STAGE START", operation: "server-wide backup lock"},
	{name: "own password set", statement: "SET PASSWORD = 'changed'", operation: "SET PASSWORD"},
	{name: "account password set", statement: "SET PASSWORD FOR 'app'@'%' = 'changed'", operation: "SET PASSWORD"},
	{name: "default role set", statement: "SET DEFAULT ROLE ALL TO 'app'@'%'", operation: "SET DEFAULT ROLE"},
}

// TestReplayGuard_MySQLRefusesServerStatementsInEveryRealm pins that no realm
// lifts the statements above. A refusal in a realm a remedy is configured for
// ends where the guard's own words end, so it never names a setting that does
// not help.
func TestReplayGuard_MySQLRefusesServerStatementsInEveryRealm(t *testing.T) {
	realms := []struct {
		name  string
		realm devclean.ReplayRealm
	}{
		{name: "database", realm: devclean.ReplayRealmDatabase},
		{name: "server databases", realm: devclean.ReplayRealmServerDatabases},
		{name: "server", realm: devclean.ReplayRealmServer},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, realm := range realms {
			guard := mysqlGuard(dialect, realm.realm).WithServerRealmRemedy(replayRemedy)
			for _, test := range mysqlEveryRealmStatements {
				t.Run(dialect+"/"+realm.name+"/"+test.name, func(t *testing.T) {
					c := qt.New(t)
					c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, reachRefusal(dialect, test.operation))
				})
			}
		}
	}
}

// TestDevReplayGuard_TrustedSQL_KeepsMySQLServerStatements is the same under
// trust: each refusal is the one without trust, and names no trust remedy.
func TestDevReplayGuard_TrustedSQL_KeepsMySQLServerStatements(t *testing.T) {
	for _, info := range []catalog.ServerInfo{trustedMySQLDev, trustedMariaDBDev} {
		for _, test := range mysqlEveryRealmStatements {
			t.Run(info.Dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				trustReplaySQL(c, info.URL)
				c.Assert(devclean.NewDevReplayGuard(info).ValidateStatement(test.statement), qt.ErrorMatches,
					reachRefusal(info.Dialect, test.operation))
			})
		}
	}
}

// mysqlTrustLifted are the MySQL and MariaDB statements trust lifts beyond
// those of trustLifted: dynamic SQL, data moved across the server's edge, a
// write to another database the target reader names, and the statements a
// server the run owns lifts.
var mysqlTrustLifted = []struct {
	name      string
	info      catalog.ServerInfo
	statement string
}{
	{"MariaDB anonymous block", trustedMariaDBDev, "BEGIN NOT ATOMIC DELETE FROM other.t WHERE 1; END"},
	{"MySQL IMPORT TABLE", trustedMySQLDev, "IMPORT TABLE FROM '/tmp/t.sdi'"},
	{"MariaDB SPIDER table", trustedMariaDBDev, `CREATE TABLE s (id int) ENGINE=SPIDER COMMENT='wrapper "mysql", host "elsewhere", table "t"'`},
	{"MySQL insert into another database without INTO", trustedMySQLDev, "INSERT other.t VALUES (1)"},
	{"MySQL view over another database", trustedMySQLDev, "CREATE VIEW v AS SELECT * FROM other.t"},
	{"MySQL function of another database", trustedMySQLDev, "SELECT other.f()"},
	{"MariaDB sequence of another database", trustedMariaDBDev, "SELECT NEXTVAL(other.s)"},
	{"MySQL key cache", trustedMySQLDev, "CACHE INDEX items IN hot_cache"},
	{"MySQL resource group of another thread", trustedMySQLDev, "SET RESOURCE GROUP batch FOR 42"},
}

// TestDevReplayGuard_TrustedSQL_LiftsMySQLConfinementRefusals is
// [TestDevReplayGuard_TrustedSQL_LiftsConfinementRefusals] for the rows above.
func TestDevReplayGuard_TrustedSQL_LiftsMySQLConfinementRefusals(t *testing.T) {
	for _, test := range mysqlTrustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			trustReplaySQL(c, test.info.URL)
			c.Assert(devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestDevReplayGuard_WithoutTrust_RefusesMySQLAndNamesTheTrustRemedy is the
// control for the test above.
func TestDevReplayGuard_WithoutTrust_RefusesMySQLAndNamesTheTrustRemedy(t *testing.T) {
	for _, test := range mysqlTrustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, `.* migration replay rejects .*`+trustRemedy)
		})
	}
}

package migrator

// White-box testing required: the rollback accounting these tests pin is
// unexported and has no public surface. Reaching it through MigrateUp would
// need a live MySQL server per row, and the point of the table is to separate
// the dialect axis from the statement axis, which only a direct call can do.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// effectOfSurvivors maps what survived the probe's ROLLBACK to the transaction
// effect that explains it. The probe is always the same six lines:
//
//	START TRANSACTION;
//	INSERT INTO led VALUES (1,'a_pre');
//	<the statement under test>
//	INSERT INTO led VALUES (2,'b_post');
//	ROLLBACK;
//	SELECT note FROM led;
//
// so the surviving rows are a direct reading of what the server did:
//
//	none          nothing was committed and the transaction was still open
//	a_pre+b_post  the prefix was committed and no transaction was left open, so
//	              the statement after it committed itself
//	a_pre         the prefix was committed and a new transaction was opened, so
//	              the statement after it was rolled back
//	b_post        the prefix was thrown away and the statement after it
//	              committed itself
var effectOfSurvivors = map[string]implicitCommitEffect{
	"none":         implicitCommitNone,
	"a_pre+b_post": implicitCommitEnds,
	"a_pre":        implicitCommitRestarts,
	"b_post":       implicitCommitDiscards,
}

// TestImplicitCommitEffectOf pins the classifier against a measurement, one row
// per statement, on both servers.
//
// Every mysql/mariadb value below is the literal output of running that row's
// statement through the probe described on [effectOfSurvivors] against MySQL
// 9.7.1 and MariaDB 11.4.12. Nothing here is derived from documentation, and
// the two servers are listed separately because they do not agree: CACHE INDEX
// and LOAD INDEX INTO CACHE end the transaction on MySQL and do nothing on
// MariaDB.
//
// Getting a row wrong is not cosmetic. A statement wrongly reported as
// committing marks a rolled-back statement applied and a resume skips it
// forever; a statement wrongly reported as committing nothing makes a resume
// re-run SQL the server already committed and duplicate its effect.
func TestImplicitCommitEffectOf(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		statement string
		mysql     string
		mariadb   string
	}{
		// Data statements and session settings: the transaction survives them.
		{name: "INSERT", statement: "INSERT INTO tgt VALUES (1)", mysql: "none", mariadb: "none"},
		{name: "UPDATE", statement: "UPDATE tgt SET i = i", mysql: "none", mariadb: "none"},
		{name: "DELETE", statement: "DELETE FROM tgt", mysql: "none", mariadb: "none"},
		{name: "REPLACE", statement: "REPLACE INTO tgt VALUES (1)", mysql: "none", mariadb: "none"},
		{name: "SELECT", statement: "SELECT COUNT(*) INTO @discard FROM tgt", mysql: "none", mariadb: "none"},
		{name: "SET SESSION", statement: "SET SESSION sql_mode = ''", mysql: "none", mariadb: "none"},
		{name: "SET user variable", statement: "SET @user_var = 1", mysql: "none", mariadb: "none"},
		// The first version of this file listed SET autocommit as committing.
		// It does not: Ptah opens the migration transaction with START
		// TRANSACTION and leaves session autocommit at 1, so the assignment has
		// nothing to change.
		{name: "SET autocommit = 1", statement: "SET autocommit = 1", mysql: "none", mariadb: "none"},
		{name: "SET autocommit = 0", statement: "SET autocommit = 0", mysql: "none", mariadb: "none"},

		// Account management.
		{
			name:      "SET PASSWORD",
			statement: "SET PASSWORD FOR 'probeu'@'%' = 'Pw234567!'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "SET DEFAULT ROLE",
			statement: "SET DEFAULT ROLE NONE TO 'probeu'@'%'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "CREATE USER",
			statement: "CREATE USER 'probeu2'@'%' IDENTIFIED BY 'Pw123456!'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "ALTER USER",
			statement: "ALTER USER 'probeu2'@'%' IDENTIFIED BY 'Pw234567!'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "RENAME USER",
			statement: "RENAME USER 'probeu2'@'%' TO 'probeu3'@'%'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{name: "DROP USER", statement: "DROP USER 'probeu3'@'%'", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{
			name:      "GRANT",
			statement: "GRANT SELECT ON icprobe.* TO 'probeu'@'%'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "REVOKE",
			statement: "REVOKE SELECT ON icprobe.* FROM 'probeu'@'%'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},

		// Schema DDL.
		{name: "CREATE TABLE", statement: "CREATE TABLE ct1 (i INT)", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{
			name:      "CREATE INDEX",
			statement: "CREATE INDEX ix1 ON ixt (i)",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "CREATE VIEW",
			statement: "CREATE VIEW v1 AS SELECT 1 AS one",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "CREATE DATABASE",
			statement: "CREATE DATABASE icprobe_scratch",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		// A column named `temp` is not the TEMPORARY qualifier. Scanning a fixed
		// number of leading tokens for it read this row as a temporary table.
		{
			name:      "CREATE TABLE with a temp column",
			statement: "CREATE TABLE hastemp (temp INT)",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{name: "DROP TABLE", statement: "DROP TABLE dropme", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "DROP INDEX", statement: "DROP INDEX ix1 ON ixt", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{
			name:      "DROP DATABASE",
			statement: "DROP DATABASE icprobe_scratch",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "ALTER TABLE",
			statement: "ALTER TABLE alt ADD COLUMN c1 INT",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "RENAME TABLE",
			statement: "RENAME TABLE ren_a TO ren_b",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{name: "TRUNCATE TABLE", statement: "TRUNCATE TABLE trunct", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},

		// The documented TEMPORARY exceptions.
		{
			name:      "CREATE TEMPORARY TABLE",
			statement: "CREATE TEMPORARY TABLE tt1 (i INT)",
			mysql:     "none", mariadb: "none",
		},
		{
			name:      "CREATE TEMPORARY TABLE AS SELECT",
			statement: "CREATE TEMPORARY TABLE tt3 AS SELECT 1 AS one",
			mysql:     "none", mariadb: "none",
		},
		{name: "DROP TEMPORARY TABLE", statement: "DROP TEMPORARY TABLE tt2", mysql: "none", mariadb: "none"},

		// Table administration.
		{name: "ANALYZE TABLE", statement: "ANALYZE TABLE tgt", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "OPTIMIZE TABLE", statement: "OPTIMIZE TABLE tgt", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "CHECK TABLE", statement: "CHECK TABLE tgt", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "REPAIR TABLE", statement: "REPAIR TABLE mi", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},

		// Server administration.
		{name: "FLUSH TABLES", statement: "FLUSH TABLES", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "FLUSH PRIVILEGES", statement: "FLUSH PRIVILEGES", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "RESET REPLICA", statement: "RESET REPLICA", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{
			name:      "INSTALL PLUGIN",
			statement: "INSTALL PLUGIN mysql_no_login SONAME 'mysql_no_login.so'",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},
		{
			name:      "UNINSTALL PLUGIN",
			statement: "UNINSTALL PLUGIN mysql_no_login",
			mysql:     "a_pre+b_post", mariadb: "a_pre+b_post",
		},

		// Locking. UNLOCK TABLES was measured with no table locked, which is the
		// only state it can meet inside a migration transaction: a LOCK TABLES
		// earlier in the body would already have ended that transaction.
		{name: "LOCK TABLES", statement: "LOCK TABLES led WRITE", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "UNLOCK TABLES", statement: "UNLOCK TABLES", mysql: "none", mariadb: "none"},

		// The two rows where the servers disagree.
		{name: "CACHE INDEX", statement: "CACHE INDEX mi IN default", mysql: "a_pre+b_post", mariadb: "none"},
		{name: "LOAD INDEX INTO CACHE", statement: "LOAD INDEX INTO CACHE mi", mysql: "a_pre+b_post", mariadb: "none"},

		// LOAD DATA is in neither server's implicit-commit set. Reporting it as
		// committing marks rows applied that the rollback took, and the resume
		// then skips the statement that would have loaded them.
		{
			name:      "LOAD DATA",
			statement: "LOAD DATA INFILE '/var/lib/mysql-files/probe.txt' INTO TABLE ld",
			mysql:     "none", mariadb: "none",
		},

		// Transaction control.
		{name: "BEGIN", statement: "BEGIN", mysql: "a_pre", mariadb: "a_pre"},
		{name: "START TRANSACTION", statement: "START TRANSACTION", mysql: "a_pre", mariadb: "a_pre"},
		{name: "COMMIT", statement: "COMMIT", mysql: "a_pre+b_post", mariadb: "a_pre+b_post"},
		{name: "COMMIT AND CHAIN", statement: "COMMIT AND CHAIN", mysql: "a_pre", mariadb: "a_pre"},
		{name: "ROLLBACK", statement: "ROLLBACK", mysql: "b_post", mariadb: "b_post"},
		{name: "SAVEPOINT", statement: "SAVEPOINT sp1", mysql: "none", mariadb: "none"},
		{name: "RELEASE SAVEPOINT", statement: "RELEASE SAVEPOINT sp2", mysql: "none", mariadb: "none"},
		// ROLLBACK TO SAVEPOINT undoes work back to the savepoint, but it
		// neither commits the prefix nor ends the transaction, and work that is
		// still pending is work this accounting already reports as uncommitted.
		{name: "ROLLBACK TO SAVEPOINT", statement: "ROLLBACK TO SAVEPOINT sp4", mysql: "none", mariadb: "none"},

		// A statement with nothing in it cannot be reached by any of the above.
		{name: "a comment alone", statement: "-- nothing here", mysql: "none", mariadb: "none"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(implicitCommitEffectOf(tt.statement, "mysql"), qt.Equals, effectOfSurvivors[tt.mysql])
			c.Assert(implicitCommitEffectOf(tt.statement, "mariadb"), qt.Equals, effectOfSurvivors[tt.mariadb])
		})
	}
}

// TestImplicitCommitEffectOf_DiscardingForms separates the statements that
// throw the open transaction away from the statements that do nothing. The
// probe on [effectOfSurvivors] cannot tell them apart, because both leave
// nothing behind, so these rows were measured with a DDL statement appended:
//
//	START TRANSACTION;
//	INSERT INTO led VALUES (1,'a_pre');
//	<the statement under test>
//	INSERT INTO led VALUES (2,'b_post');
//	CREATE TABLE disc_x (i INT);
//	ROLLBACK;
//	SELECT note FROM led;
//
// The trailing DDL commits whatever is still pending, so `a_pre+b_post` means
// nothing was discarded and `b_post` means the prefix was.
func TestImplicitCommitEffectOf_DiscardingForms(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		statement string
		survivors string
		want      implicitCommitEffect
	}{
		{
			name:      "a plain INSERT discards nothing",
			statement: "INSERT INTO tgt VALUES (1)",
			survivors: "a_pre+b_post",
			want:      implicitCommitNone,
		},
		{
			name:      "ROLLBACK discards the prefix",
			statement: "ROLLBACK",
			survivors: "b_post",
			want:      implicitCommitDiscards,
		},
		{
			name:      "ROLLBACK AND CHAIN discards the prefix too",
			statement: "ROLLBACK AND CHAIN",
			survivors: "b_post",
			want:      implicitCommitDiscards,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(implicitCommitEffectOf(tt.statement, "mysql"), qt.Equals, tt.want)
			c.Assert(implicitCommitEffectOf(tt.statement, "mariadb"), qt.Equals, tt.want)
		})
	}
}

// TestCommittedPrefixAfterRollback pins how much of a rolled-back MySQL-family
// body is still committed.
//
// The row that matters most is "a DDL statement commits everything after it
// too". Measured on MySQL 9.7.1 and MariaDB 11.4.12:
//
//	START TRANSACTION; INSERT INTO led VALUES (1,'one'); CREATE TABLE ddl1 (i INT);
//	INSERT INTO led VALUES (3,'three'); ROLLBACK; SELECT id,note FROM led ORDER BY id;
//	-> rows 1 and 3 both survive
//
// An implicit commit ends the transaction rather than flushing it, so the
// statements after it commit themselves and the ROLLBACK reaches none of them.
// Counting only up to the last committing statement understates the prefix, and
// the retry then re-executes a statement that is already committed.
func TestCommittedPrefixAfterRollback(t *testing.T) {
	c := qt.New(t)

	const dmlOnly = "INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n" +
		"INSERT INTO ledger (id) VALUES (3);\n"
	const ddlThenDML = "CREATE TABLE ledger (id INT);\n" +
		"INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n"
	const dmlThenDDL = "INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n" +
		"CREATE TABLE audit (id INT);\n"
	const dmlThenBegin = "INSERT INTO ledger (id) VALUES (1);\n" +
		"BEGIN;\n" +
		"INSERT INTO ledger (id) VALUES (2);\n" +
		"INSERT INTO ledger (id) VALUES (3);\n"
	const ddlThenRollback = "CREATE TABLE ledger (id INT);\n" +
		"ROLLBACK;\n" +
		"INSERT INTO ledger (id) VALUES (1);\n"
	const dmlThenRollbackThenDDL = "INSERT INTO ledger (id) VALUES (1);\n" +
		"ROLLBACK;\n" +
		"CREATE TABLE audit (id INT);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n"
	const ddlThenLoadIndex = "CREATE TABLE ledger (id INT);\n" +
		"LOAD INDEX INTO CACHE ledger;\n" +
		"INSERT INTO ledger (id) VALUES (1);\n"

	tests := []struct {
		name        string
		sqlText     string
		dialect     string
		executed    int
		failedIndex int
		want        int
	}{
		{
			name:    "DML only loses everything the rollback took",
			sqlText: dmlOnly, dialect: "mysql",
			executed: 1, failedIndex: 2, want: 0,
		},
		{
			name:    "DML only loses a longer prefix too",
			sqlText: dmlOnly, dialect: "mysql",
			executed: 2, failedIndex: 3, want: 0,
		},
		{
			name:    "a DDL statement commits everything after it too",
			sqlText: ddlThenDML, dialect: "mysql",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "a DDL statement commits everything after it on MariaDB too",
			sqlText: ddlThenDML, dialect: "mariadb",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "a failing DDL statement commits the DML before it",
			sqlText: dmlThenDDL, dialect: "mysql",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "BEGIN commits the prefix and puts the rest back at risk",
			sqlText: dmlThenBegin, dialect: "mysql",
			executed: 3, failedIndex: 4, want: 2,
		},
		{
			// The CREATE already ended the transaction, so this ROLLBACK has
			// nothing to throw away and the DDL before it stays committed.
			name:    "a ROLLBACK with no open transaction takes nothing back",
			sqlText: ddlThenRollback, dialect: "mysql",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			// Here the ROLLBACK does undo statement one, while the CREATE after
			// it is durable. The durable set is not a prefix, and zero is the
			// only prefix that claims nothing false.
			name:    "a body that rolls back its own transaction can claim no prefix",
			sqlText: dmlThenRollbackThenDDL, dialect: "mysql",
			executed: 3, failedIndex: 4, want: 0,
		},
		{
			name:    "LOAD INDEX keeps the MySQL prefix committed",
			sqlText: ddlThenLoadIndex, dialect: "mysql",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			// The CREATE ahead of it already ended the transaction, so the
			// answer is the same on MariaDB even though LOAD INDEX commits
			// nothing there.
			name:    "LOAD INDEX keeps the MariaDB prefix committed by the DDL before it",
			sqlText: ddlThenLoadIndex, dialect: "mariadb",
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "nothing executed keeps nothing",
			sqlText: dmlOnly, dialect: "mysql",
			executed: 0, failedIndex: 1, want: 0,
		},
		{
			name:    "an unsplittable body keeps nothing",
			sqlText: "INSERT INTO ledger (id) VALUES (1);", dialect: "mysql",
			executed: 3, failedIndex: 4, want: 0,
		},
		{
			name:    "no failing statement judges only what ran",
			sqlText: dmlThenDDL, dialect: "mysql",
			executed: 2, failedIndex: 0, want: 0,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := committedPrefixAfterRollback(tt.sqlText, tt.dialect, tt.executed, tt.failedIndex)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestRolledBackApplied is the regression guard for stokaro/ptah#887: a
// MySQL-family body that ran under tx-mode file and rolled back used to keep
// the failing statement's predecessors in `applied`, so the next attempt
// resumed past statements the rollback had undone. The PostgreSQL and
// no-transaction rows are the non-interference controls -- correcting MySQL
// must not move either of them.
func TestRolledBackApplied(t *testing.T) {
	c := qt.New(t)

	const dmlOnly = "INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n" +
		"INSERT INTO ledger (id) VALUES (3);\n"
	const ddlThenDML = "CREATE TABLE ledger (id INT);\n" +
		"INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) VALUES (2);\n"

	tests := []struct {
		name        string
		sqlText     string
		dialect     string
		txMode      MigrationTxMode
		executed    int
		failedIndex int
		want        int
	}{
		{
			name:    "MySQL file mode drops a rolled-back DML prefix",
			sqlText: dmlOnly, dialect: "mysql", txMode: MigrationTxModeFile,
			executed: 1, failedIndex: 2, want: 0,
		},
		{
			name:    "MariaDB file mode drops a rolled-back DML prefix",
			sqlText: dmlOnly, dialect: "mariadb", txMode: MigrationTxModeFile,
			executed: 1, failedIndex: 2, want: 0,
		},
		{
			name:    "MySQL file mode keeps everything a committed DDL statement carried",
			sqlText: ddlThenDML, dialect: "mysql", txMode: MigrationTxModeFile,
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "MariaDB file mode keeps everything a committed DDL statement carried",
			sqlText: ddlThenDML, dialect: "mariadb", txMode: MigrationTxModeFile,
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "MySQL all mode drops a rolled-back DML prefix",
			sqlText: dmlOnly, dialect: "mysql", txMode: MigrationTxModeAll,
			executed: 2, failedIndex: 3, want: 0,
		},
		{
			name:    "MySQL no-transaction mode keeps every committed statement",
			sqlText: dmlOnly, dialect: "mysql", txMode: MigrationTxModeNone,
			executed: 2, failedIndex: 3, want: 2,
		},
		{
			name:    "PostgreSQL file mode still drops the whole body",
			sqlText: ddlThenDML, dialect: "postgres", txMode: MigrationTxModeFile,
			executed: 2, failedIndex: 3, want: 0,
		},
		{
			name:    "SQLite file mode still drops the whole body",
			sqlText: ddlThenDML, dialect: "sqlite", txMode: MigrationTxModeFile,
			executed: 2, failedIndex: 3, want: 0,
		},
		{
			name:    "PostgreSQL no-transaction mode keeps every committed statement",
			sqlText: ddlThenDML, dialect: "postgres", txMode: MigrationTxModeNone,
			executed: 2, failedIndex: 3, want: 2,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := rolledBackApplied(tt.sqlText, tt.dialect, tt.txMode, tt.executed, tt.failedIndex)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestMigrationExecutionProgress_RolledBackMySQLBody walks the same correction
// through the error the executor actually raises, so the wiring between the
// failure and the recorded counter is covered and not only the helper.
func TestMigrationExecutionProgress_RolledBackMySQLBody(t *testing.T) {
	c := qt.New(t)

	const dmlOnly = "INSERT INTO ledger (id) VALUES (1);\n" +
		"INSERT INTO ledger (id) SELECT 2 FROM blocker;\n" +
		"INSERT INTO ledger (id) VALUES (3);\n"

	failure := &MigrationExecutionError{
		Statement:      "INSERT INTO ledger (id) SELECT 2 FROM blocker",
		StatementIndex: 2,
		Total:          3,
	}

	progress := migrationExecutionProgress(failure, dmlOnly, "mysql", MigrationTxModeFile)

	c.Assert(progress.Applied, qt.Equals, 0)
	c.Assert(progress.Total, qt.Equals, 3)
	c.Assert(progress.FailedIndex, qt.Equals, 2)
}

// TestMigrationExecutionProgress_CommittedDDLPrefix is the other half of the
// wiring: the body from stokaro/ptah#1356's first blocker, where the DDL at
// statement one committed itself and statement two along with it. Recording 1
// here made the retry re-execute the committed INSERT and duplicate its row.
func TestMigrationExecutionProgress_CommittedDDLPrefix(t *testing.T) {
	c := qt.New(t)

	const ddlThenDML = "CREATE TABLE created (id INT);\n" +
		"INSERT INTO ledger (id, note) VALUES (1, 'one');\n" +
		"INSERT INTO ledger (id, note) SELECT 2, 'two' FROM blocker;\n"

	failure := &MigrationExecutionError{
		Statement:      "INSERT INTO ledger (id, note) SELECT 2, 'two' FROM blocker",
		StatementIndex: 3,
		Total:          3,
	}

	progress := migrationExecutionProgress(failure, ddlThenDML, "mysql", MigrationTxModeFile)

	c.Assert(progress.Applied, qt.Equals, 2)
	c.Assert(progress.Total, qt.Equals, 3)
	c.Assert(progress.FailedIndex, qt.Equals, 3)
}

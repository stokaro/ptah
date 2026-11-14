package lint_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

const (
	atlasFile  = "20240826120919_init.sql"
	nativeUp   = "0000000001_x.up.sql"
	nativeDown = "0000000001_x.down.sql"
)

// transactionControlFindings lints files for dialect and keeps the TX201
// findings, which are what these tests read.
func transactionControlFindings(
	c *qt.C,
	dialect string,
	format migrationfile.DirFormat,
	owner lintext.TransactionOwner,
	files map[string]string,
) []lint.Finding {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(files), lint.Options{
		Dialect:          dialect,
		DirFormat:        format,
		TransactionOwner: owner,
	})
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(findings, func(finding lint.Finding) bool { return finding.Rule != "TX201" })
}

// A migration body that controls transaction state is refused by the migrator
// on every engine, wrapped in its transaction or run without one, so lint
// reports it on every engine. The finding names the statement that opens the
// block, once, and the sites say which line.
func TestLintFS_TransactionControl_ReportsWhatApplyRefuses(t *testing.T) {
	const block = "BEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"
	const none = "-- atlas:txmode none\n\n"
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{"SQLite block the migrator wraps", "sqlite", block, []string{atlasFile + ":1:TX201"}},
		{"SQLite block that runs without a transaction", "sqlite", none + block, []string{atlasFile + ":3:TX201"}},
		{"SQLite stray COMMIT", "sqlite", "INSERT INTO users VALUES (1);\nCOMMIT;\n", []string{atlasFile + ":2:TX201"}},
		{"SQLite two blocks", "sqlite", block + block, []string{atlasFile + ":1:TX201", atlasFile + ":4:TX201"}},
		{"SQLite SAVEPOINT", "sqlite", "SAVEPOINT s;\nINSERT INTO users VALUES (1);\n", []string{atlasFile + ":1:TX201"}},
		{"MySQL START TRANSACTION", "mysql", "START TRANSACTION;\nINSERT INTO users VALUES (1);\nCOMMIT;\n", []string{atlasFile + ":1:TX201"}},
		{"MySQL SET autocommit", "mysql", "SET autocommit = 0;\nINSERT INTO users VALUES (1);\n", []string{atlasFile + ":1:TX201"}},
		{"MariaDB block without a transaction", "mariadb", none + block, []string{atlasFile + ":3:TX201"}},
		{"SQL Server BEGIN TRAN", "sqlserver", "BEGIN TRAN;\nINSERT INTO users VALUES (1);\nCOMMIT TRAN;\n", []string{atlasFile + ":1:TX201"}},
		{"SQL Server SET IMPLICIT_TRANSACTIONS", "sqlserver", "SET IMPLICIT_TRANSACTIONS ON;\nINSERT INTO users VALUES (1);\n",
			[]string{atlasFile + ":1:TX201"}},
		{"CockroachDB block", "cockroachdb", block, []string{atlasFile + ":1:TX201"}},
		{"PostgreSQL block the migrator wraps", "postgres", block, []string{atlasFile + ":1:TX201"}},
		{"PostgreSQL START TRANSACTION", "postgres", "START TRANSACTION;\nCREATE TABLE t (id int);\nCOMMIT;\n", []string{atlasFile + ":1:TX201"}},
		{"PostgreSQL block that runs without a transaction", "postgres", none + block, []string{atlasFile + ":3:TX201"}},
		{"no dialect", "", block, []string{atlasFile + ":1:TX201"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, test.dialect, migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerMigrator, map[string]string{atlasFile: test.sql})

			c.Assert(findingSites(findings), qt.DeepEquals, test.want)
		})
	}
}

// Statements that look like control and are not stay clean, and so does a file
// without any: SQL Server's BEGIN and END delimit a block, so only BEGIN TRAN
// is control there.
func TestLintFS_TransactionControl_LeavesOtherStatementsAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{"SQLite plain file", "sqlite", "CREATE TABLE users (id int);\nINSERT INTO users VALUES (1);\n"},
		{"SQLite literal naming BEGIN", "sqlite", "INSERT INTO users VALUES ('BEGIN');\n"},
		{"MySQL file without control", "mysql", "SET NAMES utf8mb4;\nCREATE TABLE users (id int);\n"},
		{"SQL Server procedure body", "sqlserver", "CREATE PROCEDURE p AS BEGIN SELECT 1; END;\n"},
		{"SQL Server TRY block", "sqlserver", "BEGIN TRY\nSELECT 1;\nEND TRY\nBEGIN CATCH\nSELECT 2;\nEND CATCH\n"},
		{"PostgreSQL DO block", "postgres", "DO $$ BEGIN PERFORM 1; END $$;\n"},
		{"PostgreSQL escape string naming COMMIT", "postgres", "INSERT INTO users VALUES (E'a\\';COMMIT;');\n"},
		{"CockroachDB escape string naming BEGIN", "cockroachdb", "INSERT INTO users VALUES (e'\\'; BEGIN;');\n"},
		{"PostgreSQL file without control", "postgres", "CREATE TABLE users (id int);\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, test.dialect, migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerMigrator, map[string]string{atlasFile: test.sql})

			c.Assert(findings, qt.HasLen, 0)
		})
	}
}

// A run for a runner that opens its own transactions, as golang-migrate does,
// keeps the transaction around each file on an engine whose migrator services
// read no body's own transactions, so the migrator refuses a block there as it
// does in any wrapped file, and lint reports it the same way.
func TestLintFS_TransactionControl_ReportsAWrappedFileUnderARunnerThatOpensItsOwn(t *testing.T) {
	const block = "BEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{"SQLite", "sqlite", block},
		{"MySQL", "mysql", "START TRANSACTION;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"},
		{"SQL Server", "sqlserver", "BEGIN TRAN;\nINSERT INTO users VALUES (1);\nCOMMIT TRAN;\n"},
		{"CockroachDB", "cockroachdb", block},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, test.dialect, migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findingSites(findings), qt.DeepEquals, []string{atlasFile + ":1:TX201"})
			c.Assert(findings[0].Message, qt.Contains, "the migrator runs this file in one transaction")
		})
	}
}

// The rollback half is judged too: the migrator refuses transaction control in
// a down body it runs inside its transaction as it does in an up body. It does
// so wherever it opens the transaction around the file, which on an engine
// whose migrator reads no file's own transactions includes a runner that
// opens its own.
func TestLintFS_TransactionControl_ReadsTheRollbackHalf(t *testing.T) {
	files := map[string]string{
		nativeUp:   "CREATE TABLE t (id int);\n",
		nativeDown: "BEGIN;\nDROP TABLE t;\nCOMMIT;\n",
	}
	tests := []struct {
		name    string
		dialect string
		owner   lintext.TransactionOwner
	}{
		{"SQLite, the migrator wraps the file", "sqlite", lintext.TransactionOwnerMigrator},
		{"SQLite, the file opens its own", "sqlite", lintext.TransactionOwnerFile},
		{"PostgreSQL, the migrator wraps the file", "postgres", lintext.TransactionOwnerMigrator},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, test.dialect, migrationfile.DirFormatAuto, test.owner, files)

			c.Assert(findingSites(findings), qt.DeepEquals, []string{nativeDown + ":1:TX201"})
			c.Assert(findings[0].Message, qt.Contains, "the rollback stops at this BEGIN")
		})
	}
}

// Under a runner that opens no transaction around a file, PostgreSQL's
// migrator rolls a down file back in the transactions the file opens itself,
// as it applies an up file, and refuses the same forms before any migration is
// rolled back. The down file is judged by that reading.
func TestLintFS_TransactionControl_ReadsTheRollbackHalfAsTheMigratorRollsItBack(t *testing.T) {
	const up = "CREATE TABLE t (id int);\n"
	tests := []struct {
		name string
		down string
		want []string
	}{
		{"a block the file opens and closes", "BEGIN;\nDROP TABLE t;\nCOMMIT;\n", make([]string, 0)},
		{"two blocks", "BEGIN;\nDROP TABLE t;\nCOMMIT;\nBEGIN;\nSELECT 1;\nCOMMIT;\n", make([]string, 0)},
		{"a block ended by ROLLBACK", "BEGIN;\nDROP TABLE t;\nROLLBACK;\n", []string{nativeDown + ":3:TX201"}},
		{"a block left open", "BEGIN;\nDROP TABLE t;\n", []string{nativeDown + ":1:TX201"}},
		{"a block opened inside another", "BEGIN;\nBEGIN;\nDROP TABLE t;\nCOMMIT;\n", []string{nativeDown + ":2:TX201"}},
		{"a block in a file that asks for a transaction around it", "-- atlas:txmode file\n\nBEGIN;\nDROP TABLE t;\nCOMMIT;\n",
			[]string{nativeDown + ":3:TX201"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAuto, lintext.TransactionOwnerFile,
				map[string]string{nativeUp: up, nativeDown: test.down})

			c.Assert(findingSites(findings), qt.DeepEquals, test.want)
		})
	}
}

// A down file the reading refuses is reported for the rollback it stops,
// where an up file's finding names the apply.
func TestLintFS_TransactionControl_RefusedDownFileNamesTheRollback(t *testing.T) {
	c := qt.New(t)

	findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAuto, lintext.TransactionOwnerFile,
		map[string]string{
			nativeUp:   "BEGIN;\nCREATE TABLE t (id int);\nROLLBACK;\n",
			nativeDown: "BEGIN;\nDROP TABLE t;\nROLLBACK;\n",
		})

	c.Assert(findingSites(findings), qt.DeepEquals, []string{nativeDown + ":3:TX201", nativeUp + ":3:TX201"})
	c.Assert(findings[0].Message, qt.Contains, "refuses the file before any migration is rolled back: ")
	c.Assert(findings[1].Message, qt.Contains, "refuses the file before any migration runs: ")
}

// The message says what the migrator does in the mode the file runs in.
func TestLintFS_TransactionControl_MessageNamesTheMode(t *testing.T) {
	const block = "BEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"wrapped", block, "the migrator runs this file in one transaction and refuses a statement that controls " +
			"transaction state, so the apply stops at this BEGIN; remove the transaction control"},
		{"without a transaction", "-- atlas:txmode none\n\n" + block, "the migrator refuses transaction control in a " +
			"migration that runs without a transaction and commits each statement on its own, so the apply stops at " +
			"this BEGIN; remove the transaction control"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "sqlite", migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerMigrator, map[string]string{atlasFile: test.sql})

			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Equals, test.want)
			c.Assert(findings[0].Severity, qt.Equals, lint.SeverityWarning)
		})
	}
}

// Under a runner that opens no transaction around a file, PostgreSQL applies
// the transactions an up file opens itself, and a block opened inside another
// the file has not closed is reported: the file applies, and the first COMMIT
// ends what the author wrote as two blocks. A savepoint rollback inside a block
// keeps it open.
func TestLintFS_TransactionControl_NestedBlockUnderARunnerThatOpensItsOwn(t *testing.T) {
	const nested = "a transaction block is already open since the BEGIN on line %d; PostgreSQL ignores this " +
		"BEGIN with a warning, and the first COMMIT ends both blocks"
	tests := []struct {
		name    string
		sql     string
		site    string
		message string
	}{
		{"a BEGIN inside an open block", "BEGIN;\nBEGIN;\nCREATE TABLE t (id int);\nCOMMIT;\n",
			atlasFile + ":2:TX201", fmt.Sprintf(nested, 1)},
		{"START TRANSACTION inside an open block", "BEGIN;\nSTART TRANSACTION;\nCOMMIT;\n",
			atlasFile + ":2:TX201", fmt.Sprintf(nested, 1)},
		{"a block that rolls back to a savepoint stays open",
			"BEGIN;\nSAVEPOINT s;\nROLLBACK TO SAVEPOINT s;\nBEGIN;\nCOMMIT;\n", atlasFile + ":4:TX201", fmt.Sprintf(nested, 1)},
		{"a file whose directive runs it without a transaction", "-- atlas:txmode none\n\nBEGIN;\nBEGIN;\nCOMMIT;\n",
			atlasFile + ":4:TX201", fmt.Sprintf(nested, 3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findingSites(findings), qt.DeepEquals, []string{test.site})
			c.Assert(findings[0].Message, qt.Equals, test.message)
		})
	}
}

// The transactions an up file opens itself are read by the reading the
// migrator applies the directory with, so a form it refuses before any
// migration runs is reported on the statement the refusal names, and nothing
// else in the file is.
func TestLintFS_TransactionControl_ReportsWhatTheOwnTransactionReadingRefuses(t *testing.T) {
	const refused = "the migrator runs the transactions this file opens itself and refuses the file before any " +
		"migration runs: "
	tests := []struct {
		name   string
		sql    string
		site   string
		reason string
	}{
		{"a block ended by ROLLBACK", "BEGIN;\nINSERT INTO users VALUES (1);\nROLLBACK;\n", atlasFile + ":3:TX201", "ROLLBACK"},
		{"a block ended by ABORT", "START TRANSACTION;\nABORT;\n", atlasFile + ":2:TX201", "ABORT"},
		{"a chained commit", "BEGIN;\nCOMMIT AND CHAIN;\nCOMMIT;\n", atlasFile + ":2:TX201", "CHAIN"},
		{"a savepoint outside a block", "SAVEPOINT s;\nINSERT INTO users VALUES (1);\n", atlasFile + ":1:TX201",
			"SAVEPOINT outside a transaction the file opened"},
		{"a block the file leaves open", "INSERT INTO users VALUES (1);\nBEGIN;\nINSERT INTO users VALUES (2);\n",
			atlasFile + ":2:TX201", "the transaction is never closed by the file"},
		{"PREPARE TRANSACTION", "BEGIN;\nINSERT INTO users VALUES (1);\nPREPARE TRANSACTION 'x';\n",
			atlasFile + ":3:TX201", "PREPARE TRANSACTION"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findingSites(findings), qt.DeepEquals, []string{test.site})
			c.Assert(findings[0].Message, qt.Contains, refused)
			c.Assert(findings[0].Message, qt.Contains, test.reason)
		})
	}
}

// A file that opens its own transaction and whose directive asks for one
// around it is refused before any migration runs, and so is transaction
// control in a file that opens no block, which keeps the transaction around
// it: SET TRANSACTION alone is not a block of the file's own.
func TestLintFS_TransactionControl_ReportsAWrappedReadingUnderARunnerThatOpensItsOwn(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"a directive asking for a transaction around a file with its own",
			"-- atlas:txmode file\n\nBEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n",
			"the file asks for a transaction around it and opens its own"},
		{"SET TRANSACTION in a file without a block",
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\nINSERT INTO users VALUES (1);\n",
			"the migrator runs this file in one transaction"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// A file PostgreSQL applies with its own transactions as written is clean: two
// blocks one after the other, a COMMIT outside every block, which ends the
// transaction the statements before it run in, and a savepoint inside a block.
func TestLintFS_TransactionControl_LeavesARunnableOwnTransactionFileAlone(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"two blocks one after the other", "BEGIN;\nSELECT 1;\nCOMMIT;\nBEGIN;\nSELECT 2;\nEND;\n"},
		{"a COMMIT outside every block", "INSERT INTO users VALUES (1);\nCOMMIT;\nINSERT INTO users VALUES (2);\n"},
		{"a savepoint inside a block", "BEGIN;\nSAVEPOINT s;\nINSERT INTO users VALUES (1);\nRELEASE SAVEPOINT s;\nCOMMIT;\n"},
		{"a file without control", "CREATE TABLE users (id int);\n"},
		{"a block in a file whose directive runs it without a transaction",
			"-- atlas:txmode none\n\nBEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "postgres", migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findings, qt.HasLen, 0)
		})
	}
}

// A file of one statement in a golang-migrate directory runs without a
// transaction only on an engine whose migrator reads a file's own
// transactions; elsewhere the migrator keeps the transaction around it, and
// the message says which the apply does.
func TestLintFS_TransactionControl_MessageNamesTheModeUnderARunnerThatOpensItsOwn(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{"SQLite keeps the transaction around the file", "sqlite", "SAVEPOINT s;\n",
			"the migrator runs this file in one transaction and refuses"},
		{"PostgreSQL runs the statement on its own", "postgres", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n",
			"the migrator refuses transaction control in a migration that runs without a transaction"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, test.dialect, migrationfile.DirFormatAtlas,
				lintext.TransactionOwnerFile, map[string]string{atlasFile: test.sql})

			c.Assert(findingSites(findings), qt.DeepEquals, []string{atlasFile + ":1:TX201"})
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// A run that names no dialect cannot know the engine, and reads a file of a
// golang-migrate directory the way PostgreSQL's migrator runs it: a block the
// file opens and closes is not reported, in an up file or a down file, and a
// form that reading refuses is.
func TestLintFS_TransactionControl_NoDialectReadsAGolangMigrateFileAsPostgreSQLDoes(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{"a block the file opens and closes", map[string]string{nativeUp: "BEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"},
			make([]string, 0)},
		{"a block opened inside another", map[string]string{nativeUp: "BEGIN;\nBEGIN;\nCOMMIT;\n"},
			[]string{nativeUp + ":2:TX201"}},
		{"a block ended by ROLLBACK", map[string]string{nativeUp: "BEGIN;\nINSERT INTO users VALUES (1);\nROLLBACK;\n"},
			[]string{nativeUp + ":3:TX201"}},
		{"a down file", map[string]string{nativeUp: "CREATE TABLE t (id int);\n", nativeDown: "BEGIN;\nDROP TABLE t;\nCOMMIT;\n"},
			make([]string, 0)},
		{"a down file ended by ROLLBACK", map[string]string{nativeUp: "CREATE TABLE t (id int);\n",
			nativeDown: "BEGIN;\nDROP TABLE t;\nROLLBACK;\n"}, []string{nativeDown + ":3:TX201"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := transactionControlFindings(c, "", migrationfile.DirFormatAuto, lintext.TransactionOwnerFile, test.files)

			c.Assert(findingSites(findings), qt.DeepEquals, test.want)
		})
	}
}

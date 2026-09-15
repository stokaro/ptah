//go:build integration

package integration_test

// What `ptah schema apply --tx-mode all` means on a target that commits DDL as
// it runs.
//
// `migration/migrator` asks capability.TransactionalDDL before it opens the
// batch transaction, and answers `tx-mode all is not supported for dialect
// "mysql"` where the key is false. The declarative apply asks nothing:
// `internal/atlasschema.applyStatements` maps both `file` and `all` onto
// `sqliterebuild.BeginTransaction`, runs the plan, and on a failure calls
// `Rollback`. MySQL commits each DDL statement as it executes, so that rollback
// returns nil having undone nothing, and the operator is told the apply failed
// while the database holds part of the plan.
//
// Both engines are here because either one alone is satisfied by a harness that
// measures nothing. A run that leaves the first statement's table behind says
// nothing on its own -- a test that never reached the second statement looks
// the same -- and a run that leaves nothing behind is also what a target does
// when the plan never started. PostgreSQL is where the mode does what it says,
// so it is what proves this file can see a rollback at all.
//
// The `none` rows are the other control. On MySQL they establish that `all`
// buys the operator nothing: the catalog holds the same thing either way. On
// PostgreSQL they establish the opposite, that the difference between the two
// rows is the transaction mode rather than anything else about the plan.
//
// The catalog is the observable rather than the exit code. Both engines exit 2
// and name the statement the server refused, so a reader of the diagnostic
// cannot tell the two outcomes apart; what differs is what is left in the
// database afterwards.
//
// These assertions record measured behavior, not a promise. Should
// `schema apply` grow the capability check the migrator already has, the MySQL
// rows are the ones that change: the run would refuse the mode before touching
// the target, and the witness table would not exist.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for the catalog reads below
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for the catalog reads below

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// txModeAllSeededTable is the table the target already holds, carrying two rows
// that share an email.
const txModeAllSeededTable = "ptah_txall_seeded"

// txModeAllWitnessTable is what the first statement of the plan creates, and
// the only question either test asks of the catalog afterwards.
const txModeAllWitnessTable = "ptah_txall_witness"

// txModeAllDesiredSchema declares the seeded table exactly as it already
// exists, plus a unique constraint the data cannot satisfy, plus one table that
// does not exist yet.
//
// The plan this produces is two statements in a fixed order: create the witness
// table, then add the constraint. The second one fails on the data rather than
// on its syntax, which is what makes the same file usable against both engines
// and keeps the failure independent of how either dialect renders DDL.
const txModeAllDesiredSchema = `CREATE TABLE ptah_txall_seeded (
  id INT NOT NULL,
  email VARCHAR(64) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT ptah_txall_seeded_email_key UNIQUE (email)
);

CREATE TABLE ptah_txall_witness (
  id INT NOT NULL,
  PRIMARY KEY (id)
);
`

// TestSchemaApplyTxModeAllLeavesMySQLHoldingPartOfThePlan is the finding.
//
// MySQL 26.7.0 answers the second statement with `Error 1062 (23000):
// Duplicate entry`, and the witness table the first statement created is still
// there under `--tx-mode all`.
func TestSchemaApplyTxModeAllLeavesMySQLHoldingPartOfThePlan(t *testing.T) {
	tests := []struct {
		name   string
		txMode string
	}{
		{name: "all", txMode: "all"},
		{name: "none", txMode: "none"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newTxModeAllMySQLTarget(c, ctx)
			applied := runTxModeAllApply(c, target.url, test.txMode)

			assertTxModeAllPlanFailedAtItsSecondStatement(c, applied, "Duplicate entry")
			c.Assert(txModeAllMySQLHasTable(c, ctx, target, txModeAllWitnessTable), qt.IsTrue,
				txModeAllCommentf(applied))
		})
	}
}

// TestSchemaApplyTxModeAllRollsPostgreSQLBackAsAUnit is the control the finding
// needs.
//
// The same plan against a target with transactional DDL: `all` leaves nothing
// behind, `none` leaves the witness table. Without this pair, the MySQL
// assertion above would be satisfied by a test that measured the wrong thing.
func TestSchemaApplyTxModeAllRollsPostgreSQLBackAsAUnit(t *testing.T) {
	tests := []struct {
		name        string
		txMode      string
		wantWitness bool
	}{
		{name: "all", txMode: "all", wantWitness: false},
		{name: "none", txMode: "none", wantWitness: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newTxModeAllPostgresTarget(c, ctx)
			applied := runTxModeAllApply(c, target.url, test.txMode)

			assertTxModeAllPlanFailedAtItsSecondStatement(c, applied, "could not create unique index")
			c.Assert(txModeAllPostgresHasTable(c, ctx, target, txModeAllWitnessTable),
				qt.Equals, test.wantWitness, txModeAllCommentf(applied))
		})
	}
}

// txModeAllMySQLTarget is one throwaway MySQL database, with both spellings of
// its address.
//
// `admin` comes from dbtarget.DriverDSN because go-sql-driver reads a `mysql://`
// prefix as part of the username, while `url` is what Ptah connects with and
// carries the scheme its dialect is resolved from.
type txModeAllMySQLTarget struct {
	admin    *sql.DB
	database string
	url      string
}

// newTxModeAllMySQLTarget creates a database of its own for one row, seeds it,
// and removes it afterwards.
//
// The administrative account rather than the ordinary one: `schema apply`
// reconciles the WHOLE target, so a shared database would put a drop for every
// other test's table into the plan under measurement here.
func newTxModeAllMySQLTarget(c *qt.C, ctx context.Context) txModeAllMySQLTarget {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, dbtarget.MySQLAdmin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_txall_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), adminDB, database) })

	target := txModeAllMySQLTarget{
		admin:    adminDB,
		database: database,
		// replaceMySQLDatabaseName rather than replaceDatabaseName: the latter
		// goes through url.Parse, and the driver-style host in
		// `mysql://root:***@tcp(host:3306)/mysql` makes it answer
		// `invalid port ":3306)"`.
		url: replaceMySQLDatabaseName(c, dbtarget.URL(c, dbtarget.MySQLAdmin), database),
	}
	for _, statement := range []string{
		fmt.Sprintf("CREATE TABLE `%s`.`%s` "+
			"(id INT NOT NULL, email VARCHAR(64) NOT NULL, PRIMARY KEY (id))",
			database, txModeAllSeededTable),
		fmt.Sprintf("INSERT INTO `%s`.`%s` (id, email) VALUES (1, 'dup@example.test'), (2, 'dup@example.test')",
			database, txModeAllSeededTable),
	} {
		_, execErr := adminDB.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	return target
}

// txModeAllPostgresTarget is the same for PostgreSQL, where one handle serves
// both the setup and the catalog read.
type txModeAllPostgresTarget struct {
	db  *sql.DB
	url string
}

// newTxModeAllPostgresTarget creates and seeds a throwaway PostgreSQL database
// for one row.
func newTxModeAllPostgresTarget(c *qt.C, ctx context.Context) txModeAllPostgresTarget {
	c.Helper()

	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	database := fmt.Sprintf("ptah_txall_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, database) })

	rowDB, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, database))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rowDB.Close() })

	target := txModeAllPostgresTarget{db: rowDB, url: replaceDatabaseName(c, adminURL, database)}
	for _, statement := range []string{
		fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL, email VARCHAR(64) NOT NULL, PRIMARY KEY (id))",
			txModeAllSeededTable),
		fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'dup@example.test'), (2, 'dup@example.test')",
			txModeAllSeededTable),
	} {
		_, execErr := rowDB.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	return target
}

// runTxModeAllApply drives the shipped binary over the failing plan.
//
// The process rather than the cobra tree in memory: the exit code is part of
// what an operator reads, and an in-process run compares an error value against
// a status nobody produced. It runs in a directory of its own so that no
// project config beside the test decides anything.
func runTxModeAllApply(c *qt.C, dbURL, txMode string) clirun.Result {
	c.Helper()

	work := c.TempDir()
	schemaPath := filepath.Join(work, "desired.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(txModeAllDesiredSchema), 0o600), qt.IsNil)

	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--tx-mode", txMode,
		"--auto-approve")
}

// assertTxModeAllPlanFailedAtItsSecondStatement establishes the premise every
// catalog assertion rests on.
//
// The plan has to be the two statements in that order, and the run has to have
// failed on the second one for the reason this fixture arranges. A plan that
// emitted the constraint first would run nothing before the failure, so both
// engines would leave no witness table and the MySQL rows would pass while
// measuring nothing at all.
func assertTxModeAllPlanFailedAtItsSecondStatement(c *qt.C, applied clirun.Result, serverRefusal string) {
	c.Helper()

	c.Assert(applied.ExitCode, qt.Equals, 2, txModeAllCommentf(applied))
	c.Assert(applied.Stderr, qt.Contains, serverRefusal, txModeAllCommentf(applied))
	c.Assert(applied.Stderr, qt.Contains, "ptah_txall_seeded_email_key", txModeAllCommentf(applied))

	c.Assert(applied.Stdout, qt.Contains, txModeAllWitnessTable, txModeAllCommentf(applied))
	c.Assert(applied.Stdout, qt.Contains, "ADD CONSTRAINT", txModeAllCommentf(applied))
	c.Assert(
		strings.Index(applied.Stdout, "CREATE TABLE") < strings.Index(applied.Stdout, "ADD CONSTRAINT"),
		qt.IsTrue, txModeAllCommentf(applied))
}

// txModeAllMySQLHasTable asks the catalog rather than Ptah.
//
// Reading the answer back through the same reader that planned the apply would
// let one misunderstanding satisfy both sides of the question.
func txModeAllMySQLHasTable(c *qt.C, ctx context.Context, target txModeAllMySQLTarget, table string) bool {
	c.Helper()

	var count int
	c.Assert(target.admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		target.database, table).Scan(&count), qt.IsNil)
	return count > 0
}

// txModeAllPostgresHasTable is the same question of pg_tables.
func txModeAllPostgresHasTable(c *qt.C, ctx context.Context, target txModeAllPostgresTarget, table string) bool {
	c.Helper()

	var count int
	c.Assert(target.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = $1",
		table).Scan(&count), qt.IsNil)
	return count > 0
}

// txModeAllCommentf attaches both streams to a failure, since the plan and the
// diagnostic land on different ones and either can explain an unexpected
// result.
func txModeAllCommentf(applied clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", applied.ExitCode, applied.Stdout, applied.Stderr)
}

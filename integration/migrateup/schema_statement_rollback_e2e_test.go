//go:build integration

package migrateup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// rollbackTarget is a database of a test's own, and a direct handle on it.
type rollbackTarget struct {
	url  string
	conn *dbschema.DatabaseConnection
}

// newRollbackTarget creates the database. The MySQL family's test account may
// create only the ptah_rt_ prefix.
func newRollbackTarget(c *qt.C, engine dbtarget.Engine) *rollbackTarget {
	c.Helper()
	address := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_rt_rollback_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	target, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.Background(), dropRollbackDatabase(engine, name))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE seed (id INT PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	return &rollbackTarget{url: target, conn: conn}
}

// dropRollbackDatabase drops the database, ending the sessions PostgreSQL keeps
// open on it.
func dropRollbackDatabase(engine dbtarget.Engine, name string) string {
	if engine == dbtarget.PostgreSQL {
		return "DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"
	}
	return "DROP DATABASE IF EXISTS " + name
}

// rowsIn counts the rows of table, and answers -1 when the table is not there.
func (r *rollbackTarget) rowsIn(c *qt.C, table string) int {
	c.Helper()
	var rows int
	if err := r.conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM "+table).Scan(&rows); err != nil {
		return -1
	}
	return rows
}

// migrationsDir writes one migration per body, and a down file for each.
func migrationsDir(c *qt.C, down string, bodies ...string) string {
	c.Helper()
	dir := c.TempDir()
	for index, body := range bodies {
		base := filepath.Join(dir, fmt.Sprintf("%010d_m", index+1))
		c.Assert(os.WriteFile(base+".up.sql", []byte(body), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(base+".down.sql", []byte(down), 0o600), qt.IsNil)
	}
	return dir
}

// runUp runs `migrations up --json` and reads the document.
func (r *rollbackTarget) runUp(c *qt.C, dir string, extra ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	args := append([]string{"migrations", "up", "--db-url", r.url, "--migrations-dir", dir, "--json"}, extra...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// dirtyRow reads what the history records, through the command a person reads it
// with.
func (r *rollbackTarget) dirtyRow(c *qt.C, dir string) *migrator.MigrationRevision {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "status", "--db-url", r.url, "--migrations-dir", dir, "--json")
	var status migrator.MigrationStatus
	c.Assert(json.Unmarshal([]byte(result.Stdout), &status), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return status.DirtyRevision
}

// rollbackAnswer is what one engine reports for one body and what it holds after.
type rollbackAnswer struct {
	outcome migrator.RunOutcome
	applied int
	total   int
	// created and seeded are the rows read back from the table the body creates
	// and the table it writes to, with -1 for a table that is not there.
	created int
	seeded  int
}

// rollbackEngines are the engines the default transaction mode runs a body on and
// whose answers differ. The first three answer differently from each other, and
// the MySQL family answers through a witness of its own.
var rollbackEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "postgresql", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	{name: "mysql", engine: dbtarget.MySQL},
	{name: "mariadb", engine: dbtarget.MariaDB},
}

const (
	createCreated = "CREATE TABLE created (id INT PRIMARY KEY);\n"
	insertCreated = "INSERT INTO created (id) VALUES (1);\n"
	insertSeed    = "INSERT INTO seed (id) VALUES (1);\n"
	insertAbsent  = "INSERT INTO absent (id) VALUES (1);\n"
)

// TestFailedMigrationReportsWhatStayedE2E_FailurePath runs one body per file in the
// default transaction mode and holds the document to what the server holds. The
// database is read back, because that is the answer the outcome makes a claim
// about: `failed` says nothing stayed, `partial` names the leading statements that
// did, and `unknown` says no count describes them.
func TestFailedMigrationReportsWhatStayedE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		answers map[dbtarget.Engine]rollbackAnswer
	}{
		{
			name: "a schema statement, then a failure",
			body: createCreated + insertAbsent,
			answers: map[dbtarget.Engine]rollbackAnswer{
				dbtarget.PostgreSQL:  {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.CockroachDB: {outcome: migrator.RunOutcomePartial, applied: 1, total: 2},
				dbtarget.YugabyteDB:  {outcome: migrator.RunOutcomePartial, applied: 1, total: 2},
				dbtarget.MySQL:       {outcome: migrator.RunOutcomePartial, applied: 1, total: 2},
				dbtarget.MariaDB:     {outcome: migrator.RunOutcomePartial, applied: 1, total: 2},
			},
		},
		{
			name: "a schema statement, data, then a failure",
			body: createCreated + insertCreated + insertAbsent,
			answers: map[dbtarget.Engine]rollbackAnswer{
				dbtarget.PostgreSQL:  {outcome: migrator.RunOutcomeFailed, total: 3, created: -1},
				dbtarget.CockroachDB: {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, created: 1},
				dbtarget.YugabyteDB:  {outcome: migrator.RunOutcomePartial, applied: 1, total: 3},
				dbtarget.MySQL:       {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, created: 1},
				dbtarget.MariaDB:     {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, created: 1},
			},
		},
		{
			name: "data, a schema statement, then a failure",
			body: insertSeed + createCreated + insertAbsent,
			answers: map[dbtarget.Engine]rollbackAnswer{
				dbtarget.PostgreSQL:  {outcome: migrator.RunOutcomeFailed, total: 3, created: -1},
				dbtarget.CockroachDB: {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, seeded: 1},
				dbtarget.YugabyteDB:  {outcome: migrator.RunOutcomeUnknown, total: 3},
				dbtarget.MySQL:       {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, seeded: 1},
				dbtarget.MariaDB:     {outcome: migrator.RunOutcomePartial, applied: 2, total: 3, seeded: 1},
			},
		},
		{
			name: "data, then a schema statement the server refuses",
			body: insertSeed + "CREATE TABLE seed (id INT PRIMARY KEY);\n",
			answers: map[dbtarget.Engine]rollbackAnswer{
				dbtarget.PostgreSQL:  {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.CockroachDB: {outcome: migrator.RunOutcomePartial, applied: 1, total: 2, created: -1, seeded: 1},
				dbtarget.YugabyteDB:  {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.MySQL:       {outcome: migrator.RunOutcomeUnknown, applied: 1, total: 2, created: -1, seeded: 1},
				dbtarget.MariaDB:     {outcome: migrator.RunOutcomeUnknown, applied: 1, total: 2, created: -1, seeded: 1},
			},
		},
		{
			name: "data alone, then a failure",
			body: insertSeed + insertAbsent,
			answers: map[dbtarget.Engine]rollbackAnswer{
				dbtarget.PostgreSQL:  {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.CockroachDB: {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.YugabyteDB:  {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.MySQL:       {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
				dbtarget.MariaDB:     {outcome: migrator.RunOutcomeFailed, total: 2, created: -1},
			},
		},
	}
	for _, engine := range rollbackEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newRollbackTarget(c, engine.engine)
				want := test.answers[engine.engine]

				result, document := target.runUp(c, migrationsDir(c, "SELECT 1;\n", test.body))

				c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(document.Outcome, qt.Equals, want.outcome, qt.Commentf("stdout:\n%s", result.Stdout))
				c.Assert(document.Status, qt.IsNotNil)
				c.Assert(document.Status.DirtyRevision, qt.IsNotNil)
				c.Assert(document.Status.DirtyRevision.Applied, qt.Equals, want.applied)
				c.Assert(document.Status.DirtyRevision.Total, qt.Equals, want.total)
				c.Assert(target.rowsIn(c, "created"), qt.Equals, want.created)
				c.Assert(target.rowsIn(c, "seed"), qt.Equals, want.seeded)
			})
		}
	}
}

// TestCleanMigrationReportsAppliedE2E_HappyPath is the control: a run that does not
// fail reports what it reported, on every engine, whatever the body mixes.
func TestCleanMigrationReportsAppliedE2E_HappyPath(t *testing.T) {
	bodies := []struct {
		name string
		body string
	}{
		{name: "schema statements", body: createCreated + "CREATE TABLE created2 (id INT PRIMARY KEY);\n"},
		{name: "a schema statement, then data", body: createCreated + insertCreated + insertSeed},
		{name: "data, then a schema statement", body: insertSeed + createCreated + insertCreated},
	}
	for _, engine := range rollbackEngines {
		for _, test := range bodies {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newRollbackTarget(c, engine.engine)

				result, document := target.runUp(c, migrationsDir(c, "SELECT 1;\n", test.body))

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
				c.Assert(document.Status.DirtyRevision, qt.IsNil)
				c.Assert(document.Applied, qt.DeepEquals, []int64{1})
				c.Assert(target.rowsIn(c, "created") >= 0, qt.IsTrue)
			})
		}
	}
}

// TestFailedMigrationResumesPastWhatStayedE2E_HappyPath is the point of recording
// the count: the recovery the document points at finishes the migration. With the
// count at zero the retry would run the CREATE TABLE again, and the server would
// refuse it for as long as anyone retried.
func TestFailedMigrationResumesPastWhatStayedE2E_HappyPath(t *testing.T) {
	for _, engine := range rollbackEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, engine.engine)
			dir := migrationsDir(c, "SELECT 1;\n", createCreated+insertAbsent)
			target.runUp(c, dir)
			_, err := target.conn.ExecContext(c.Context(), "CREATE TABLE absent (id INT PRIMARY KEY)")
			c.Assert(err, qt.IsNil)

			result, document := target.runUp(c, dir, "--allow-dirty")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(target.rowsIn(c, "created"), qt.Equals, 0)
			c.Assert(target.rowsIn(c, "absent"), qt.Equals, 1)
		})
	}
}

// TestFailedResumeCountsFromWhereItStartedE2E_FailurePath fails a second time. The
// second attempt starts past what the first one left, and the row has to add what
// the second one left to that. On YugabyteDB the data the second attempt ran ahead
// of its schema statement went, so no count describes the result, and the row keeps
// the count the first attempt proved.
func TestFailedResumeCountsFromWhereItStartedE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		wantOutcome migrator.RunOutcome
		wantApplied int
		wantAbsent  int
	}{
		{name: "cockroachdb", engine: dbtarget.CockroachDB, wantOutcome: migrator.RunOutcomePartial, wantApplied: 3, wantAbsent: 1},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, wantOutcome: migrator.RunOutcomeUnknown, wantApplied: 1, wantAbsent: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, test.engine)
			dir := migrationsDir(c, "SELECT 1;\n",
				createCreated+insertAbsent+"CREATE TABLE later (id INT PRIMARY KEY);\nINSERT INTO missing (id) VALUES (1);\n")
			_, first := target.runUp(c, dir)
			c.Assert(first.Status.DirtyRevision.Applied, qt.Equals, 1)
			_, err := target.conn.ExecContext(c.Context(), "CREATE TABLE absent (id INT PRIMARY KEY)")
			c.Assert(err, qt.IsNil)

			_, second := target.runUp(c, dir, "--allow-dirty")

			c.Assert(second.Outcome, qt.Equals, test.wantOutcome)
			c.Assert(second.Status.DirtyRevision.Applied, qt.Equals, test.wantApplied)
			c.Assert(second.Status.DirtyRevision.Total, qt.Equals, 4)
			c.Assert(target.rowsIn(c, "later"), qt.Equals, 0)
			c.Assert(target.rowsIn(c, "absent"), qt.Equals, test.wantAbsent)
		})
	}
}

// TestFailedCommitReportsWhatStayedE2E_FailurePath fails at the commit, on a
// constraint the transaction deferred. Every statement ran, the schema statements
// of YugabyteDB stay, and the data goes with the commit. PostgreSQL is the control.
// One transaction for the run has one row and no count for what stayed.
func TestFailedCommitReportsWhatStayedE2E_FailurePath(t *testing.T) {
	const body = "CREATE TABLE parent (id INT PRIMARY KEY);\n" +
		"CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED);\n" +
		"INSERT INTO child (id, parent_id) VALUES (1, 99);\n"
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		txMode      string
		wantOutcome migrator.RunOutcome
		wantApplied int
		wantChild   int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, txMode: "file", wantOutcome: migrator.RunOutcomeFailed, wantChild: -1},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, txMode: "file", wantOutcome: migrator.RunOutcomePartial, wantApplied: 2},
		{name: "yugabytedb, one transaction", engine: dbtarget.YugabyteDB, txMode: "all", wantOutcome: migrator.RunOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, test.engine)

			result, document := target.runUp(c, migrationsDir(c, "SELECT 1;\n", body), "--tx-mode", test.txMode)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(document.Failure.Commit, qt.IsTrue)
			c.Assert(document.Outcome, qt.Equals, test.wantOutcome)
			c.Assert(document.Status.DirtyRevision.Applied, qt.Equals, test.wantApplied)
			c.Assert(target.rowsIn(c, "child"), qt.Equals, test.wantChild)
		})
	}
}

// TestRollbackThatLeavesNoCountIsEndedByHandE2E_FailurePath holds the one shape no
// count describes, on the engine that produces it: a schema statement stayed and
// the data ahead of it did not. A retry could not run the body from the first
// statement or from the second, so both are refused, and a repair after a person
// finished the work is what ends it.
func TestRollbackThatLeavesNoCountIsEndedByHandE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newRollbackTarget(c, dbtarget.YugabyteDB)
	dir := migrationsDir(c, "SELECT 1;\n", insertSeed+createCreated+insertAbsent)
	target.runUp(c, dir)
	c.Assert(target.rowsIn(c, "created"), qt.Equals, 0)
	_, err := target.conn.ExecContext(c.Context(), "CREATE TABLE absent (id INT PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	_, retried := target.runUp(c, dir, "--allow-dirty")
	resumed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "repair", "--db-url", target.url, "--migrations-dir", dir, "--version", "1", "--resume-from", "2")
	repaired := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "repair", "--db-url", target.url, "--migrations-dir", dir, "--version", "1")

	c.Assert(retried.Outcome, qt.Equals, migrator.RunOutcomeUnknown)
	c.Assert(retried.Error.Message, qt.Matches, `(?s).*left some statements in place and undid others; inspect the database.*`)
	c.Assert(resumed.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", resumed.Stdout, resumed.Stderr))
	c.Assert(resumed.Stderr, qt.Contains, "omit --resume-from")
	c.Assert(target.rowsIn(c, "absent"), qt.Equals, 0)
	c.Assert(repaired.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", repaired.Stdout, repaired.Stderr))
	c.Assert(target.dirtyRow(c, dir), qt.IsNil)
}

// TestFailedRollbackReportsWhatStayedE2E_FailurePath is the same question asked of a
// down migration. It shares the transaction and the answer, and what stayed is a
// table that is gone.
func TestFailedRollbackReportsWhatStayedE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		wantApplied int
		wantCreated int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, wantCreated: 0},
		{name: "cockroachdb", engine: dbtarget.CockroachDB, wantApplied: 1, wantCreated: -1},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, wantApplied: 1, wantCreated: -1},
		{name: "mysql", engine: dbtarget.MySQL, wantApplied: 1, wantCreated: -1},
		{name: "mariadb", engine: dbtarget.MariaDB, wantApplied: 1, wantCreated: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, test.engine)
			dir := migrationsDir(c, "DROP TABLE created;\n"+insertAbsent, createCreated)
			up, _ := target.runUp(c, dir)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))

			down := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"migrations", "down", "--db-url", target.url, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			dirty := target.dirtyRow(c, dir)
			c.Assert(dirty, qt.IsNotNil)
			c.Assert(dirty.Direction, qt.Equals, migrator.MigrationDirectionDown)
			c.Assert(dirty.Applied, qt.Equals, test.wantApplied)
			c.Assert(dirty.Total, qt.Equals, 2)
			c.Assert(target.rowsIn(c, "created"), qt.Equals, test.wantCreated)
		})
	}
}

// TestOneTransactionForTheRunReportsWhatStayedE2E_FailurePath runs two migrations
// under --tx-mode all. YugabyteDB takes the mode and keeps the schema statements of
// both migrations, and one row cannot count them, so the outcome is unknown. The
// row PostgreSQL leaves is the control.
func TestOneTransactionForTheRunReportsWhatStayedE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		wantOutcome migrator.RunOutcome
		wantCreated int
		wantOther   int
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, wantOutcome: migrator.RunOutcomeFailed, wantCreated: -1, wantOther: -1},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, wantOutcome: migrator.RunOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, test.engine)
			dir := migrationsDir(c, "SELECT 1;\n", createCreated, "CREATE TABLE other (id INT PRIMARY KEY);\n"+insertAbsent)

			result, document := target.runUp(c, dir, "--tx-mode", "all")

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(document.Outcome, qt.Equals, test.wantOutcome, qt.Commentf("stdout:\n%s", result.Stdout))
			c.Assert(document.Status.DirtyRevision.Version, qt.Equals, int64(2))
			c.Assert(document.Status.DirtyRevision.Applied, qt.Equals, 0)
			c.Assert(target.rowsIn(c, "created"), qt.Equals, test.wantCreated)
			c.Assert(target.rowsIn(c, "other"), qt.Equals, test.wantOther)
		})
	}
}

// TestNoTransactionKeepsItsCountE2E_FailurePath is the control on --tx-mode none,
// which keeps what it did: every statement commits on its own and the row counts
// them, on every engine.
func TestNoTransactionKeepsItsCountE2E_FailurePath(t *testing.T) {
	for _, engine := range rollbackEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, engine.engine)

			_, document := target.runUp(c, migrationsDir(c, "SELECT 1;\n", insertSeed+createCreated+insertAbsent), "--tx-mode", "none")

			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomePartial)
			c.Assert(document.Status.DirtyRevision.Applied, qt.Equals, 2)
			c.Assert(target.rowsIn(c, "created"), qt.Equals, 0)
			c.Assert(target.rowsIn(c, "seed"), qt.Equals, 1)
		})
	}
}

// TestAtlasRevisionFormatKeepsTheRowOfWhatStayedE2E_FailurePath holds the other
// revision format, where a failure the rollback took back is removed from the table.
// A failure that left statements in place keeps its row, and the retry resumes.
func TestAtlasRevisionFormatKeepsTheRowOfWhatStayedE2E_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRollbackTarget(c, test.engine)
			dir := migrationsDir(c, "SELECT 1;\n", createCreated+insertAbsent)

			_, document := target.runUp(c, dir, "--revision-format", "atlas")
			_, err := target.conn.ExecContext(c.Context(), "CREATE TABLE absent (id INT PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			result, resumed := target.runUp(c, dir, "--revision-format", "atlas", "--allow-dirty")

			c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomePartial)
			c.Assert(document.Status.DirtyRevision.Applied, qt.Equals, 1)
			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(resumed.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(target.rowsIn(c, "absent"), qt.Equals, 1)
		})
	}
}

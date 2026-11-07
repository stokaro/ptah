//go:build integration

package atlasschema_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests hold the same record on `ptah migrations up --json`, on the paths a
// migration can fail at. A commit, a failure to record the failure and a
// connection that cannot put the session back all build the error another way
// than a refused statement does.

// migrationDocument is the run document with its record kept as the bytes it was
// written in.
type migrationDocument struct {
	Outcome migrator.RunOutcome   `json:"outcome"`
	Error   *migrator.ErrorReport `json:"error"`
	Failure json.RawMessage       `json:"failure"`
}

// writeMigrations writes one migration per body, in order, into a new directory.
func writeMigrations(c *qt.C, bodies ...string) string {
	c.Helper()
	dir := c.TempDir()
	for index, body := range bodies {
		name := fmt.Sprintf("%010d_step", index+1)
		c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(body), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	}
	return dir
}

// runMigrationsUp runs `ptah migrations up --json` and decodes the document.
func runMigrationsUp(c *qt.C, address, dir string, args ...string) (clirun.Result, migrationDocument) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"migrations", "up", "--db-url", address, "--migrations-dir", dir, "--json"}, args...)...)
	var document migrationDocument
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return result, document
}

// TestMigrationsUpFailureRecordOfAStatementE2E fails a statement of a migration on
// each engine, in the per-file transaction and without one.
func TestMigrationsUpFailureRecordOfAStatementE2E(t *testing.T) {
	tests := []struct {
		name       string
		engine     dbtarget.Engine
		header     string
		wantRecord string
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"sqlstate":"23505"}`},
		{name: "postgresql without a transaction", engine: dbtarget.PostgreSQL, header: "-- +ptah no_transaction\n", wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"sqlstate":"23505"}`},
		{name: "mysql", engine: dbtarget.MySQL, wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"sqlstate":"23000","error_number":1062}`},
		{name: "mysql without a transaction", engine: dbtarget.MySQL, header: "-- +ptah no_transaction\n", wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"sqlstate":"23000","error_number":1062}`},
		{name: "mariadb", engine: dbtarget.MariaDB, wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"sqlstate":"23000","error_number":1062}`},
		{name: "sql server", engine: dbtarget.SQLServer, wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"error_number":2627}`},
		{name: "sql server without a transaction", engine: dbtarget.SQLServer, header: "-- +ptah no_transaction\n", wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"error_number":2627}`},
		{name: "oracle", engine: dbtarget.Oracle, wantRecord: `{"version":1,"version_key":"1","statement":2,"statements":2,"error_number":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newSharedFailureTarget(c, test.engine)
			accounts, planned, revisions := f.table("accounts"), f.table("planned"), f.table("revisions")
			f.dropAfterwards(c, accounts, planned, revisions, revisions+"_log")
			f.exec(c, strings.ReplaceAll(uniqueTableDDL, "{t}", accounts),
				fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, '%s')", accounts, failureRowValue))
			dir := writeMigrations(c, test.header+
				fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY);\n", planned)+
				fmt.Sprintf("INSERT INTO %s (id, email) VALUES (2, '%s');\n", accounts, failureRowValue))

			result, document := runMigrationsUp(c, f.url, dir, "--migrations-table", revisions)

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Logf("message: %s", document.Error.Message)
			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
			c.Assert(compactRecord(c, document.Failure), qt.Equals, test.wantRecord)
			c.Assert(document.Error.Message, qt.Contains, failureRowValue)
		})
	}
}

// deferredCommitBodies are two migrations whose second violates a deferred foreign
// key, which PostgreSQL answers at the commit.
func deferredCommitBodies(parents, children string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE %[1]s (id INT PRIMARY KEY);\n"+
			"CREATE TABLE %[2]s (id INT PRIMARY KEY, parent INT REFERENCES %[1]s (id) DEFERRABLE INITIALLY DEFERRED, note VARCHAR(100));\n", parents, children),
		fmt.Sprintf("INSERT INTO %[1]s (id, parent, note) VALUES (1, 999, '%[2]s');\n"+
			"INSERT INTO %[1]s (id, parent, note) VALUES (2, 999, '%[2]s');\n", children, failureRowValue),
	}
}

// TestMigrationsUpFailureRecordOfACommitE2E fails the commit of the migration, and
// of the batch under one transaction for the whole run, which has no migration of
// its own to blame and names the last.
func TestMigrationsUpFailureRecordOfACommitE2E(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		wantRecord string
	}{
		{
			name: "the per-file transaction", mode: "file",
			wantRecord: `{"version":2,"version_key":"2","statement":0,"statements":2,"commit":true,"sqlstate":"23503"}`,
		},
		{
			name: "one transaction for the run", mode: "all",
			wantRecord: `{"version":2,"version_key":"2","statement":0,"statements":2,"commit":true,"sqlstate":"23503"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, dbtarget.PostgreSQL)
			dir := writeMigrations(c, deferredCommitBodies(f.table("parents"), f.table("children"))...)

			result, document := runMigrationsUp(c, f.url, dir, "--tx-mode", test.mode)

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
			c.Assert(compactRecord(c, document.Failure), qt.Equals, test.wantRecord)
		})
	}
}

// TestMigrationsUpFailureRecordWhenTheFailureCannotBeRecordedE2E is the batch whose
// statement fails and whose revision row cannot be written afterwards. The second
// failure takes its place in nothing: the statement is still what stopped the run.
func TestMigrationsUpFailureRecordWhenTheFailureCannotBeRecordedE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	revisions := f.table("revisions")
	first := writeMigrations(c, "CREATE TABLE "+f.table("first_unit")+" (id INT PRIMARY KEY);\n")
	result, _ := runMigrationsUp(c, f.url, first, "--migrations-table", revisions)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
	f.exec(c,
		"CREATE FUNCTION "+f.table("refuse")+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'no revision rows' USING ERRCODE = '42501'; END $$",
		"CREATE TRIGGER "+f.table("refuse")+" BEFORE INSERT OR UPDATE ON "+revisions+" FOR EACH ROW EXECUTE FUNCTION "+f.table("refuse")+"()",
	)
	dir := writeMigrations(c,
		"CREATE TABLE "+f.table("first_unit")+" (id INT PRIMARY KEY);\n",
		"INSERT INTO "+f.table("absent")+" (id, note) VALUES (1, '"+failureRowValue+"');\n",
	)

	result, document := runMigrationsUp(c, f.url, dir, "--migrations-table", revisions, "--tx-mode", "all")

	c.Logf("failure: %s", compactRecord(c, document.Failure))
	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
	c.Assert(document.Error.Message, qt.Contains, "additionally failed to record")
	c.Assert(compactRecord(c, document.Failure), qt.Equals, `{"version":2,"version_key":"2","statement":1,"statements":1,"sqlstate":"42P01"}`)
}

// sentAlone matches the simple query that is exactly the statement starting with
// prefix. A revision row quotes the statement it marks, so a search of the whole
// message would end the connection at the mark before the statement instead.
func sentAlone(prefix string) func([]byte) bool {
	return func(message []byte) bool {
		return message[0] == 'Q' && bytes.HasPrefix(message[5:], []byte(prefix))
	}
}

// TestMigrationsUpFailureRecordOfALostConnectionE2E loses the connection while a
// statement of a migration runs. The server gives no answer, so the record is the
// position alone. With a lock timeout in force the run then cannot put the session
// back, and that second failure is the one the message leads with.
func TestMigrationsUpFailureRecordOfALostConnectionE2E(t *testing.T) {
	tests := []struct {
		name string
		args []string
		body string
	}{
		{
			name: "in the per-file transaction",
			body: "CREATE TABLE lost_first (id INT PRIMARY KEY);\nCREATE TABLE lost_table (id INT PRIMARY KEY);\n",
		},
		{
			name: "without a transaction",
			body: "-- +ptah no_transaction\nCREATE TABLE lost_first (id INT PRIMARY KEY);\nCREATE TABLE lost_table (id INT PRIMARY KEY);\n",
		},
		{
			name: "without a transaction, under a lock timeout it cannot take back",
			args: []string{"--lock-timeout", "5s"},
			body: "-- +ptah no_transaction\nCREATE TABLE lost_first (id INT PRIMARY KEY);\nCREATE TABLE lost_table (id INT PRIMARY KEY);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, address, breaker := connectionBreakerDatabase(c, false, sentAlone("CREATE TABLE lost_table"))
			dir := writeMigrations(c, test.body)

			result, document := runMigrationsUp(c, address, dir, test.args...)

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Logf("message: %s", document.Error.Message)
			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
			c.Assert(breaker.broken.Load(), qt.IsTrue)
			c.Assert(compactRecord(c, document.Failure), qt.Equals, `{"version":1,"version_key":"1","statement":2,"statements":2}`)
		})
	}
}

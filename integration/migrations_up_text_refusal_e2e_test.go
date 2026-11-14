//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A run asks every file it selected, before the first of them runs, each
// refusal the migrator makes from a file's text and the run's options: a file
// that controls the transaction Ptah runs it in or runs it without, a
// directive the online DDL executor rejects, and on MySQL in tx-mode file, a
// statement the transaction witness cannot follow. The shape is a first file
// that creates base and a second file the migrator refuses. The run is refused
// before it creates base or writes a revision row, where the file would
// otherwise be refused only once the first file was applied, and an invalid
// directive would leave the second revision failed with nothing applied.
//
// What each run left is read back from the server: which of base and b exist,
// and how many rows the revision table holds.

// The refusals the migrator makes of the second file.
const (
	managedTransactionControlRefusal = "migration 2 cannot run tx-mode file up statement 1 because it controls " +
		"transaction state"
	unwrappedTransactionControlRefusal = `migration 2 cannot run up statement 1 without a transaction because "BEGIN" ` +
		"controls transaction state"
	onlineDDLDirectiveRefusal = `invalid migration directives in 0000000002_two.up.sql: unknown online_ddl_tool ` +
		`directive value "bogus"`
)

// firstFileStatement is the statement of the first file. A run refused before
// the first file neither sends it nor, in a dry run, prints it.
const firstFileStatement = "CREATE TABLE base (id int)"

// textRefusalDirectory writes a directory whose first migration creates base
// and whose second migration's up file is up. name spells each file's name
// from its version, description and direction.
func textRefusalDirectory(c *qt.C, up string, name func(version int, description, direction string) string) string {
	c.Helper()
	return runnerMigrations(c, map[string]string{
		name(1, "base", "up"):   firstFileStatement + ";\n",
		name(1, "base", "down"): "DROP TABLE base;\n",
		name(2, "two", "up"):    up,
		name(2, "two", "down"):  "DROP TABLE b;\n",
	})
}

// postgresTextRefusalCensus is how many rows the revision table holds, or -1
// when there is no such table, and which of base and b exist, read from the
// PostgreSQL database at url.
func postgresTextRefusalCensus(c *qt.C, url, revisionTable string) (int, []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows := -1
	var tableExists bool
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", revisionTable).Scan(&tableExists),
		qt.IsNil)
	if tableExists {
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM "+revisionTable).Scan(&rows), qt.IsNil)
	}
	existing := make([]string, 0, 2)
	for _, relation := range []string{"base", "b"} {
		var exists bool
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists), qt.IsNil)
		existing = append(existing, fmt.Sprintf("%s=%t", relation, exists))
	}
	return rows, existing
}

// migrationsDown runs `ptah migrations down` to version 0 over dir against
// target, in an environment that carries no PTAH_ variable.
func migrationsDown(c *qt.C, dir, target string, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()},
		append([]string{"migrations", "down", "--migrations-dir", dir, "--db-url", target, "--target", "0", "--confirm"},
			flags...)...)
}

// The refusal comes before the first file, in a dry run too. Nothing is
// created, no revision row is written, and the first file's statement is
// neither sent nor printed. A dry run creates no revision table either.
func TestMigrationsUpRefusesWhatTheFileTextRefusesBeforeTheFirstFile_FailurePathE2E(t *testing.T) {
	tests := []struct {
		name     string
		up       string
		flags    []string
		wantRows int
		wantErr  string
	}{
		{
			name:     "transaction control in a file run in a transaction",
			up:       "BEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			wantRows: 0,
			wantErr:  managedTransactionControlRefusal,
		},
		{
			name:     "transaction control in a file run in a transaction, dry run",
			up:       "BEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			flags:    []string{"--dry-run"},
			wantRows: -1,
			wantErr:  managedTransactionControlRefusal,
		},
		{
			name:     "transaction control in a no_transaction file",
			up:       "-- +ptah no_transaction\nBEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			wantRows: 0,
			wantErr:  unwrappedTransactionControlRefusal,
		},
		{
			name:     "an online DDL directive the executor rejects",
			up:       "-- +ptah online_ddl_tool=bogus\nCREATE TABLE b (id int);\n",
			wantRows: 0,
			wantErr:  onlineDDLDirectiveRefusal,
		},
		{
			name:     "an online DDL directive the executor rejects, dry run",
			up:       "-- +ptah online_ddl_tool=bogus\nCREATE TABLE b (id int);\n",
			flags:    []string{"--dry-run"},
			wantRows: -1,
			wantErr:  onlineDDLDirectiveRefusal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := textRefusalDirectory(c, test.up, ptahFileName)
			target, _ := scratchReplayDatabase(c)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := postgresTextRefusalCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "b=false"})
			c.Assert(got.Stderr, qt.Contains, "error: error running migrations: "+test.wantErr)
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), firstFileStatement)
		})
	}
}

// The controls: a directive the executor accepts, and a file the golang-migrate
// runner runs with its own transaction, which no transaction of Ptah's wraps.
// Both files apply.
func TestMigrationsUpRefusesWhatTheFileTextRefusesBeforeTheFirstFile_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name  string
		up    string
		flags []string
	}{
		{
			name: "an online DDL directive the executor accepts",
			up:   "-- +ptah online_ddl_tool=none\nCREATE TABLE b (id int);\n",
		},
		{
			name:  "a file with its own transaction under the golang-migrate runner",
			up:    "BEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			flags: []string{"--runner", "golang-migrate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := textRefusalDirectory(c, test.up, ptahFileName)
			target, _ := scratchReplayDatabase(c)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := postgresTextRefusalCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, 2)
			c.Assert(relations, qt.DeepEquals, []string{"base=true", "b=true"})
		})
	}
}

// A rollback asks every down file the same way before it rolls back the
// first. The second migration's down file, which runs first, is not rolled
// back, and both revisions still record their migrations applied.
func TestMigrationsDownRefusesWhatTheFileTextRefusesBeforeTheFirstFile_FailurePathE2E(t *testing.T) {
	tests := []struct {
		name    string
		down    string
		wantErr string
	}{
		{
			name: "transaction control in a file run in a transaction",
			down: "BEGIN;\nDROP TABLE base;\nCOMMIT;\n",
			wantErr: "migration 1 cannot run tx-mode file down statement 1 because it controls " +
				"transaction state",
		},
		{
			name: "an online DDL directive the executor rejects",
			down: "-- +ptah online_ddl_tool=bogus\nDROP TABLE base;\n",
			wantErr: `invalid migration directives in 0000000001_base.down.sql: unknown online_ddl_tool ` +
				`directive value "bogus"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := runnerMigrations(c, map[string]string{
				ptahFileName(1, "base", "up"):   "CREATE TABLE base (id int);\n",
				ptahFileName(1, "base", "down"): test.down,
				ptahFileName(2, "two", "up"):    "CREATE TABLE b (id int);\n",
				ptahFileName(2, "two", "down"):  "DROP TABLE b;\n",
			})
			target, _ := scratchReplayDatabase(c)
			applied := migrationsUp(c, dir, target)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))

			got := migrationsDown(c, dir, target)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := postgresTextRefusalCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, 2)
			c.Assert(relations, qt.DeepEquals, []string{"base=true", "b=true"})
			c.Assert(got.Stderr, qt.Contains, "error: error running down migrations: "+test.wantErr)
		})
	}
}

// The controls of the rollback: a directive the executor accepts, and a down
// file the golang-migrate runner runs with its own transaction. Both
// migrations roll back.
func TestMigrationsDownRefusesWhatTheFileTextRefusesBeforeTheFirstFile_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name  string
		down  string
		flags []string
	}{
		{
			name: "an online DDL directive the executor accepts",
			down: "-- +ptah online_ddl_tool=none\nDROP TABLE base;\n",
		},
		{
			name:  "a file with its own transaction under the golang-migrate runner",
			down:  "BEGIN;\nDROP TABLE base;\nCOMMIT;\n",
			flags: []string{"--runner", "golang-migrate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := runnerMigrations(c, map[string]string{
				ptahFileName(1, "base", "up"):   "CREATE TABLE base (id int);\n",
				ptahFileName(1, "base", "down"): test.down,
				ptahFileName(2, "two", "up"):    "CREATE TABLE b (id int);\n",
				ptahFileName(2, "two", "down"):  "DROP TABLE b;\n",
			})
			target, _ := scratchReplayDatabase(c)
			applied := migrationsUp(c, dir, target, test.flags...)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))

			got := migrationsDown(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := postgresTextRefusalCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, 0)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "b=false"})
		})
	}
}

// `ptah-compat migrate apply` runs the same migrator and refuses a file that
// controls its transaction before the first file, in a dry run too. The
// community binary applies the first file and then fails the second, and its
// dry run reports both files ok; the retained divergence for it cites this
// test.
func TestAtlasMigrateApplyRefusesTransactionControlBeforeTheFirstFileE2E(t *testing.T) {
	tests := []struct {
		name     string
		up       string
		flags    []string
		wantRows int
		wantErr  string
	}{
		{
			name:     "a file run in a transaction",
			up:       "BEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			wantRows: 0,
			wantErr:  managedTransactionControlRefusal,
		},
		{
			name:     "a file run in a transaction, dry run",
			up:       "BEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			flags:    []string{"--dry-run"},
			wantRows: -1,
			wantErr:  managedTransactionControlRefusal,
		},
		{
			name:     "a txmode none file",
			up:       "-- atlas:txmode none\n\nBEGIN;\nCREATE TABLE b (id int);\nCOMMIT;\n",
			wantRows: 0,
			wantErr:  unwrappedTransactionControlRefusal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := textRefusalDirectory(c, test.up, atlasFileName)
			target, _ := scratchReplayDatabase(c)
			options := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()}
			hashed := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://"+dir)
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))

			got := clirun.Run(c, clirun.Compat, options,
				append([]string{"migrate", "apply", "--url", target, "--dir", "file://" + dir}, test.flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := postgresTextRefusalCensus(c, target, "atlas_schema_revisions.atlas_schema_revisions")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "b=false"})
			c.Assert(got.Stderr, qt.Contains, "Error: error applying migrations: "+test.wantErr)
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), firstFileStatement)
		})
	}
}

// mysqlTextRefusalCensus is how many rows revisionTable holds, or -1 when
// there is no such table, and which of base and b exist, read from database on
// the MySQL server s administers.
func mysqlTextRefusalCensus(c *qt.C, s mysqlScratch, database, revisionTable string) (int, []string) {
	c.Helper()
	rows := -1
	var tables int
	c.Assert(s.admin.QueryRowContext(c.Context(),
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		database, revisionTable,
	).Scan(&tables), qt.IsNil)
	if tables > 0 {
		c.Assert(s.admin.QueryRowContext(c.Context(),
			"SELECT count(*) FROM `"+database+"`.`"+revisionTable+"`",
		).Scan(&rows), qt.IsNil)
	}
	existing := make([]string, 0, 2)
	for _, relation := range []string{"base", "b"} {
		var count int
		c.Assert(s.admin.QueryRowContext(c.Context(),
			"SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			database, relation,
		).Scan(&count), qt.IsNil)
		existing = append(existing, fmt.Sprintf("%s=%t", relation, count > 0))
	}
	return rows, existing
}

// witnessRefusal is what the transaction witness says of the second file.
const witnessRefusal = "migration 2 statement 1 selects non-transactional storage engine MyISAM; " +
	"tx-mode file requires InnoDB on MySQL-family databases"

// On MySQL a file in tx-mode file runs under Ptah's transaction witness, which
// refuses a table on a non-transactional storage engine. The refusal reads the
// file alone, so it comes before the first file too, in a dry run as well.
func TestMigrationsUpRefusesWhatTheWitnessRefusesBeforeTheFirstFile_FailurePathE2E(t *testing.T) {
	tests := []struct {
		name     string
		flags    []string
		wantRows int
	}{
		{name: "tx-mode file", wantRows: 0},
		{name: "dry run", flags: []string{"--dry-run"}, wantRows: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			database, target := scratch.database(c, "witness")
			dir := textRefusalDirectory(c, "CREATE TABLE b (id int) ENGINE=MyISAM;\n", ptahFileName)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := mysqlTextRefusalCensus(c, scratch, database, "schema_migrations")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "b=false"})
			c.Assert(got.Stderr, qt.Contains, "error: error running migrations: "+witnessRefusal)
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), firstFileStatement)
		})
	}
}

// The controls run the same table without a transaction, where the witness
// does not apply, and both files apply.
func TestMigrationsUpRefusesWhatTheWitnessRefusesBeforeTheFirstFile_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name  string
		up    string
		flags []string
	}{
		{name: "tx-mode none", up: "CREATE TABLE b (id int) ENGINE=MyISAM;\n", flags: []string{"--tx-mode", "none"}},
		{name: "a no_transaction file", up: "-- +ptah no_transaction\nCREATE TABLE b (id int) ENGINE=MyISAM;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			database, target := scratch.database(c, "witness")
			dir := textRefusalDirectory(c, test.up, ptahFileName)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := mysqlTextRefusalCensus(c, scratch, database, "schema_migrations")
			c.Assert(rows, qt.Equals, 2)
			c.Assert(relations, qt.DeepEquals, []string{"base=true", "b=true"})
		})
	}
}

// `ptah-compat migrate apply` runs the same migrator and refuses the same file
// before the first, in a dry run too. The community binary applies both files,
// and its dry run reports both ok; the retained divergence for it cites this
// test.
func TestAtlasMigrateApplyRefusesWhatTheWitnessRefusesBeforeTheFirstFileE2E(t *testing.T) {
	tests := []struct {
		name     string
		flags    []string
		wantRows int
	}{
		{name: "apply", wantRows: 0},
		{name: "dry run", flags: []string{"--dry-run"}, wantRows: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			database, target := scratch.database(c, "witness")
			dir := textRefusalDirectory(c, "CREATE TABLE b (id int) ENGINE=MyISAM;\n", atlasFileName)
			options := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()}
			hashed := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://"+dir)
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))

			got := clirun.Run(c, clirun.Compat, options,
				append([]string{"migrate", "apply", "--url", target, "--dir", "file://" + dir}, test.flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := mysqlTextRefusalCensus(c, scratch, database, "atlas_schema_revisions")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "b=false"})
			c.Assert(got.Stderr, qt.Contains, "Error: error applying migrations: "+witnessRefusal)
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), firstFileStatement)
		})
	}
}

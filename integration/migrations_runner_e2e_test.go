//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// `ptah migrations up --runner golang-migrate`, or `migration.runner` in
// ptah.yaml, applies a directory the way golang-migrate does, which opens no
// transaction around a file. A file with its own BEGIN and COMMIT runs outside
// any transaction of the migrator's, so when a later statement fails, the
// revision row records the statements the file's COMMIT made permanent and a
// retry resumes after them.
//
// Under the default runner the migrator refuses the file before that file
// runs, because its COMMIT would end the transaction Ptah owns.
//
// `ptah migrations down` takes the same runner, resolved the same way, and
// rolls a down file back the way the up file is applied. A failed rollback
// records the statements the file's COMMIT made permanent and leaves the
// revision dirty, as every failed rollback does.

// runnerMigrations writes a migration directory whose second version opens
// and commits a transaction of its own and then fails, and returns it.
func runnerMigrations(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	return dir
}

// ownTransactionFiles is a directory whose second version commits a table in
// its own transaction before a statement that fails.
var ownTransactionFiles = map[string]string{
	"0000000001_init.up.sql":       "CREATE TABLE base (id int);\n",
	"0000000001_init.down.sql":     "DROP TABLE base;\n",
	"0000000002_backfill.up.sql":   "BEGIN;\nCREATE TABLE t2 (id int);\nCOMMIT;\nINSERT INTO missing_table VALUES (1);\n",
	"0000000002_backfill.down.sql": "DROP TABLE t2;\n",
}

// revisionApplied is how many statements the revision row of version records
// as applied, and whether the table name exists, read from the database at
// url.
func revisionApplied(c *qt.C, url string, version int64, table string) (int, bool) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var applied int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT applied FROM schema_migrations WHERE version = $1", version).Scan(&applied), qt.IsNil)
	var exists bool
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists), qt.IsNil)
	return applied, exists
}

// builtPtah builds the native binary once per test and returns its path.
func builtPtah(t *testing.T, c *qt.C) string {
	t.Helper()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, c.Context(), e2eRepoRoot(t), binary)
	return binary
}

func runOwnTransactionFiles(c *qt.C, binary, config string, flags ...string) (target, output string, err error) {
	c.Helper()
	dir := runnerMigrations(c, ownTransactionFiles)
	workdir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workdir, "ptah.yaml"), []byte(config), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	target, _ = scratchReplayDatabase(c)
	output, err = runPtahInDir(c.Context(), workdir, binary,
		append([]string{"migrations", "up", "--migrations-dir", dir, "--db-url", target}, flags...)...)
	return target, output, err
}

func TestMigrationsUpRunnerRefusesWrappedTransactionControlE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "the default runner"},
		{name: "the flag over ptah.yaml", config: "migration:\n  runner: golang-migrate\n", flags: []string{"--runner", "ptah"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := runOwnTransactionFiles(c, binary, test.config, test.flags...)
			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "controls transaction state")
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			var created bool
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass('base') IS NOT NULL").Scan(&created), qt.IsNil)
			c.Assert(created, qt.IsTrue)
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass('t2') IS NOT NULL").Scan(&created), qt.IsNil)
			c.Assert(created, qt.IsFalse)
		})
	}
}

func TestMigrationsUpRunnerAppliesTheFileOwnTransactionsE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "golang-migrate named by the flag", flags: []string{"--runner", "golang-migrate"}},
		{name: "golang-migrate named in ptah.yaml", config: "migration:\n  runner: golang-migrate\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := runOwnTransactionFiles(c, binary, test.config, test.flags...)
			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `relation "missing_table" does not exist`)
			applied, created := revisionApplied(c, target, 2, "t2")
			c.Assert(applied, qt.Equals, 3)
			c.Assert(created, qt.IsTrue)
		})
	}
}

// The apply gate judges a file's transaction control by the runner too. With
// TX201 raised to an error and gated, the default runner refuses a file that
// opens its own transaction before anything runs, and golang-migrate, which
// runs that transaction as written, applies it.
func TestMigrationsUpRunnerDecidesTheGateE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	tests := []struct {
		name        string
		flags       []string
		wantCreated bool
	}{
		{name: "the default runner", wantCreated: false},
		{name: "golang-migrate", flags: []string{"--runner", "golang-migrate"}, wantCreated: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := runnerMigrations(c, map[string]string{
				"0000000001_own.up.sql":   "BEGIN;\nCREATE TABLE t1 (id int);\nCOMMIT;\n",
				"0000000001_own.down.sql": "DROP TABLE t1;\n",
				".ptah-lint.yaml":         "gate:\n  families: [TX]\nrules:\n  TX201:\n    severity: error\n",
			})
			target, _ := scratchReplayDatabase(c)

			out, _ := runPtahInDir(c.Context(), c.TempDir(), binary,
				append([]string{"migrations", "up", "--migrations-dir", dir, "--db-url", target}, test.flags...)...)

			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			var created bool
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass('t1') IS NOT NULL").Scan(&created), qt.IsNil)
			c.Assert(created, qt.Equals, test.wantCreated, qt.Commentf("%s", out))
		})
	}
}

// `ptah migrations lint --dev-url` replays the directory the way the runner
// applies it. A file whose COMMIT ends the transaction its first statement
// runs in, as golang-migrate's single query does, replays under golang-migrate;
// a migrator that wraps the file refuses the COMMIT, since no transaction of
// the file's is open to close.
func TestMigrationsLintRunnerDecidesTheReplayE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	tests := []struct {
		name     string
		flags    []string
		wantCode string
	}{
		{name: "the default runner", wantCode: "exit status 2"},
		{name: "golang-migrate", flags: []string{"--runner", "golang-migrate"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := runnerMigrations(c, map[string]string{
				"0000000001_commit.up.sql":   "CREATE TABLE base (id int);\nCOMMIT;\nCREATE TABLE after_commit (id int);\n",
				"0000000001_commit.down.sql": "DROP TABLE after_commit;\nDROP TABLE base;\n",
			})
			dev, _ := scratchReplayDatabase(c)

			out, err := runPtahInDir(c.Context(), c.TempDir(), binary,
				append([]string{"migrations", "lint", "--dir", dir, "--dev-url", dev}, test.flags...)...)

			c.Assert(exitStatus(err), qt.Equals, test.wantCode, qt.Commentf("%s", out))
		})
	}
}

// ownTransactionDownFiles is a directory whose second version rolls back in a
// transaction of its own, followed by a statement that fails when down is the
// failing variant.
func ownTransactionDownFiles(down string) map[string]string {
	return map[string]string{
		"0000000001_init.up.sql":       "CREATE TABLE base (id int);\n",
		"0000000001_init.down.sql":     "DROP TABLE base;\n",
		"0000000002_backfill.up.sql":   "CREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n",
		"0000000002_backfill.down.sql": down,
	}
}

// downOwnTransactionsBody drops both tables of the second version, one in a
// transaction the file opens and commits itself.
const downOwnTransactionsBody = "BEGIN;\nDROP TABLE t2;\nCOMMIT;\nDROP TABLE t3;\n"

// rollBackOwnTransactionFiles applies the directory down describes and rolls
// its second version back with the down command's flags, from a working
// directory holding config as ptah.yaml.
func rollBackOwnTransactionFiles(c *qt.C, binary, config, down string, flags ...string) (target, output string, err error) {
	c.Helper()
	dir := runnerMigrations(c, ownTransactionDownFiles(down))
	workdir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workdir, "ptah.yaml"), []byte(config), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	target, _ = scratchReplayDatabase(c)
	out, err := runPtahInDir(c.Context(), c.TempDir(), binary, "migrations", "up", "--migrations-dir", dir, "--db-url", target)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	output, err = runPtahInDir(c.Context(), workdir, binary,
		append([]string{"migrations", "down", "--migrations-dir", dir, "--db-url", target, "--target", "1", "--confirm"},
			flags...)...)
	return target, output, err
}

// revisionCensus is the revision rows of version, read from the database at
// url, and which of tables exist.
func revisionCensus(c *qt.C, url string, version int64, tables ...string) (int, []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM schema_migrations WHERE version = $1", version).Scan(&rows),
		qt.IsNil)
	existing := make([]string, 0, len(tables))
	for _, table := range tables {
		var exists bool
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists), qt.IsNil)
		existing = append(existing, fmt.Sprintf("%s=%t", table, exists))
	}
	return rows, existing
}

func TestMigrationsDownRunnerRollsBackTheFileOwnTransactionsE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "golang-migrate named by the flag", flags: []string{"--runner", "golang-migrate"}},
		{name: "golang-migrate named in ptah.yaml", config: "migration:\n  runner: golang-migrate\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := rollBackOwnTransactionFiles(c, binary, test.config, downOwnTransactionsBody, test.flags...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 0)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
		})
	}
}

// --shadow-db replays the rollback the way the target runs it, so a down file
// that opens its own transactions is verified on the shadow database and then
// rolled back on the target.
func TestMigrationsDownRunnerVerifiesOnTheShadowDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	shadow, _ := scratchReplayDatabase(c)

	target, out, err := rollBackOwnTransactionFiles(c, binary, "", downOwnTransactionsBody,
		"--runner", "golang-migrate", "--shadow-db", shadow)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
	c.Assert(rows, qt.Equals, 0)
	c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
}

// Under the default runner, and with --runner ptah over a ptah.yaml that names
// golang-migrate, the migrator wraps the down file and refuses its
// transaction control before the file runs: both tables stay, and so does the
// revision.
func TestMigrationsDownRunnerRefusesWrappedTransactionControlE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "the default runner"},
		{name: "the flag over ptah.yaml", config: "migration:\n  runner: golang-migrate\n", flags: []string{"--runner", "ptah"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := rollBackOwnTransactionFiles(c, binary, test.config, downOwnTransactionsBody, test.flags...)
			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "controls transaction state")
			rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 1)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=true", "t3=true"})
		})
	}
}

// A down file that fails after its own transaction committed leaves that
// transaction's work in place, and the revision row records it: three
// statements applied, on a row the next rollback refuses as dirty.
func TestMigrationsDownRunnerRecordsTheCommittedTransactionE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	target, out, err := rollBackOwnTransactionFiles(c, binary, "",
		"BEGIN;\nDROP TABLE t2;\nCOMMIT;\nINSERT INTO missing_table VALUES (1);\nDROP TABLE t3;\n", "--runner", "golang-migrate")

	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `relation "missing_table" does not exist`)
	applied, dropped := revisionApplied(c, target, 2, "t2")
	c.Assert(applied, qt.Equals, 3)
	c.Assert(dropped, qt.IsFalse)
	_, tables := revisionCensus(c, target, 2, "t3")
	c.Assert(tables, qt.DeepEquals, []string{"t3=true"})
}

// exitStatus is the process error's text, empty for a run that exited 0.
func exitStatus(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

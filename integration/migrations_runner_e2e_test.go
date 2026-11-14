//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"os/exec"
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
			c.Assert(created, qt.IsFalse)
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

// ownTransactionUpFiles is a directory whose up files open and commit their
// own transactions, so the default runner refuses to replay either of them.
var ownTransactionUpFiles = map[string]string{
	"0000000001_init.up.sql":       "BEGIN;\nCREATE TABLE base (id int);\nCOMMIT;\n",
	"0000000001_init.down.sql":     "DROP TABLE base;\n",
	"0000000002_backfill.up.sql":   "BEGIN;\nCREATE TABLE t2 (id int);\nCOMMIT;\nCREATE TABLE t3 (id int);\n",
	"0000000002_backfill.down.sql": "DROP TABLE t3;\nDROP TABLE t2;\n",
}

// rollback is a `migrations down --target 1` run over a directory that
// `migrations up` applies first.
type rollback struct {
	files map[string]string
	// upFlags are added to the `migrations up` run that applies files.
	upFlags []string
	// config is ptah.yaml in the directory the rollback runs from.
	config string
	// env is added to an environment that carries no PTAH_ variable, so the
	// runner the rollback reads is the one the test names.
	env   []string
	flags []string
}

// rollBack applies r.files to a scratch database, then rolls its second
// version back, and returns the database and what the rollback printed.
func rollBack(c *qt.C, binary string, r rollback) (target, output string, err error) {
	c.Helper()
	dir := runnerMigrations(c, r.files)
	target, _ = scratchReplayDatabase(c)
	out, err := runPtahInDir(c.Context(), c.TempDir(), binary,
		append([]string{"migrations", "up", "--migrations-dir", dir, "--db-url", target}, r.upFlags...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	output, err = rollBackApplied(c, binary, dir, target, r)
	return target, output, err
}

// rollBackApplied rolls the second version of dir back on target, where the
// test applied it.
func rollBackApplied(c *qt.C, binary, dir, target string, r rollback) (string, error) {
	c.Helper()
	return runConfigured(c, binary, r,
		"migrations", "down", "--migrations-dir", dir, "--db-url", target, "--target", "1", "--confirm")
}

// runConfigured runs the binary with args from a directory that holds r's
// ptah.yaml, in an environment that carries no PTAH_ variable but r's.
func runConfigured(c *qt.C, binary string, r rollback, args ...string) (string, error) {
	c.Helper()
	workdir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workdir, "ptah.yaml"), []byte(r.config), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	cmd := exec.CommandContext(c.Context(), binary, append(args, r.flags...)...)                   // #nosec G204 -- the binary is built by the test and the arguments are its own
	cmd.Dir = workdir
	cmd.Env = append(environmentWithoutPtahVariables(), r.env...)
	output, err := cmd.CombinedOutput()
	return string(output), err
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

// revisionRows is every revision row, read whole from the database at url.
func revisionRows(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT row_to_json(r)::text FROM schema_migrations r ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		got = append(got, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}

// The runner comes from --runner, then PTAH_RUNNER, then `migration.runner` in
// ptah.yaml. Each row names golang-migrate at one of them and the ptah runner
// at the next one down, so a row passes only when the higher one wins.
func TestMigrationsDownRunnerRollsBackTheFileOwnTransactionsE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "golang-migrate named by the flag", flags: []string{"--runner", "golang-migrate"}},
		{name: "golang-migrate named in ptah.yaml", config: "migration:\n  runner: golang-migrate\n"},
		{name: "golang-migrate named by PTAH_RUNNER", env: []string{"PTAH_RUNNER=golang-migrate"}},
		{
			name:  "the flag over PTAH_RUNNER",
			env:   []string{"PTAH_RUNNER=ptah"},
			flags: []string{"--runner", "golang-migrate"},
		},
		{
			name:   "PTAH_RUNNER over ptah.yaml",
			config: "migration:\n  runner: ptah\n",
			env:    []string{"PTAH_RUNNER=golang-migrate"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := rollBack(c, binary, rollback{
				files:  ownTransactionDownFiles(downOwnTransactionsBody),
				config: test.config,
				env:    test.env,
				flags:  test.flags,
			})
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

	target, out, err := rollBack(c, binary, rollback{
		files: ownTransactionDownFiles(downOwnTransactionsBody),
		flags: []string{"--runner", "golang-migrate", "--shadow-db", shadow},
	})

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
	c.Assert(rows, qt.Equals, 0)
	c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
}

// --plan builds the target version on the shadow database by running the up
// files, and runs them the way the runner does. Up files that open their own
// transactions replay there under golang-migrate, and the derived rollback
// drops what the second version created.
func TestMigrationsDownRunnerPlansOnTheShadowDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	shadow, _ := scratchReplayDatabase(c)

	target, out, err := rollBack(c, binary, rollback{
		files:   ownTransactionUpFiles,
		upFlags: []string{"--runner", "golang-migrate"},
		flags:   []string{"--runner", "golang-migrate", "--plan", "--shadow-db", shadow},
	})

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Derived rollback plan")
	rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
	c.Assert(rows, qt.Equals, 0)
	c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
}

// A dry run of a down file that opens its own transactions prints the
// rollback and runs none of it: the revision rows read back the same, and
// both tables stay.
func TestMigrationsDownRunnerDryRunLeavesTheDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	r := rollback{
		files: ownTransactionDownFiles(downOwnTransactionsBody),
		flags: []string{"--runner", "golang-migrate", "--dry-run"},
	}
	dir := runnerMigrations(c, r.files)
	target, _ := scratchReplayDatabase(c)
	out, err := runPtahInDir(c.Context(), c.TempDir(), binary, "migrations", "up", "--migrations-dir", dir, "--db-url", target)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	before := revisionRows(c, target)

	out, err = rollBackApplied(c, binary, dir, target, r)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Would have rolled back these migrations: [2]")
	c.Assert(revisionRows(c, target), qt.DeepEquals, before)
	c.Assert(before, qt.HasLen, 2)
	_, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
	c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=true", "t3=true"})
}

// Under the default runner, and wherever a higher source names the ptah runner
// over a lower one that names golang-migrate, the migrator wraps the down file
// and refuses its transaction control before the file runs: both tables stay,
// and so does the revision.
func TestMigrationsDownRunnerRefusesWrappedTransactionControlE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "the default runner"},
		{name: "the flag over ptah.yaml", config: "migration:\n  runner: golang-migrate\n", flags: []string{"--runner", "ptah"}},
		{name: "the flag over PTAH_RUNNER", env: []string{"PTAH_RUNNER=golang-migrate"}, flags: []string{"--runner", "ptah"}},
		{
			name:   "PTAH_RUNNER over ptah.yaml",
			config: "migration:\n  runner: golang-migrate\n",
			env:    []string{"PTAH_RUNNER=ptah"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, out, err := rollBack(c, binary, rollback{
				files:  ownTransactionDownFiles(downOwnTransactionsBody),
				config: test.config,
				env:    test.env,
				flags:  test.flags,
			})
			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "controls transaction state")
			rows, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 1)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=true", "t3=true"})
		})
	}
}

// A runner Ptah does not model is refused before the directory is read and
// the database is opened, whichever source names it: neither exists here, and
// the refusal names the runner.
func TestMigrationsDownRunnerRefusesAnUnknownRunnerE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "named by the flag", flags: []string{"--runner", "flyway"}},
		{name: "named by PTAH_RUNNER", env: []string{"PTAH_RUNNER=flyway"}},
		{name: "named in ptah.yaml", config: "migration:\n  runner: flyway\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := rollBackApplied(c, binary, filepath.Join(c.TempDir(), "missing"), unreachableDatabaseURL, rollback{
				config: test.config,
				env:    test.env,
				flags:  test.flags,
			})
			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Equals, "error: invalid runner \"flyway\": expected ptah or golang-migrate\n")
		})
	}
}

// A down file that fails after its own transaction committed leaves that
// transaction's work in place, and the revision row records it: three
// statements applied, on a row the next rollback refuses as dirty.
func TestMigrationsDownRunnerRecordsTheCommittedTransactionE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	target, out, err := rollBack(c, binary, rollback{
		files: ownTransactionDownFiles("BEGIN;\nDROP TABLE t2;\nCOMMIT;\nINSERT INTO missing_table VALUES (1);\nDROP TABLE t3;\n"),
		flags: []string{"--runner", "golang-migrate"},
	})

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

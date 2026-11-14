//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A migration that controls transaction state is refused by the migrator
// whether the migrator wraps the file or runs it without a transaction, and
// lint reports it before a deploy does. These tests run both binaries on the
// directory of the report: a SQLite directory whose second file carries
// `-- atlas:txmode none` and holds its own BEGIN and COMMIT. Lint and apply
// have to agree on it, so the apply refusal is asserted beside every finding.

const (
	transactionControlBase = "CREATE TABLE users (id int);\n"
	transactionControlNone = "-- atlas:txmode none\n\nBEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"
	transactionControlWrap = "BEGIN;\nINSERT INTO users VALUES (1);\nCOMMIT;\n"
	transactionControlPlan = "INSERT INTO users VALUES (1);\n"

	transactionControlNoneMessage = "the migrator refuses transaction control in a migration that runs without a " +
		"transaction and commits each statement on its own, so the apply stops at this BEGIN; " +
		"remove the transaction control"
	transactionControlWrapMessage = "the migrator runs this file in one transaction and refuses a statement that " +
		"controls transaction state, so the apply stops at this BEGIN; remove the transaction control"
)

// writeTransactionControlProject writes 1.sql and 2.sql under <root>/migrations
// and hashes the directory with the shipped `migrate hash`.
func writeTransactionControlProject(c *qt.C, second string) string {
	c.Helper()
	root := c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	writeLintE2EFile(c, migrations, "1.sql", transactionControlBase)
	writeLintE2EFile(c, migrations, "2.sql", second)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	return root
}

func compatLintTransactionControl(c *qt.C, root string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		append([]string{
			"migrate", "lint", "--dir", "file://migrations",
			"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"), "--latest", "1",
		}, args...)...)
}

func compatApplyTransactionControl(c *qt.C, root string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "apply", "--dir", "file://migrations", "--url", "sqlite://"+filepath.Join(c.TempDir(), "t.db"))
}

func nativeLintTransactionControl(c *qt.C, root string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		append([]string{
			"migrations", "lint", "--dir", "migrations", "--dir-format", "atlas", "--dialect", "sqlite", "--latest", "1",
		}, args...)...)
}

func TestMigrateLintTransactionControlE2E_CompatReportsAndApplyRefuses(t *testing.T) {
	c := qt.New(t)
	root := writeTransactionControlProject(c, transactionControlNone)

	lint := compatLintTransactionControl(c, root)
	apply := compatApplyTransactionControl(c, root)

	c.Assert(lint.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", lint.Stdout, lint.Stderr))
	c.Assert(lint.Stderr, qt.Equals, "")
	c.Assert(redactLintE2EDurations(lint.Stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- diagnostics detected:\n"+
			"      -- L3 [TX201]: the migrator refuses transaction control in a migration that runs without a\n"+
			"         transaction and commits each statement on its own, so the apply stops at this BEGIN;\n"+
			"         remove the transaction control\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version with warnings\n"+
			"  -- 1 diagnostic\n")
	c.Assert(apply.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
	c.Assert(apply.Stderr, qt.Contains,
		`migration 2 cannot run up statement 1 without a transaction because "BEGIN" controls transaction state`)
}

// A project that raises nestedtx to an error fails the run, which is how a
// pipeline stops before the deploy.
func TestMigrateLintTransactionControlE2E_CompatErrorSeverityFailsTheRun(t *testing.T) {
	c := qt.New(t)
	root := writeTransactionControlProject(c, transactionControlNone)
	writeLintE2EFile(c, root, "atlas.hcl", "lint {\n  nestedtx {\n    error = true\n  }\n}\n\n"+
		"env \"ci\" {\n  migration {\n    dir = \"file://migrations\"\n  }\n}\n")

	lint := compatLintTransactionControl(c, root, "--env", "ci")

	c.Assert(lint.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", lint.Stdout, lint.Stderr))
	c.Assert(lint.Stdout, qt.Contains, "[TX201]")
	c.Assert(lint.Stdout, qt.Contains, "1 version with errors")
}

// Wrapped, the migrator refuses the file under a different sentence, and lint
// says which. The dev database replay would stop at the BEGIN first, so the
// text is read without one.
func TestMigrateLintTransactionControlE2E_NativeReportsBothModes(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		line    string
		message string
		apply   string
	}{
		{"without a transaction", transactionControlNone, "migrations/2.sql:3", transactionControlNoneMessage,
			`migration 2 cannot run up statement 1 without a transaction because "BEGIN" controls transaction state`},
		{"wrapped in the migrator's transaction", transactionControlWrap, "migrations/2.sql:1", transactionControlWrapMessage,
			"migration 2 cannot run tx-mode file up statement 1 because it controls transaction state"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeTransactionControlProject(c, test.sql)

			lint := nativeLintTransactionControl(c, root, "--fail-on", "any")
			apply := compatApplyTransactionControl(c, root)

			c.Assert(lint.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", lint.Stdout, lint.Stderr))
			c.Assert(lint.Stdout, qt.Contains, test.line+" [warning] TX201: "+test.message)
			c.Assert(lint.Stdout, qt.Contains, "1 finding(s).")
			c.Assert(apply.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
			c.Assert(apply.Stderr, qt.Contains, test.apply)
		})
	}
}

// The control: the same directory without transaction control is clean under
// both binaries, and the apply runs it.
func TestMigrateLintTransactionControlE2E_AFileWithoutControlIsClean(t *testing.T) {
	c := qt.New(t)
	root := writeTransactionControlProject(c, "-- atlas:txmode none\n\n"+transactionControlPlan)

	compat := compatLintTransactionControl(c, root)
	native := nativeLintTransactionControl(c, root, "--fail-on", "any")
	apply := compatApplyTransactionControl(c, root)

	c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
	c.Assert(compat.Stdout, qt.Not(qt.Contains), "TX201")
	c.Assert(native.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", native.Stdout, native.Stderr))
	c.Assert(native.Stdout, qt.Not(qt.Contains), "TX201")
	c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
}

// A directory written for golang-migrate on an engine whose migrator reads no
// body's own transactions keeps the transaction around each file, in the up
// and the down half alike, so the up of a file holding a block is refused, and
// so is the rollback of a down file holding one, under the same runner. Lint
// reports both before an apply or a rollback does. Version 1 is applied on its
// own first, because the full run refuses before any file runs and the
// rollback needs version 1 in place.
func TestMigrateLintTransactionControlE2E_NativeGolangMigrateDirectory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	writeLintE2EFile(c, migrations, "1_a.up.sql", transactionControlBase)
	writeLintE2EFile(c, migrations, "1_a.down.sql", "BEGIN;\nDROP TABLE users;\nCOMMIT;\n")
	writeLintE2EFile(c, migrations, "2_b.up.sql", transactionControlWrap)
	writeLintE2EFile(c, migrations, "2_b.down.sql", "DELETE FROM users;\n")
	options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}
	database := "sqlite://" + filepath.Join(c.TempDir(), "t.db")

	lint := clirun.Run(c, clirun.Ptah, options, "migrations", "lint", "--dir", "migrations",
		"--runner", "golang-migrate", "--dialect", "sqlite", "--fail-on", "any")
	first := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--migrations-dir", "migrations",
		"--db-url", database, "--runner", "golang-migrate", "--to-version", "1")
	up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--migrations-dir", "migrations",
		"--db-url", database, "--runner", "golang-migrate")
	down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--migrations-dir", "migrations",
		"--db-url", database, "--target", "0", "--confirm", "--runner", "golang-migrate")

	c.Assert(lint.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", lint.Stdout, lint.Stderr))
	c.Assert(lint.Stdout, qt.Contains, "migrations/1_a.down.sql:1 [warning] TX201: the migrator runs this file in one "+
		"transaction and refuses a statement that controls transaction state, so the rollback stops at this BEGIN")
	c.Assert(lint.Stdout, qt.Contains, "migrations/2_b.up.sql:1 [warning] TX201: "+transactionControlWrapMessage)
	c.Assert(lint.Stdout, qt.Contains, "2 finding(s).")
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, "migration 2 cannot run tx-mode file up statement 1 because it controls transaction state")
	c.Assert(down.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(down.Stderr, qt.Contains,
		"migration 1 cannot run tx-mode file down statement 1 because it controls transaction state")
}

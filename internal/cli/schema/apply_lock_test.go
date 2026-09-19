package schema_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyRefusesLockTimeoutBeforeConnecting drives every dialect with
// no advisory lock at an address nothing is listening on. The refusal, rather
// than a connection failure, is what says the decision is made from the URL.
func TestSchemaApplyRefusesLockTimeoutBeforeConnecting(t *testing.T) {
	tests := []struct {
		name    string
		dbURL   string
		dialect string
	}{
		{name: "sqlite", dbURL: "sqlite://target.db", dialect: "sqlite"},
		{name: "clickhouse", dbURL: "clickhouse://127.0.0.1:1/db", dialect: "clickhouse"},
		{name: "cockroachdb", dbURL: "cockroachdb://root@127.0.0.1:1/db", dialect: "cockroachdb"},
		{name: "spanner", dbURL: "spanner://127.0.0.1:1/db", dialect: "spanner"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			t.Chdir(dir)
			schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

			out, err := runSchema("", "apply",
				"--db-url", test.dbURL,
				"--schema-file", schemaPath,
				"--lock-timeout", "5s",
				"--auto-approve",
			)

			c.Assert(err, qt.ErrorMatches, fmtLockRefusal(test.dialect), qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "connect to --db-url")
		})
	}
}

// TestSchemaApplyRefusedLockTimeoutAppliesNothing measures the target rather
// than the exit code: the reporter's complaint is that the apply went ahead.
func TestSchemaApplyRefusedLockTimeoutAppliesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, fmtLockRefusal("sqlite"), qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Planned schema changes:")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyRefusedLockTimeoutCreatesNoDatabase repeats the reporter's own
// command, where the SQLite file does not exist yet. An absent file is the
// strongest reading of "nothing was applied": the connection that would have
// created it was never opened.
func TestSchemaApplyRefusedLockTimeoutCreatesNoDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (\n    id INTEGER PRIMARY KEY,\n    name TEXT NOT NULL\n);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, fmtLockRefusal("sqlite"), qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestSchemaApplyRefusesEmptyLockTimeoutOnUnlockedDialect pins the predicate to
// presence. An empty value asks for an unbounded wait, which is a wait this
// target never makes either, so reading the string would let this spelling
// through in silence.
func TestSchemaApplyRefusesEmptyLockTimeoutOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--lock-timeout", "",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, fmtLockRefusal("sqlite"), qt.Commentf("%s", out))
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyPlanFileRefusesLockTimeoutOnUnlockedDialect covers the second
// apply path. A pre-approved plan executes SQL the same way, so it owes the
// same refusal, and it takes its own branch through the command.
func TestSchemaApplyPlanFileRefusesLockTimeoutOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	planOut, planErr := runSchema("", "plan",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--output", planPath,
	)
	c.Assert(planErr, qt.IsNil, qt.Commentf("%s", planOut))

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--plan", planPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, fmtLockRefusal("sqlite"), qt.Commentf("%s", out))
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyWithoutLockTimeoutStillAppliesOnUnlockedDialect is the control
// for the refusal above: an unlocked dialect keeps applying, silently, when
// nobody asked for a lock.
func TestSchemaApplyWithoutLockTimeoutStillAppliesOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(out, qt.Not(qt.Contains), "schema apply lock")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"orders", "users"})
}

// TestSchemaApplyKeepsLockTimeoutOnLockingDialect is the second control: the
// refusal must not fire where the lock exists. The address has no server, so
// the run gets as far as the connection and fails there, which is the part
// worth asserting -- a refusal that swallowed every dialect would never reach
// it.
func TestSchemaApplyKeepsLockTimeoutOnLockingDialect(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
	}{
		{name: "postgres", dbURL: "postgres://ptah@127.0.0.1:1/db?sslmode=disable"},
		{name: "mysql", dbURL: "mysql://ptah@127.0.0.1:1/db"},
		{name: "mariadb", dbURL: "mariadb://ptah@127.0.0.1:1/db"},
		{name: "sqlserver", dbURL: "sqlserver://ptah@127.0.0.1:1?database=db"},
		{name: "yugabytedb", dbURL: "yugabytedb://ptah@127.0.0.1:1/db?sslmode=disable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

			out, err := runSchema("", "apply",
				"--db-url", test.dbURL,
				"--schema-file", schemaPath,
				"--lock-timeout", "5s",
				"--auto-approve",
				"--connect-timeout", "2s",
			)

			c.Assert(err, qt.ErrorMatches, `(?s)connect to --db-url:.*`, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "session advisory lock")
		})
	}
}

// fmtLockRefusal renders the refusal a dialect with no advisory lock answers a
// typed --lock-timeout with.
func fmtLockRefusal(dialect string) string {
	return `--lock-timeout requested a schema apply lock, and dialect "` + dialect + `" has none: ` +
		`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
		`Remove --lock-timeout to apply without a lock`
}

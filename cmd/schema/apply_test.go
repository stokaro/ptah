package schema_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// seedSQLite executes DDL against a SQLite database file so tests start from a
// known live schema state.
func seedSQLite(c *qt.C, dbPath, schemaSQL string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, schemaSQL), qt.IsNil)
}

// listSQLiteTables returns the user tables of a SQLite database file.
func listSQLiteTables(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// runSchema executes a native `ptah schema ...` subcommand with args and
// returns combined output plus the execution error.
func runSchema(stdin string, args ...string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func writeSchemaSQLFile(c *qt.C, dir, name, content string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

func TestSchemaApplyAppliesSchemaFileWithAutoApprove(t *testing.T) {
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
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(out, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"orders", "users"})
}

func TestSchemaApplyDeclinedConfirmationAppliesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("no\n", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply canceled.")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

func TestSchemaApplyDryRunAppliesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

func TestSchemaApplySyncedSchemaReportsNoChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced, no changes to be made.")
}

func TestSchemaApplyRequiresDesiredSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "apply", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.ErrorMatches, "a desired schema source is required: .*", qt.Commentf("%s", out))
}

func TestSchemaApplyRejectsMalformedSQLiteVirtualDropToggleBeforeSourceWork(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runSchema("", "apply", "--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"))

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "desired schema source")
}

func TestSchemaApplyRejectsMalformedSQLiteToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--config", configPath,
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}

func TestSchemaApplyRejectsMalformedConfigSelectedSQLiteToggle(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("url: sqlite://target.db\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "apply", "--config", configPath)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
}

func TestSchemaApplyDoesNotApplySQLiteToggleBeforePostgresProjectConfig(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "apply",
		"--db-url", "postgres://localhost/database",
		"--config", configPath,
	)

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config .*unknown ptah.yaml key "unknown".*`, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
}

func TestSchemaApplyRefusesDevURLPointingAtTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--dev-url", "sqlite://"+dbPath,
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, ".*--dev-url must not point at the target database.*", qt.Commentf("%s", out))
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

func TestSchemaApplyDevSimulationRunsBeforeTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	devPath := filepath.Join(dir, "dev.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--dev-url", "sqlite://"+devPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"orders", "users"})
	// The dev database rehearsed the plan and was then handed back with
	// nothing in it, the way the pinned community binary leaves its own dev
	// database. A dev URL stays reusable by the next command.
	c.Assert(listSQLiteTables(c, devPath), qt.HasLen, 0)
}

func TestSchemaApplyPlanFileExecutesAndRefusesStaleTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "plan",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--output", planPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// Drift the target after the plan was computed: the fingerprint check must
	// refuse execution.
	seedSQLite(c, dbPath, "CREATE TABLE drifted (id INTEGER PRIMARY KEY);")
	out, err = runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--plan", planPath,
		"--auto-approve",
	)
	c.Assert(err, qt.ErrorMatches, "pre-planned migration is stale: .*", qt.Commentf("%s", out))

	// Reverting the drift makes the same plan executable.
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, "DROP TABLE drifted;"), qt.IsNil)
	dbschema.CloseAndWarn(conn)

	out, err = runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--plan", planPath,
		"--auto-approve",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"orders", "users"})
}

// TestSchemaApplyPlanFileRejectsAtlasHCLPlanByName pins the native tree's
// half of the interoperability contract: the native plan format stays JSON,
// but handing it an Atlas `.plan.hcl` must name the command that does read it
// instead of leaking a JSON decoder complaint about the letter 'p' — the
// exact defect the compat campaign filed.
func TestSchemaApplyPlanFileRejectsAtlasHCLPlanByName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	planPath := filepath.Join(dir, "atlas.plan.hcl")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile(planPath, []byte(
		"plan \"20260801102801\" {\n"+
			"  from      = \"2Avyplv6jw8kAsH/g2YFPkfnp+UNBpomMXPUl/4R4+Q=\"\n"+
			"  to        = \"YEugbm2aJqmXFA8dDrzmqLPC4tiNUrXe6YCrvazKOiY=\"\n"+
			"  migration = <<-SQL\n  CREATE TABLE `posts` (`id` integer);\n  SQL\n}\n",
	), 0o600), qt.IsNil)

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--plan", planPath,
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches,
		`plan file .* is in the Atlas \.plan\.hcl format, which the native .ptah schema apply --plan. does not read; `+
			`apply it with .ptah-compat schema apply --plan file://.* --to <desired state>., `+
			`or produce a native plan with .ptah schema plan --output <name>\.plan\.json.`)
	c.Assert(out, qt.Not(qt.Contains), "invalid character")
	c.Assert(listSQLiteTables(c, dbPath), qt.DeepEquals, []string{"users"})
}

func TestSchemaApplyPlanRejectsConflictingFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+dbPath,
		"--plan", filepath.Join(dir, "missing.plan.json"),
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(err, qt.ErrorMatches, "ptah schema apply --plan cannot be combined with --dev-url: .*", qt.Commentf("%s", out))
}

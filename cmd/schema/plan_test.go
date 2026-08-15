package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/safety"
)

func TestSchemaPlanSavesFingerprintedPlanFile(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan.db")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c.TB, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "plan",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--output", planPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(out, qt.Contains, "Plan saved to file://"+planPath)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
	c.Assert(plan.Dialect, qt.Equals, "sqlite")
	c.Assert(plan.FromFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.ToFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.Destructive, qt.IsFalse)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Safe)
}

func TestSchemaPlanRequiresSaveOutputOrDryRun(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan.db")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c.TB, dir, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "plan",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.ErrorMatches, "pass --save or --output <path> to write a local plan file, .*", qt.Commentf("%s", out))
}

func TestSchemaPlanRejectsMalformedSQLiteVirtualDropToggleBeforeSourceWork(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runSchema("", "plan", "--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"))

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "desired schema source")
}

func TestSchemaPlanRejectsMalformedSQLiteToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "plan",
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--config", configPath,
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}

func TestSchemaPlanDoesNotApplySQLiteToggleBeforePostgresProjectConfig(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("unknown: true\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "plan",
		"--db-url", "postgres://localhost/database",
		"--config", configPath,
	)

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config .*unknown ptah.yaml key "unknown".*`, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
}

func TestSchemaPlanDryRunPrintsDocumentWithoutSaving(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan.db")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	schemaPath := writeSchemaSQLFile(c.TB, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "plan",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `"format_version": 1`)
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	for _, entry := range entries {
		c.Assert(entry.Name(), qt.Not(qt.Matches), `.*\.plan\.json`)
	}
}

// TestSchemaPlanMatchesAtlasSchemaPlan proves the native verb and its Atlas
// twin produce the same plan document from the same inputs: both wrap
// atlasschema.PreparePlanFile, so the saved plan files must be byte-identical.
func TestSchemaPlanMatchesAtlasSchemaPlan(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	nativeDB := filepath.Join(dir, "native.db")
	atlasDB := filepath.Join(dir, "atlas.db")
	nativePlan := filepath.Join(dir, "native.plan.json")
	atlasPlan := filepath.Join(dir, "atlas.plan.json")
	const seed = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
	seedSQLite(c.TB, nativeDB, seed)
	seedSQLite(c.TB, atlasDB, seed)
	schemaPath := writeSchemaSQLFile(c.TB, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	nativeOut, err := runSchema("", "plan",
		"--db-url", "sqlite://"+nativeDB,
		"--schema-file", schemaPath,
		"--output", nativePlan,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))

	atlasCmd := atlas.NewCompatCommand("atlas")
	var atlasOut bytes.Buffer
	atlasCmd.SetOut(&atlasOut)
	atlasCmd.SetErr(&atlasOut)
	atlasCmd.SetArgs([]string{"schema", "plan",
		"--from", "sqlite://" + atlasDB,
		"--to", "file://" + schemaPath,
		"--output", atlasPlan,
	})
	c.Assert(atlasCmd.Execute(), qt.IsNil, qt.Commentf("%s", atlasOut.String()))

	nativeDocument, err := os.ReadFile(nativePlan)
	c.Assert(err, qt.IsNil)
	atlasDocument, err := os.ReadFile(atlasPlan)
	c.Assert(err, qt.IsNil)
	c.Assert(string(nativeDocument), qt.Equals, string(atlasDocument))
}

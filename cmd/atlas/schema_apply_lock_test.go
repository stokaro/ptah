package atlas_test

// CLI coverage for `atlas schema apply --lock-timeout` and the --dev-url
// dev-database plan simulation added for stokaro/ptah#812.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

const schemaApplyLockUnsupportedNote = `note: schema apply locking is not supported for dialect "sqlite"; --lock-timeout is ignored and the apply proceeds without a database lock`

func TestSchemaApplyLockTimeoutSQLiteIsExplicitNoOp(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lock-apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE lock_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--lock-timeout", "10s",
		"--auto-approve",
	})

	err := cmd.Execute()

	// SQLite has no advisory-lock semantics: the capability decision is an
	// explicit no-op with a note, and the apply itself proceeds normally.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, schemaApplyLockUnsupportedNote)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "lock_users"), qt.Equals, 1)
}

func TestSchemaApplyWithoutLockTimeoutPrintsNoLockNote(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "no-note.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE quiet_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--auto-approve",
	})

	err := cmd.Execute()

	// Without an explicit --lock-timeout the unsupported-dialect no-op stays
	// silent, matching the migrator's behavior on lockless dialects.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Not(qt.Contains), "note: schema apply locking is not supported")
}

func TestSchemaApplyPlanFileAcceptsLockTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lock-plan.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE locked_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--auto-approve", "--lock-timeout", "10s")

	// The pre-approved plan path serializes its fingerprint verification and
	// execution under the same apply lock, so --lock-timeout is accepted there
	// too instead of being rejected.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, schemaApplyLockUnsupportedNote)
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "locked_orders"), qt.Equals, 1)
}

func TestSchemaApplyDevSimulationRunsPlanOnDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sim-target.db")
	devPath := filepath.Join(dir, "sim-dev.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE sim_users (id INTEGER PRIMARY KEY);`)
	// Pre-litter the dev database: the simulation must reset it first.
	seedSQLiteSchema(c, devPath, `CREATE TABLE sim_stale (id INTEGER PRIMARY KEY);`)
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE sim_users (id INTEGER PRIMARY KEY);
CREATE TABLE sim_orders (id INTEGER PRIMARY KEY);
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + devPath,
		"--auto-approve",
	})

	err := cmd.Execute()

	// The dev database ends at recreated current state plus the rehearsed
	// plan, with stale objects gone; the target gets the plan afterwards.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, devPath, "sim_users"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, devPath, "sim_orders"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, devPath, "sim_stale"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "sim_orders"), qt.Equals, 1)
}

func TestSchemaApplyDevSimulationFailureLeavesTargetUnchanged(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sim-fail-target.db")
	devPath := filepath.Join(dir, "sim-fail-dev.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE sim_fail_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	// The editor appends a statement that collides with the planned one, so
	// the rehearsal on the dev database fails deterministically.
	installAppendEditor(t, "CREATE TABLE sim_fail_users (id INTEGER PRIMARY KEY);")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + devPath,
		"--edit",
		"--auto-approve",
	})

	err := cmd.Execute()

	// The edited SQL is rehearsed exactly as it would be applied; the failed
	// simulation refuses the apply and the target database stays unchanged.
	c.Assert(err, qt.ErrorMatches, `(?s)dev database simulation failed during plan: .*sim_fail_users.*; the plan was not applied to the target database`)
	c.Assert(out.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "sim_fail_users"), qt.Equals, 0)
}

func TestSchemaApplyDevURLMustDifferFromTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sim-same.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE sim_same_users (id INTEGER PRIMARY KEY);`)
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE sim_same_users (id INTEGER PRIMARY KEY);
CREATE TABLE sim_same_orders (id INTEGER PRIMARY KEY);
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + dbPath,
		"--auto-approve",
	})

	err := cmd.Execute()

	// Simulation resets the dev database destructively, so pointing --dev-url
	// at the target must refuse before anything is dropped.
	c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
	c.Assert(sqliteTableCount(c, dbPath, "sim_same_users"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "sim_same_orders"), qt.Equals, 0)
}

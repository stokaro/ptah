package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/migration/migrator"
)

// These tests pin the integrity gate on `ptah migrations down`.
//
// The defect they close was recorded on stokaro/ptah#928 item 4 and re-measured
// before the fix: on ONE hashed directory whose `_init.down.sql` was rewritten
// with ptah.sum left stale, `migrations up` exited 2 and refused, while
// `migrations down --target 0 --confirm` exited 0 and executed the rewritten
// file. A post-run census of the SQLite catalog listed `evil_down` — a table
// that appears in no committed migration — and no longer listed `widgets`.
//
// Verification guarding the constructive direction and not the destructive one
// is backwards. `down` is the direction where an operator cannot inspect the
// result afterwards, because the objects are gone either way.
//
// Every assertion below about what did or did not execute is made against the
// real catalog through [tableCensus], never against the plan or the printed
// SQL: a rollback that was refused and a rollback that ran and rolled itself
// back print differently but must be distinguished by what the database holds.

// evilDownSQL is the rewritten down migration. It CREATES a table before
// dropping the real one, so the catalog can tell "the attacker's SQL ran" from
// "the honest rollback ran" — dropping alone would be indistinguishable from a
// legitimate rollback.
const evilDownSQL = "CREATE TABLE evil_down (id INTEGER PRIMARY KEY);\nDROP TABLE widgets;\n"

// writeHashedWidgetsDir writes the one-migration ptah-format fixture and hashes
// it, so ptah.sum records the HONEST down file.
func writeHashedWidgetsDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	return dir
}

// rewriteDownFile replaces the down migration and deliberately does NOT
// re-hash, which is the whole fixture: the bytes on disk no longer match the
// bytes ptah.sum recorded.
func rewriteDownFile(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(dir, "0000000001_init.down.sql")
	c.Assert(os.WriteFile(path, []byte(evilDownSQL), 0o600), qt.IsNil)
}

// applyWidgets brings a fresh database to version 1 through the honest
// directory, so the rollback under test has something to roll back.
func applyWidgets(c *qt.C, dir, dbPath string) {
	c.Helper()
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}

func runDown(args ...string) (string, error) {
	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// tableCensus reads the real catalog and returns the sorted user table names.
//
// This is the "rendered is not applied" half. Reading the rollback plan would
// say what the tool intended; only the catalog says what the database now
// holds, and the tampered file's signature is a table nobody committed.
func tableCensus(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(names)
	return names
}

// TestMigrateDown_TamperedHashedDirRefusesAndLeavesCatalogIntact is the
// regression for the recorded defect, asserted on the catalog.
//
// The three assertions are separate on purpose. The refusal proves the gate
// fires; `widgets` still present proves the honest rollback did not run either,
// so the refusal happened BEFORE execution rather than partway through; and
// `evil_down` absent proves the rewritten statement never reached the database.
// The first alone would also hold for a run that executed everything and then
// reported an error.
func TestMigrateDown_TamperedHashedDirRefusesAndLeavesCatalogIntact(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "down.db")
	applyWidgets(c, dir, dbPath)
	rewriteDownFile(c, dir)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "changed: 0000000001_init.down.sql")
	census := tableCensus(c, dbPath)
	c.Assert(census, qt.Contains, "widgets")
	c.Assert(census, qt.Not(qt.Contains), "evil_down")
}

// TestMigrateDown_TamperedHashedDirRefusesBeforeConfirmation pins the gate's
// POSITION, not merely its existence.
//
// The prompt is the operator's last chance to stop, and a run that asks "type
// YES to roll back" before it has decided whether the directory is even
// runnable has spent that chance on a run it was always going to refuse. The
// empty stdin is the discriminator: a gate placed after the prompt would block
// reading a confirmation and fail with a read error instead of the checksum
// error, and the assertion on the message is what tells the two apart.
func TestMigrateDown_TamperedHashedDirRefusesBeforeConfirmation(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "prompt.db")
	applyWidgets(c, dir, dbPath)
	rewriteDownFile(c, dir)

	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir, "--target", "0"})
	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out.String()))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Not(qt.Contains), "read rollback confirmation")
	c.Assert(out.String(), qt.Not(qt.Contains), "Type 'YES' to confirm")
}

// TestMigrateDown_UnhashedDirStaysUngated pins the boundary the gate must NOT
// cross.
//
// A directory nobody ever hashed carries no recorded intent to compare against,
// so refusing it would remove a capability rather than protect one. This is the
// same boundary `migrations up` has held since stokaro/ptah#955, and it is what
// keeps the fix from being a behavior change for every operator who does not
// use ptah.sum at all.
func TestMigrateDown_UnhashedDirStaysUngated(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(c.TempDir(), "unhashed.db")
	applyWidgets(c, dir, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "widgets")
}

// TestMigrateDown_CleanHashedDirRollsBack is the non-interference control: the
// gate must refuse drift and nothing else.
//
// Without it, a gate that refused every hashed directory would pass every
// assertion above. The census is what makes it a control rather than a smoke
// test — it proves the rollback reached the catalog, not merely that the
// command exited 0.
func TestMigrateDown_CleanHashedDirRollsBack(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "clean.db")
	applyWidgets(c, dir, dbPath)
	c.Assert(tableCensus(c, dbPath), qt.Contains, "widgets")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "widgets")
}

// TestMigrateDown_EscapeHatchExecutesAndSaysWhatItSkipped pins the capability
// the gate must not remove, and the announcement that has to accompany it.
//
// Repository policy forbids removing a capability, and an operator recovering
// from a botched edit genuinely needs to roll back through a directory whose
// sum is stale — re-hashing first would record the botched bytes as intended.
// So the escape exists; the assertions here are that it WORKS (the catalog
// shows the rewritten file executed) and that it is NEVER SILENT.
//
// The notice assertions are load-bearing individually: naming the variable
// tells the operator what to unset, and naming the drifted file tells them what
// they just accepted. A warning that said only "verification skipped" would
// pass a weaker test and help nobody.
func TestMigrateDown_EscapeHatchExecutesAndSaysWhatItSkipped(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "escape.db")
	applyWidgets(c, dir, dbPath)
	rewriteDownFile(c, dir)
	c.Setenv(migrationintegrity.AllowUnverifiedEnvVar, "true")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, migrationintegrity.AllowUnverifiedEnvVar)
	c.Assert(out, qt.Contains, "verification was SKIPPED")
	c.Assert(out, qt.Contains, "changed: 0000000001_init.down.sql")
	// The override really did execute the rewritten file, which is the
	// capability being preserved.
	c.Assert(tableCensus(c, dbPath), qt.Contains, "evil_down")
}

// TestMigrateDown_EscapeHatchStaysSilentOnACleanDirectory pins that the notice
// fires on an actual override and not on the variable being set.
//
// A run against a directory that verifies has skipped nothing, so it has
// nothing to announce. Announcing anyway would train operators to read the
// warning as noise, which is how the one run that mattered gets ignored.
func TestMigrateDown_EscapeHatchStaysSilentOnACleanDirectory(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "escape-clean.db")
	applyWidgets(c, dir, dbPath)
	c.Setenv(migrationintegrity.AllowUnverifiedEnvVar, "true")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), migrationintegrity.AllowUnverifiedEnvVar)
	c.Assert(out, qt.Not(qt.Contains), "SKIPPED")
}

// TestMigrateDown_EscapeHatchRejectsAnUnparseableValue pins the envbool
// contract on this variable: absence selects the default, a present value has
// to parse, and anything else refuses the command.
//
// The refusal has to happen on a CLEAN directory too, which is what this test
// fixes. A reader that only consulted the variable on the drifted path would
// let `PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR=yes` sit unnoticed in a CI
// environment file until the day it was load-bearing — and on that day it reads
// as false, the strict side, which is the safe direction but not the one the
// operator believes they configured.
func TestMigrateDown_EscapeHatchRejectsAnUnparseableValue(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "badenv.db")
	applyWidgets(c, dir, dbPath)
	c.Setenv(migrationintegrity.AllowUnverifiedEnvVar, "yes")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, migrationintegrity.AllowUnverifiedEnvVar)
	c.Assert(err.Error(), qt.Contains, `"yes"`)
	// The clean directory was never the reason: nothing was rolled back.
	c.Assert(tableCensus(c, dbPath), qt.Contains, "widgets")
}

func TestMigrateDown_EscapeHatchRejectsAnUnparseableValueBeforeArguments(t *testing.T) {
	c := qt.New(t)
	c.Setenv(migrationintegrity.AllowUnverifiedEnvVar, "yes")

	_, err := runDown()

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "yes" for PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR`)
}

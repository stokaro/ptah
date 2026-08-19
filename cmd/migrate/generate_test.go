package migrate_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/safety"
)

func TestMigrateGenerateCommandExposesShadowDBFlag(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateGenerateCommand()

	c.Assert(cmd.Name(), qt.Equals, "generate")
	c.Assert(cmd.Flags().Lookup("shadow-db"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("config"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("env"), qt.IsNotNil)
}

func TestMigrateGenerateProjectConfigPrecedence(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("migrate:\n  generate:\n    shadow_db: postgres://localhost/ptah_shadow\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "local" {
  dev = "postgres://localhost/atlas_shadow"
}
`), 0o600), qt.IsNil)

	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := migrate.NewMigrateGenerateCommand()
	c.Assert(cmd.ParseFlags([]string{"--shadow-db", "postgres://localhost/flag_shadow"}), qt.IsNil)
	flagShadow, err := cmd.Flags().GetString("shadow-db")
	c.Assert(err, qt.IsNil)
	cfg, err := dbcli.LoadProjectConfig(cmd, "")
	c.Assert(err, qt.IsNil)

	shadowDB := dbcli.EffectiveString(
		cmd,
		"shadow-db",
		flagShadow,
		cfg.StringValue(projectconfig.StringDevURL),
	)

	c.Assert(shadowDB, qt.Equals, "postgres://localhost/flag_shadow")

	cmd = migrate.NewMigrateGenerateCommand()
	cfg, err = dbcli.LoadProjectConfig(cmd, "")
	c.Assert(err, qt.IsNil)
	shadowDB = dbcli.EffectiveString(
		cmd,
		"shadow-db",
		"",
		cfg.StringValue(projectconfig.StringDevURL),
	)
	c.Assert(shadowDB, qt.Equals, "postgres://localhost/atlas_shadow")
}

func TestMigratePlanCommandRejectsAtlasApplyAtRoot(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["apply"\]`)
}

func TestMigrateGenerateJSONReportWritesSiblingSafetyArtifact(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	var stdout, stderr bytes.Buffer
	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--schema-file", schemaFile,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
		"--migrations-dir", migrationsDir,
		"--name", "init",
		"--report", "json",
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	reportFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*_init.safety.json"))
	c.Assert(err, qt.IsNil)
	c.Assert(reportFiles, qt.HasLen, 1)
	c.Assert(stdout.String(), qt.Contains, "REPORT: ")
	c.Assert(stdout.String(), qt.Contains, filepath.Base(reportFiles[0]))

	rawReport, err := os.ReadFile(reportFiles[0])
	c.Assert(err, qt.IsNil)
	var report safety.Report
	c.Assert(json.Unmarshal(rawReport, &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Safe)
	c.Assert(report.Destructive, qt.IsFalse)
	c.Assert(report.Assessments, qt.HasLen, 1)
}

func TestMigrateGenerateValidatesSQLiteToggleBeforeMigrationsDirectoryPath(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + filepath.Join(root, "target.db"),
		"--migrations-dir", "../outside",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
}

func TestMigrateGenerateDoesNotValidateSQLiteToggleForPostgresPathFailure(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetArgs([]string{
		"--db-url", "postgres://localhost/ptah",
		"--migrations-dir", filepath.Join(root, "migrations"),
	})

	err := cmd.Execute()

	// The run fails for a PostgreSQL reason -- there is no server on that URL
	// -- and that IS the claim: a malformed SQLite toggle is not validated on a
	// path that never reaches SQLite.
	//
	// The assertion used to read the toggle's absence off a path refusal, with
	// "--migrations-dir ../outside" short-circuiting the run before the
	// connection. stokaro/ptah#1622 removed that refusal, and leaning on an
	// unrelated guard to prove this claim was the weaker spelling anyway: it
	// passed for any early failure at all.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), sqlitevirtual.AllowDropEnvVar)
}

// TestMigrateGenerateResolvesADockerDevURL covers stokaro/ptah#1701.
//
// `--dev-url docker://…` used to be handed straight to the connector, which
// answered `unsupported database dialect: docker` — naming a dialect the user
// never wrote, for the native spelling of a workflow ptah-compat migrate diff
// provisions for.
//
// The assertion is the absence of that sentence rather than a success, because
// what happens next depends on whether a container runtime is reachable: with
// one the run provisions and proceeds, without one it fails naming the daemon.
// Both prove the value reached the provisioner, which is the whole change; only
// the old behavior can produce the dialect error.
func TestMigrateGenerateResolvesADockerDevURL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(dir, "models"), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "models", "models.go"), []byte(`package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	cmd := migrate.NewMigrateGenerateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--root-dir", filepath.Join(dir, "models"),
		"--migrations-dir", filepath.Join(dir, "migrations"),
		"--dev-url", "docker://postgres/18/dev",
		"--replay",
		"--name", "init",
	})

	err := cmd.Execute()

	// A run that reached the provisioner may still fail for reasons this test
	// does not control. What it may never do again is report the scheme as a
	// dialect.
	c.Assert(errorText(err), qt.Not(qt.Contains), "unsupported database dialect: docker")
	c.Assert(out.String(), qt.Not(qt.Contains), "unsupported database dialect: docker")
}

// errorText renders an error for an assertion that must hold whether or not the
// run failed, so the caller states one expectation instead of branching on the
// outcome it does not control.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

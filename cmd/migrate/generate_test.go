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

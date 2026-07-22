package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestSchemaApplySupportsFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--format", `{{ len .Changes }}|{{ printf "%.6s" (.MarshalSQL) }}|{{ sql . "  " }}`,
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "1|CREATE|  CREATE TABLE")
	c.Assert(out.String(), qt.Not(qt.Contains), "Auto-approval enabled")
	c.Assert(out.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaApplyFormatDryRunDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format-dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE dry_run_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--format", `{{ sql . "" }}`,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(sqliteTableCount(c, dbPath, "dry_run_users"), qt.Equals, 0)
}

func TestSchemaApplyFormatSeparatesInteractivePrompt(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format-prompt.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE prompt_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("NO\n"))
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--format", `{{ len .Changes }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "1\nApply these schema changes?")
	c.Assert(out.String(), qt.Contains, "Schema apply canceled.")
	c.Assert(sqliteTableCount(c, dbPath, "prompt_users"), qt.Equals, 0)
}

func TestSchemaApplyFormatReportsSynced(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format-synced.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	schemaSQL := `CREATE TABLE synced_users (id INTEGER PRIMARY KEY);`
	c.Assert(os.WriteFile(schemaPath, []byte(schemaSQL), 0o600), qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, schemaSQL), qt.IsNil)
	dbschema.CloseAndWarn(conn)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--format", `{{ with .Changes }}changed{{ else }}synced{{ end }}`,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
}

func TestSchemaApplyUsesAtlasProjectEnvSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  src = "schema.sql"
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "env_users"), qt.Equals, 1)
}

func TestSchemaApplyUsesAtlasProjectEnvSchemaBlockAndFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-schema-format.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_schema_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    src = "schema.sql"
  }
  format {
    schema {
      apply = "{{ len .Changes }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1")
	c.Assert(sqliteTableCount(c, dbPath, "env_schema_users"), qt.Equals, 1)
}

func TestSchemaApplyUsesAtlasProjectSchemaMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-schema-mode.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_schema_mode_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    src = "schema.sql"
    mode {
      tables = false
    }
  }
  format {
    schema {
      apply = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
	c.Assert(sqliteTableCount(c, dbPath, "env_schema_mode_users"), qt.Equals, 0)
}

func TestSchemaApplyUsesAtlasProjectDefaultsWithExplicitTargetFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-explicit-target-defaults.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE explicit_target_defaults (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  schema {
    mode {
      tables = false
    }
  }
  format {
    schema {
      apply = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "schema.sql",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
	c.Assert(sqliteTableCount(c, dbPath, "explicit_target_defaults"), qt.Equals, 0)
}

func TestSchemaApplyUsesEvaluatedAtlasProjectEnvSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-eval.db")
	schemaDir := filepath.Join(dir, "schema")
	c.Assert(os.Mkdir(schemaDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(schemaDir, "users.hcl"), []byte(`schema "main" {}

table "env_eval_users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`variable "db_url" {
  default = "sqlite://`+dbPath+`"
}

data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = var.db_url
  src = data.hcl_schema.app.url
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "env_eval_users"), qt.Equals, 1)
}

func TestSchemaApplyUsesAtlasProjectEnvExclude(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-exclude.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`
CREATE TABLE env_keep (id INTEGER PRIMARY KEY);
CREATE TABLE env_skip (id INTEGER PRIMARY KEY);
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  src = "schema.sql"
  dev = "sqlite://dev.db"
  exclude = ["env_skip"]
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "env_keep"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "env_skip"), qt.Equals, 0)
}

func TestSchemaApplyUsesAtlasProjectDiffSkipDropTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-diff-skip.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, `CREATE TABLE old_users (id INTEGER PRIMARY KEY);`), qt.IsNil)
	dbschema.CloseAndWarn(conn)
	c.Assert(os.WriteFile("schema.hcl", []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    src = "schema.hcl"
  }
  format {
    schema {
      apply = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
  diff {
    skip {
      drop_table = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
	c.Assert(sqliteTableCount(c, dbPath, "old_users"), qt.Equals, 1)
}

func TestSchemaApplyAllowsAtlasProjectConcurrentIndexPolicyForSQLite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-concurrent-index.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    src = "schema.sql"
  }
  diff {
    concurrent_index {
      create = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaApplyPrefersExplicitFlagsOverProjectEnv(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	configDBPath := filepath.Join(dir, "config.db")
	cliDBPath := filepath.Join(dir, "cli.db")
	c.Assert(os.WriteFile("config.sql", []byte(`CREATE TABLE config_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("cli.sql", []byte(`CREATE TABLE cli_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+configDBPath+`"
  src = "config.sql"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--url", "sqlite://" + cliDBPath,
		"--to", "cli.sql",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, cliDBPath, "cli_users"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, configDBPath, "config_users"), qt.Equals, 0)
}

func TestSchemaApplyExplicitFlagsIgnoreUnneededAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "explicit.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE explicit_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = "sqlite://prod.db"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "schema.sql",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "explicit_users"), qt.Equals, 1)
}

func TestSchemaApplyAtlasEnvIgnoresMismatchedPtahEnv(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "atlas-env.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE atlas_env_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  other:
    url: sqlite://other.db
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  src = "schema.sql"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "atlas_env_users"), qt.Equals, 1)
}

func TestSchemaApplyRejectsInvalidFormatBeforeLoadingFiles(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://apply.db",
		"--to", "file://schema.sql",
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --url")
}

func sqliteTableCount(c *qt.C, dbPath, table string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

package schematests_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/migration/migrator"
)

func TestSchemaApplySupportsFormat(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(out.String(), qt.Contains, "1|-- Cre|-- Create \"users\" table\nCREATE TABLE")
	c.Assert(out.String(), qt.Not(qt.Contains), "Auto-approval enabled")
	c.Assert(out.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

// TestSchemaApplyFormatRendersJSON is the end-to-end half of the
// stokaro/ptah#940 item C regression test: on the command line
// `--format '{{ json . }}'` exits 1 at template-parse time with
// `function "json" not defined` unless the helper is registered, where the
// pinned community binary v1.3.0 renders the document and exits 0.
func TestSchemaApplyFormatRendersJSON(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-json.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE json_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--format", `{{ json . }}`,
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `"Driver":"sqlite"`)
	c.Assert(out.String(), qt.Contains, `"Changes":{"Applied":[`)
	c.Assert(out.String(), qt.Contains, "json_users")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "json_users"), qt.Equals, 1)
}

func TestSchemaApplyFormatDryRunDoesNotApply(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format-dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE dry_run_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "dry_run_users"), qt.Equals, 0)
}

func TestSchemaApplyFormatSeparatesInteractivePrompt(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-format-prompt.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE prompt_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "prompt_users"), qt.Equals, 0)
}

func TestSchemaApplyFormatReportsSynced(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
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

	cmd := atlas.NewCompatCommand("atlas")
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
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
  src = "schema.sql"
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_users"), qt.Equals, 1)
}

func TestSchemaApplyUsesAtlasProjectEnvSchemaBlockAndFormat(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-schema-format.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_schema_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
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

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_schema_users"), qt.Equals, 1)
}

func TestSchemaApplyUsesAtlasProjectSchemaMode(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-schema-mode.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_schema_mode_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
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

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_schema_mode_users"), qt.Equals, 0)
}

func TestSchemaApplyUsesAtlasProjectDefaultsWithExplicitTargetFlags(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
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

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--env", "local",
		"--url", "sqlite://" + dbPath,
		"--to", "schema.sql",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "explicit_target_defaults"), qt.Equals, 0)
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
  default = "sqlite://`+filepath.ToSlash(dbPath)+`"
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

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_eval_users"), qt.Equals, 1)
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
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
  src = "schema.sql"
  dev = "sqlite://dev.db"
  exclude = ["env_skip"]
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_keep"), qt.Equals, 1)
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "env_skip"), qt.Equals, 0)
}

func TestSchemaApplyUsesAtlasProjectDiffSkipDropTable(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
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
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
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

	stdout, stderr, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "synced")
	c.Assert(stderr, qt.Equals, "SKIP: DROP TABLE of old_users omitted by diff policy (skip: drop_table)\n")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "old_users"), qt.Equals, 1)
}

// skipColumnAndIndexPolicy is the env block that skips column and index drops.
const skipColumnAndIndexPolicy = "  diff {\n    skip {\n      drop_index = true\n      drop_column = true\n    }\n  }\n"

// skipColumnAndIndexProject is a SQLite database holding t(id, a, b) and an
// index on a, a desired schema of t(id, a) plus whatever extra declares, and
// an atlas.hcl env carrying policy. Dropping b is a table rebuild on SQLite,
// and a rebuild loses every index the desired table does not declare.
func skipColumnAndIndexProject(c *qt.C, extra, policy string) string {
	c.Helper()
	dir := c.TempDir()
	c.Chdir(dir)
	dbPath := filepath.Join(dir, "skip.db")
	atlastest.SqliteExec(c, dbPath, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT); CREATE INDEX t_a ON t(a);")
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT"+extra+");\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
  src = "file://schema.sql"
`+policy+`}
`), 0o600), qt.IsNil)
	return dbPath
}

// TestSchemaApplyUsesAtlasProjectDiffSkipDropColumnAndIndex pins the two
// drops the community binary v1.3.0 withholds under this policy: measured on
// SQLite, it answers `Schema is synced, no changes to be made` where the
// unskipped run rebuilds t without b and loses t_a. The plan here says what it
// left out, which the community binary does not.
func TestSchemaApplyUsesAtlasProjectDiffSkipDropColumnAndIndex(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dbPath := skipColumnAndIndexProject(c, "", skipColumnAndIndexPolicy)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "Schema is synced, no changes to be made\n")
	c.Assert(stderr, qt.Equals, "SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n"+
		"SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n")
	c.Assert(atlastest.SqliteQueryInt(c, dbPath, "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'b'"), qt.Equals, 1)
	c.Assert(atlastest.SqliteIndexCount(c, dbPath, "t_a"), qt.Equals, 1)
}

// TestSchemaApplyUsesAtlasProjectDiffSkipBesideAPlannedChange pins the plan a
// skip leaves something in: the column is added, b and t_a stay, and the plan
// opens with the comment for each change it left out. The community binary
// v1.3.0 plans the same single ADD COLUMN, without the comment.
func TestSchemaApplyUsesAtlasProjectDiffSkipBesideAPlannedChange(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dbPath := skipColumnAndIndexProject(c, ", c TEXT", skipColumnAndIndexPolicy)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n"+
		"-- SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n")
	c.Assert(stdout, qt.Contains, `ALTER TABLE "t" ADD COLUMN "c" text NULL`)
	c.Assert(stdout, qt.Not(qt.Contains), "DROP TABLE")
	c.Assert(atlastest.SqliteQueryInt(c, dbPath, "SELECT count(*) FROM pragma_table_info('t') WHERE name IN ('b', 'c')"), qt.Equals, 2)
	c.Assert(atlastest.SqliteIndexCount(c, dbPath, "t_a"), qt.Equals, 1)
}

// TestSchemaApplyWithoutAtlasProjectDiffSkipDropsColumnAndIndex is the
// control: the same project without the policy rebuilds the table, so the
// rows above measure the policy and not the fixture.
func TestSchemaApplyWithoutAtlasProjectDiffSkipDropsColumnAndIndex(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dbPath := skipColumnAndIndexProject(c, "", "")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, `DROP TABLE "t"`)
	c.Assert(stdout, qt.Not(qt.Contains), "SKIP:")
	c.Assert(atlastest.SqliteQueryInt(c, dbPath, "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'b'"), qt.Equals, 0)
	c.Assert(atlastest.SqliteIndexCount(c, dbPath, "t_a"), qt.Equals, 0)
}

func TestSchemaApplyAllowsAtlasProjectConcurrentIndexPolicyForSQLite(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "project-env-concurrent-index.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
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

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaApplyPrefersExplicitFlagsOverProjectEnv(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	configDBPath := filepath.Join(dir, "config.db")
	cliDBPath := filepath.Join(dir, "cli.db")
	c.Assert(os.WriteFile("config.sql", []byte(`CREATE TABLE config_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("cli.sql", []byte(`CREATE TABLE cli_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+filepath.ToSlash(configDBPath)+`"
  src = "config.sql"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, cliDBPath, "cli_users"), qt.Equals, 1)
	c.Assert(atlastest.SqliteTableCount(c, configDBPath, "config_users"), qt.Equals, 0)
}

func TestSchemaApplyExplicitFlagsIgnoreUnneededAtlasProjectConfig(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
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

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "explicit_users"), qt.Equals, 1)
}

func TestSchemaApplyAtlasEnvIgnoresMismatchedPtahEnv(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
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
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
  src = "schema.sql"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
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
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "atlas_env_users"), qt.Equals, 1)
}

func TestSchemaApplyRejectsInvalidFormatBeforeLoadingFiles(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("atlas")
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

	c.Assert(err, qt.ErrorMatches, `parse log format: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --url")
}

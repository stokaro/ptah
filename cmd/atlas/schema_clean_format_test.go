package atlas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestSchemaCleanFormatDryRunJSONDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-dry-run.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath + "?password=hidden",
		"--dry-run",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	got := schemaCleanJSONReport{}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Env.Driver, qt.Equals, "sqlite")
	c.Assert(got.Env.URL, qt.Equals, "sqlite://"+dbPath+"?password=xxxxx")
	c.Assert(got.DryRun, qt.Equals, true)
	c.Assert(got.Applied, qt.Equals, false)
	c.Assert(got.Objects, qt.DeepEquals, []schemaCleanJSONObject{{Type: "table", Name: "users"}})
	c.Assert(got.Changes, qt.DeepEquals, []schemaCleanJSONChange{
		{Type: "table", Name: "users", Cmd: `DROP TABLE IF EXISTS "users"`},
	})
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaCleanFormatCustomTemplate(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-template.db")
	createSQLiteSchemaCleanTable(c, dbPath, "events")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--dry-run",
		"--format", `{{ .Env.Driver }}|{{ len .Changes }}|{{ (index .Changes 0).Cmd }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `sqlite|1|DROP TABLE IF EXISTS "events"`)
	c.Assert(sqliteTableCount(c, dbPath, "events"), qt.Equals, 1)
}

func TestSchemaCleanInvalidFormatFailsBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-invalid.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --url")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaCleanRuntimeInvalidFormatFailsBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-runtime-invalid.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ .DoesNotExist }}",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --url")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaCleanActualInvalidFormatFailsBeforeApplying(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-actual-invalid.db")
	createSQLiteSchemaCleanTable(c, dbPath, "kept_users")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ if .Applied }}{{ .DoesNotExist }}{{ end }}",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*`)
	c.Assert(sqliteTableCount(c, dbPath, "kept_users"), qt.Equals, 1)
}

func TestSchemaCleanFormatRequiresInteractiveApproval(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-prompt.db")
	createSQLiteSchemaCleanTable(c, dbPath, "prompt_users")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("NO\n"))
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ len .Changes }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "1\nWARNING: This operation will permanently delete")
	c.Assert(out.String(), qt.Contains, "Schema clean canceled.")
	c.Assert(sqliteTableCount(c, dbPath, "prompt_users"), qt.Equals, 1)
}

func TestSchemaCleanFormatAutoApproveApplies(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-format-apply.db")
	createSQLiteSchemaCleanTable(c, dbPath, "applied_users")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ json . }}",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	got := schemaCleanJSONReport{}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.DryRun, qt.Equals, false)
	c.Assert(got.Applied, qt.Equals, true)
	c.Assert(got.Changes, qt.DeepEquals, []schemaCleanJSONChange{
		{Type: "table", Name: "applied_users", Cmd: `DROP TABLE IF EXISTS "applied_users"`},
	})
	c.Assert(sqliteTableCount(c, dbPath, "applied_users"), qt.Equals, 0)
}

func TestSchemaCleanDefaultOutputDryRun(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-default-dry-run.db")
	createSQLiteSchemaCleanTable(c, dbPath, "default_users")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--url", "sqlite://" + dbPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "[DRY RUN] Would clean schema objects")
	c.Assert(out.String(), qt.Contains, `- DROP TABLE IF EXISTS "default_users"`)
	c.Assert(sqliteTableCount(c, dbPath, "default_users"), qt.Equals, 1)
}

func TestSchemaCleanUsesAtlasProjectEnvURLAndFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "clean-project-format.db")
	createSQLiteSchemaCleanTable(c, dbPath, "project_users")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  format {
    schema {
      clean = "{{ .Env.Driver }}:{{ len .Changes }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "clean",
		"--env", "local",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite:1")
	c.Assert(sqliteTableCount(c, dbPath, "project_users"), qt.Equals, 1)
}

type schemaCleanJSONReport struct {
	Env struct {
		Driver string
		URL    string
	}
	DryRun  bool
	Applied bool
	Objects []schemaCleanJSONObject
	Changes []schemaCleanJSONChange
}

type schemaCleanJSONObject struct {
	Type   string
	Schema string `json:",omitempty"`
	Name   string
}

type schemaCleanJSONChange struct {
	Type   string
	Schema string `json:",omitempty"`
	Name   string
	Cmd    string
}

func createSQLiteSchemaCleanTable(c *qt.C, dbPath, table string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(
		context.Background(),
		conn,
		migrator.MigrationTxModeAll,
		"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY)",
	), qt.IsNil)
}

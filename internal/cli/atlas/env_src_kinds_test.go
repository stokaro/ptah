package atlas_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasregistry"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/schemaartifacttest"
	"ptah.run/migration/migrationfile"
)

// registryUsers is the desired schema the registry rows below pull: one
// `users` table.
func registryUsers() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
	}
}

// TestSchemaVerbsWithoutToReadEveryEnvSourceKind walks the desired-state
// source kinds an env's `src` can name, and runs `schema diff` and `schema
// apply` with no --to on each. An omitted --to takes the env's source, and
// must reach every kind `--to env://src` reaches; a kind left out is read as a
// local file and refused as one (stokaro/ptah#4061).
//
// Each row's desired schema creates one table the row names, so a row that
// silently read nothing, or another row's state, cannot pass.
func TestSchemaVerbsWithoutToReadEveryEnvSourceKind(t *testing.T) {
	tests := []struct {
		name string
		// data is the atlas.hcl text before the env, {dir} standing for the
		// project directory.
		data string
		// src is the env's `src` expression.
		src string
		// files are written into the project directory.
		files map[string]string
		// hashDirs are migration directories whose atlas.sum is written.
		hashDirs []string
		// seedDBs are SQLite databases, by file name, and the DDL each holds.
		seedDBs map[string]string
		// table is the table the desired schema creates.
		table string
	}{
		{
			name:  "a local schema file",
			src:   `"file://schema.sql"`,
			files: map[string]string{"schema.sql": "CREATE TABLE file_t (id INTEGER PRIMARY KEY);\n"},
			table: "file_t",
		},
		{
			name:  "a schema directory",
			src:   `"file://schema"`,
			files: map[string]string{"schema/a.sql": "CREATE TABLE dir_t (id INTEGER PRIMARY KEY);\n"},
			table: "dir_t",
		},
		{
			name:     "a migration directory",
			src:      `"file://migrations"`,
			files:    map[string]string{"migrations/20260101000000_init.sql": "CREATE TABLE migrated_t (id INTEGER PRIMARY KEY);\n"},
			hashDirs: []string{"migrations"},
			table:    "migrated_t",
		},
		{
			name:    "a database URL",
			src:     `"sqlite://{dir}/desired.db"`,
			seedDBs: map[string]string{"desired.db": "CREATE TABLE live_t (id INTEGER PRIMARY KEY)"},
			table:   "live_t",
		},
		{
			name: "data.hcl_schema",
			data: `data "hcl_schema" "app" {
  path = "schema.hcl"
}
`,
			src: `data.hcl_schema.app.url`,
			files: map[string]string{"schema.hcl": `schema "main" {}

table "hcl_t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`},
			table: "hcl_t",
		},
		{
			name: "data.remote_schema",
			data: `data "remote_schema" "app" {
  name = "app"
  tag  = "prod"
}
`,
			src:   `data.remote_schema.app.url`,
			table: "users",
		},
		{
			name:  "an atlas:// registry reference",
			src:   `"atlas://app?tag=prod"`,
			table: "users",
		},
		{
			name: "data.composite_schema",
			data: `data "composite_schema" "app" {
  schema "main" {
    url = "file://part.sql"
  }
}
`,
			src:   `data.composite_schema.app.url`,
			files: map[string]string{"part.sql": "CREATE TABLE composed_t (id INTEGER PRIMARY KEY);\n"},
			table: "composed_t",
		},
		{
			name: "data.external_schema",
			data: `data "external_schema" "app" {
  program = [` + strconv.Quote(os.Args[0]) + `, "-test.run=TestExternalSchemaHelperProcess"]
  env     = ["GO_WANT_ATLAS_EXTERNAL_HELPER=1", "ATLAS_EXTERNAL_HELPER_MODE=sql", "GORACE=atexit_sleep_ms=0"]
}
`,
			src:   `data.external_schema.app.url`,
			table: "ext_users",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ALLOW_EXTERNAL_SCHEMA", "1")
			t.Setenv(atlasregistry.PlainHTTP.Name(), "1")
			host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", registryUsers())
			t.Setenv(atlasregistry.NamespaceEnvVar, host+"/acme")
			dir := t.TempDir()
			for name, content := range test.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
			}
			for _, hashDir := range test.hashDirs {
				_, err := migratesum.WriteWithFormat(filepath.Join(dir, hashDir), migrationfile.DirFormatAtlas)
				c.Assert(err, qt.IsNil)
			}
			for name, ddl := range test.seedDBs {
				atlastest.SeedSQLiteDBAt(t, filepath.Join(dir, name), ddl)
			}
			target := filepath.Join(dir, "target.db")
			empty := filepath.Join(dir, "empty.db")
			atlastest.SeedSQLiteDBAt(t, empty, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
			config := filepath.Join(dir, "atlas.hcl")
			c.Assert(os.WriteFile(config, []byte(strings.ReplaceAll(test.data+`
env "dev" {
  url = "`+atlasurl.SQLiteURLFromPath(target)+`"
  dev = "sqlite://dev?mode=memory"
  src = `+test.src+`
}
`, "{dir}", filepath.ToSlash(dir))), 0o600), qt.IsNil)
			project := []string{"--config", "file://" + filepath.ToSlash(config), "--env", "dev"}

			diff, diffErr := atlastest.RunCompatCommand(t,
				append([]string{"schema", "diff", "--from", atlasurl.SQLiteURLFromPath(empty)}, project...)...)
			_, applyErr := atlastest.RunCompatCommand(t,
				append([]string{"schema", "apply", "--auto-approve"}, project...)...)
			applied, inspectErr := atlastest.RunCompatCommand(t,
				"schema", "inspect", "--url", atlasurl.SQLiteURLFromPath(target))

			c.Assert(diffErr, qt.IsNil, qt.Commentf("%s", diff))
			c.Assert(diff, qt.Contains, test.table)
			c.Assert(applyErr, qt.IsNil)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Contains, `table "`+test.table+`"`)
		})
	}
}

// TestSchemaVerbsWithoutToRefuseARegistryStateWhereOnlyFilesAreRead names the
// registry data source where a verb reads local schema files only, rather
// than calling the env's source a local file it could not resolve.
func TestSchemaVerbsWithoutToRefuseARegistryStateWhereOnlyFilesAreRead(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "schema plan",
			args:    []string{"schema", "plan"},
			wantErr: `.*does not support atlas\.hcl data\.remote_schema desired state yet; pass --to explicitly`,
		},
		{
			name:    "schema test",
			args:    []string{"schema", "test"},
			wantErr: `atlas schema test does not support atlas\.hcl data\.remote_schema desired state yet; pass --url explicitly`,
		},
		{
			name: "schema apply with a saved plan",
			args: []string{"schema", "apply", "--plan", "file://plan.json", "--auto-approve"},
			wantErr: `schema apply --plan does not support atlas\.hcl data\.remote_schema desired state yet; ` +
				`pass --to explicitly`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasregistry.NamespaceEnvVar, "registry.invalid/acme")
			dir := t.TempDir()
			config := filepath.Join(dir, "atlas.hcl")
			c.Assert(os.WriteFile(config, []byte(`data "remote_schema" "app" {
  name = "app"
}

env "dev" {
  url = "`+atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))+`"
  dev = "sqlite://dev?mode=memory"
  src = data.remote_schema.app.url
}
`), 0o600), qt.IsNil)

			out, err := atlastest.RunCompatCommand(t,
				append(test.args, "--config", "file://"+filepath.ToSlash(config), "--env", "dev")...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
		})
	}
}

// TestSchemaTestWithoutURLReadsADatabaseEnvSource keeps the verb's one
// non-file source: `schema test` reads a database URL `src` as its --url,
// ahead of the refusal every other non-file state meets.
func TestSchemaTestWithoutURLReadsADatabaseEnvSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	desired := filepath.Join(dir, "desired.db")
	atlastest.SeedSQLiteDBAt(t, desired, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	testsDir := filepath.Join(dir, "tests")
	c.Assert(os.MkdirAll(testsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users schema works\n"+
			"    steps:\n"+
			"      - exec: INSERT INTO users (id, name) VALUES (1, 'ada')\n"+
			"      - assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)
	config := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(config, []byte(`env "dev" {
  dev = "`+atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db"))+`"
  src = "`+atlasurl.SQLiteURLFromPath(desired)+`"
}
`), 0o600), qt.IsNil)

	out, err := atlastest.RunCompatCommand(t,
		"schema", "test", testsDir, "--config", "file://"+filepath.ToSlash(config), "--env", "dev")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

package atlassource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlasregistry"
	"ptah.run/internal/atlassource"
)

// TestEnvSourceIsLocal pins the spellings that read as a local path and the
// ones that do not.
func TestEnvSourceIsLocal(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "a relative path", value: "schema.sql", want: true},
		{name: "a path with a drive letter", value: `C:\project\schema.sql`, want: true},
		{name: "a file URL", value: "file://schema.sql", want: true},
		{name: "a file URL with a query", value: "file://schema.sql?format=sql", want: true},
		{name: "surrounding space", value: "  file://schema.sql ", want: true},
		{name: "a separator only inside the query", value: "schema.sql?from=a://b", want: true},
		{name: "a database URL", value: "postgres://localhost:5432/app", want: false},
		{name: "a registry reference", value: "atlas://app?tag=prod", want: false},
		{name: "a nested env reference", value: "env://src", want: false},
		{name: "a docker URL", value: "docker://postgres/17/dev", want: false},
		{name: "a remote schema marker", value: projectconfig.RemoteSchemaMarkerScheme + "://oci://r/acme/app:prod", want: false},
		{name: "a composite schema marker", value: projectconfig.CompositeSchemaMarkerScheme + "://app", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlassource.EnvSourceIsLocal(test.value), qt.Equals, test.want)
		})
	}
}

// TestEnvSourceIsLocal_AgreesWithEnvSrcExpansion walks the kinds an env's
// `src` can name, expands each through env://src, and holds the predicate to
// the expansion's own answer: a value it calls local is a local file or a
// migration directory, and every other kind is one it does not. A command that
// takes an env's sources for an omitted --to routes on the predicate, so a
// kind where the two disagree is a kind that command reads wrongly.
func TestEnvSourceIsLocal_AgreesWithEnvSrcExpansion(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		src      string
		files    map[string]string
		wantKind atlassource.Kind
	}{
		{
			name:     "a local schema file",
			src:      `"file://schema.sql"`,
			files:    map[string]string{"schema.sql": "CREATE TABLE t (id int);\n"},
			wantKind: atlassource.KindLocalFile,
		},
		{
			name:     "a migration directory",
			src:      `"migrations"`,
			files:    map[string]string{"migrations/atlas.sum": "h1:x=\n"},
			wantKind: atlassource.KindMigrationDir,
		},
		{
			name:     "a database URL",
			src:      `"postgres://localhost:5432/app"`,
			wantKind: atlassource.KindDatabase,
		},
		{
			name:     "a registry reference",
			src:      `"atlas://app?tag=prod"`,
			wantKind: atlassource.KindRemoteSchema,
		},
		{
			name: "data.remote_schema",
			data: `data "remote_schema" "app" {
  name = "app"
}
`,
			src:      `data.remote_schema.app.url`,
			wantKind: atlassource.KindRemoteSchema,
		},
		{
			name: "data.composite_schema",
			data: `data "composite_schema" "app" {
  schema "public" {
    url = "file://schema.sql"
  }
}
`,
			src:      `data.composite_schema.app.url`,
			files:    map[string]string{"schema.sql": "CREATE TABLE t (id int);\n"},
			wantKind: atlassource.KindCompositeSchema,
		},
		{
			name: "data.hcl_schema",
			data: `data "hcl_schema" "app" {
  path = "schema.hcl"
}
`,
			src:      `data.hcl_schema.app.url`,
			files:    map[string]string{"schema.hcl": "schema \"public\" {}\n"},
			wantKind: atlassource.KindLocalFile,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(atlasregistry.NamespaceEnvVar, "registry.invalid/acme")
			dir := c.TempDir()
			for name, content := range test.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
			}
			raw := test.data + "\nenv \"dev\" {\n  src = " + test.src + "\n}\n"
			configPath := filepath.Join(dir, "atlas.hcl")
			c.Assert(os.WriteFile(configPath, []byte(raw), 0o600), qt.IsNil)
			cfg, err := projectconfig.ParseAtlas([]byte(raw), configPath, "dev")
			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaSources, qt.HasLen, 1)

			set, err := atlassource.ClassifySet("--to", []string{"env://src"},
				atlassource.ProjectEnv{Loaded: true, Config: cfg, BaseDir: dir})

			c.Assert(err, qt.IsNil)
			c.Assert(set.Kind, qt.Equals, test.wantKind)
			c.Assert(atlassource.EnvSourceIsLocal(cfg.SchemaSources[0]), qt.Equals,
				set.Kind == atlassource.KindLocalFile || set.Kind == atlassource.KindMigrationDir)
		})
	}
}

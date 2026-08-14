//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

const strictCompatGateSuffix = ` is unavailable while PTAH_ATLAS_STRICT_COMPAT is enabled.

Unset PTAH_ATLAS_STRICT_COMPAT to use Ptah's full compatibility surface.

`

func TestStrictCompatProcessUsesPtahGateDiagnostics(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	tests := []struct {
		name       string
		args       []string
		wantStdout string
		wantStderr string
		wantCode   int
	}{
		{
			name:       "schema plan help",
			args:       []string{"schema", "plan", "--help"},
			wantStdout: "'ptah-compat schema plan'" + strictCompatGateSuffix,
		},
		{
			name:       "schema plan execution",
			args:       []string{"schema", "plan"},
			wantStderr: "Abort: 'ptah-compat schema plan'" + strictCompatGateSuffix,
			wantCode:   1,
		},
		{
			name:       "schema apply plan execution",
			args:       []string{"schema", "apply", "--plan", "file://missing.plan"},
			wantStderr: "Abort: 'ptah-compat schema apply --plan'" + strictCompatGateSuffix,
			wantCode:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				test.args...,
			)

			c.Assert(code, qt.Equals, test.wantCode)
			c.Assert(stdout, qt.Equals, test.wantStdout)
			c.Assert(stderr, qt.Equals, test.wantStderr)
		})
	}

	t.Run("migrate diff desired snapshot uses target dialect", func(t *testing.T) {
		c := qt.New(t)
		fixture := newStrictMigrationPreflightFixture(t)
		c.Assert(os.WriteFile(filepath.Join(fixture.desired, "1_init.sql"), []byte(
			"SELECT 'prefix \\'\n-- +ptah no_transaction=maybe\n;\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(fixture.desired, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"migrate", "diff", "next",
			"--dir", "file://"+fixture.current,
			"--to", "file://"+fixture.desired,
			"--dev-url", "sqlite://"+fixture.dev,
		)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"Error: load --to schema: Atlas Community Edition strict compatibility does not support "+
				"Ptah migration directives in 1_init.sql\n")
		assertPathsDoNotExist(t,
			fixture.dev,
			filepath.Join(fixture.root, ".current.ptah-migrate-diff.lock"),
		)
	})
}

func TestFullCompatProcessRetainsExtensionsByDefault(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")

	tests := []struct {
		name string
		env  []string
	}{
		{name: "selector absent"},
		{name: "selector false", env: []string{"PTAH_ATLAS_STRICT_COMPAT=false"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, code := runAtlasBinary(compat, test.env, "schema", "plan", "--help")

			c.Assert(code, qt.Equals, 0)
			c.Assert(stdout, qt.Contains, "Atlas OSS `atlas schema plan` command path.")
			c.Assert(stdout, qt.Not(qt.Contains), "not supported by the community version")
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

func TestStrictCompatProcessRejectsInvalidPolicyBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	tests := [][]string{{"--help"}, {"version"}, {"unknown-command"}}

	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT="},
				args...,
			)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				`Error: invalid boolean value "" for PTAH_ATLAS_STRICT_COMPAT`+"\n")
		})
	}
}

func TestStrictCompatProcessRejectsExtensionEnvironmentBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	for _, assignment := range []string{
		"PTAH_ATLAS_INSPECT_ALL_BLOCKS=1",
		"PTAH_MIGRATIONS_DIR=/must-not-be-read",
		"PTAH_URL=sqlite://must-not-be-ignored",
	} {
		t.Run(strings.SplitN(assignment, "=", 2)[0], func(t *testing.T) {
			c := qt.New(t)
			name := strings.SplitN(assignment, "=", 2)[0]
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{
					"PTAH_ATLAS_STRICT_COMPAT=1",
					assignment,
				},
				"version",
			)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: PTAH_ATLAS_STRICT_COMPAT does not allow "+name+"\n")
		})
	}
}

func TestStrictCompatProcessValidatesRetainedEnvironmentBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{
			"PTAH_ATLAS_STRICT_COMPAT=1",
			"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=maybe",
		},
		"version",
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: invalid boolean value "maybe" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`+"\n")
}

func TestNativeProcessIgnoresStrictCompatEnvironment(t *testing.T) {
	c := qt.New(t)
	native := buildSchemaInspectBinary(c, "ptah", "go.5x5.cz/ptah/cmd/ptah")

	stdout, stderr, code := runAtlasBinary(
		native,
		[]string{"PTAH_ATLAS_STRICT_COMPAT="},
		"version",
	)

	c.Assert(code, qt.Equals, 0)
	c.Assert(stdout, qt.Contains, "Version:")
	c.Assert(stderr, qt.Equals, "")
}

func TestStrictCompatRefusesProDesiredObjectsWhileFullModeRetainsThem(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	fromPath := filepath.Join(dir, "from.sql")
	toPath := filepath.Join(dir, "to.sql")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(fromPath, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
			"CREATE VIEW active_users AS SELECT id FROM users;\n",
	), 0o600), qt.IsNil)
	args := []string{
		"schema", "diff",
		"--from", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + devPath,
	}

	stdout, stderr, code := runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "CREATE VIEW")
	c.Assert(stderr, qt.Equals, "")

	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: load --to schema: Atlas Community Edition strict compatibility does not support desired schema views\n")
}

func TestStrictCompatRefusesProInspectTemplateFunctionsBeforeSourceWork(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	for _, function := range []string{"hcl", "split", "write"} {
		t.Run(function, func(t *testing.T) {
			c := qt.New(t)
			format := `{{ sql . | ` + function + ` }}`
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				"schema", "inspect",
				"--url", "unknown://source-must-not-be-resolved",
				"--format", format,
			)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				`Error: Atlas Community Edition strict compatibility does not support schema inspect template function "`+
					function+"\"\n")
		})
	}
}

func TestStrictCompatRefusesAuthoredInspectExtensionsBeforeDevReset(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
			"CREATE VIEW active_users AS SELECT id FROM users;\n",
	), 0o600), qt.IsNil)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "inspect",
		"--url", "file://"+schemaPath,
		"--dev-url", "sqlite://"+devPath,
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support desired schema views\n")
	_, statErr := os.Stat(devPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestStrictCompatRefusesUnknownAuthoredInspectHCL(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.hcl")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(schemaPath, []byte("wibble \"written\" {}\n"), 0o600), qt.IsNil)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "inspect",
		"--url", "file://"+schemaPath,
		"--dev-url", "sqlite://"+devPath,
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `unsupported top-level block "wibble"`)
	_, statErr := os.Stat(devPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestStrictCompatRefusesYAMLSchemaWhileFullModeRetainsIt(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "schema.yaml")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(schemaPath, []byte(`tables:
  users:
    columns:
      id: {type: INTEGER, primary: true}
`), 0o600), qt.IsNil)
	args := []string{
		"schema", "inspect",
		"--url", "file://" + schemaPath,
		"--dev-url", "sqlite://" + devPath,
		"--format", "{{ sql . }}",
	}

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support YAML schema source "schema.yaml"`+"\n")
	_, statErr := os.Stat(devPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "CREATE TABLE")
	c.Assert(stdout, qt.Contains, "users")
	c.Assert(stderr, qt.Equals, "")
}

func TestStrictCompatRefusesIgnoredProjectConfigConstructs(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	configPath := filepath.Join(c.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(`env "local" {
  url = "sqlite://file?mode=memory&_fk=1"
  pro_option = "written"
}
`), 0o600), qt.IsNil)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "inspect",
		"--config", "file://"+configPath,
		"--env", "local",
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains,
		`Atlas Community Edition strict compatibility refuses ignored atlas.hcl attribute "pro_option"`)
}

func TestStrictCompatMatchesCommunityDynamicEnvTypeBoundary(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	configPath := filepath.Join(dir, "atlas.hcl")
	databasePath := filepath.Join(dir, "database.db")
	c.Assert(os.WriteFile(configPath, []byte(`variable "targets" {
  type    = list(string)
  default = ["sqlite://`+databasePath+`"]
}

env {
  for_each = var.targets
  name     = atlas.env
  url      = each.value
}
`), 0o600), qt.IsNil)
	args := []string{
		"schema", "inspect",
		"--config", "file://" + configPath,
		"--env", "local",
	}

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: schemahcl: for_each does not support list of string type\n")
	_, statErr := os.Stat(databasePath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Equals, "schema \"main\" {\n}\n")
	c.Assert(stderr, qt.Equals, "")
}

func TestStrictCompatValidatesEveryDynamicEnvironmentBeforeApply(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	configPath := filepath.Join(dir, "atlas.hcl")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "20240101000000_init.sql"), []byte(
		"CREATE TABLE users (id integer PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(configPath, []byte(`env {
  for_each = ["first", "second"]
  name     = atlas.env
  url      = "sqlite://file?mode=memory&_fk=1"

  schema {
    src = [each.key == 0 ? "file://schema.hcl" : "file://schema.yaml"]
  }
  migration {
    dir = "file://`+migrationsDir+`"
  }
}
`), 0o600), qt.IsNil)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"migrate", "apply", "--dry-run",
		"--config", "file://"+configPath,
		"--env", "local",
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support YAML schema source "schema.yaml"`+"\n")
}

func TestStrictCompatRefusesExtendedMigrationContentBeforeDatabaseWork(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	tests := []struct {
		name       string
		migration  string
		wantStderr string
	}{
		{
			name: "Atlas txtar",
			migration: "-- atlas:txtar\n\n-- migration.sql --\n" +
				"CREATE TABLE users (id integer);\n",
			wantStderr: "Error: Atlas Community Edition strict compatibility does not support " +
				"Atlas txtar migration 20260811000001_users.sql\n",
		},
		{
			name: "Ptah pre-migration check",
			migration: `-- +ptah check name="ready" assert="SELECT 1"` + "\n" +
				"CREATE TABLE users (id integer);\n",
			wantStderr: "Error: Atlas Community Edition strict compatibility does not support " +
				"Ptah pre-migration checks in 20260811000001_users.sql\n",
		},
		{
			name: "Ptah file directive",
			migration: "-- +ptah no_transaction\n" +
				"CREATE TABLE users (id integer);\n",
			wantStderr: "Error: Atlas Community Edition strict compatibility does not support " +
				"Ptah migration directives in 20260811000001_users.sql\n",
		},
		{
			name: "Atlas SQL template",
			migration: "{{ if eq .Env \"\" }}\n" +
				"CREATE TABLE users (id integer);\n{{ end }}\n",
			wantStderr: "Error: Atlas Community Edition strict compatibility does not support " +
				"SQL template migration 20260811000001_users.sql\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := c.TempDir()
			migrationPath := filepath.Join(dir, "20260811000001_users.sql")
			databasePath := filepath.Join(dir, "strict.db")
			c.Assert(os.WriteFile(migrationPath, []byte(test.migration), 0o600), qt.IsNil)

			stdout, stderr, code := runAtlasBinary(
				compat,
				nil,
				"migrate", "hash", "--dir", "file://"+dir,
			)
			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))

			args := []string{
				"migrate", "apply",
				"--url", "sqlite://" + databasePath,
				"--dir", "file://" + dir,
			}
			stdout, stderr, code = runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				args...,
			)
			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.wantStderr)
			_, statErr := os.Stat(databasePath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)

			stdout, stderr, code = runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				"migrate", "lint",
				"--dir", "file://"+dir,
				"--dev-url", "sqlite://"+filepath.Join(dir, "lint-dev.db"),
				"--latest", "1",
			)
			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.wantStderr)

			validateDBPath := filepath.Join(dir, "validate-dev.db")
			stdout, stderr, code = runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				"migrate", "validate",
				"--dir", "file://"+dir,
				"--dev-url", "sqlite://"+validateDBPath,
			)
			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.wantStderr)
			_, statErr = os.Stat(validateDBPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)

			stdout, stderr, code = runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				"migrate", "validate",
				"--dir", "file://"+dir,
			)
			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")

			stdout, stderr, code = runAtlasBinary(compat, nil, args...)
			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(stdout, qt.Contains, "Migration complete.")
			c.Assert(stderr, qt.Equals, "")
			_, statErr = os.Stat(databasePath)
			c.Assert(statErr, qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+databasePath)
			c.Assert(err, qt.IsNil)
			rows, err := conn.QueryContext(t.Context(),
				"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'")
			c.Assert(err, qt.IsNil)
			c.Assert(rows.Next(), qt.IsTrue)
			var tableCount int
			c.Assert(rows.Scan(&tableCount), qt.IsNil)
			c.Assert(tableCount, qt.Equals, 1)
			c.Assert(rows.Close(), qt.IsNil)
			dbschema.CloseAndWarn(conn)
		})
	}
}

func TestStrictCompatPreflightsSourcesBeforeDatabaseAndLockArtifacts(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	strictEnv := []string{"PTAH_ATLAS_STRICT_COMPAT=1"}

	t.Run("schema apply YAML before target connection", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		targetPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "dev.db")
		stdout, stderr, code := runAtlasBinary(
			compat,
			strictEnv,
			"schema", "apply",
			"--url", "sqlite://"+targetPath,
			"--to", "file://"+filepath.Join(dir, "missing.yaml"),
			"--dev-url", "sqlite://"+devPath,
			"--auto-approve",
		)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			`Error: load --to schema: Atlas Community Edition strict compatibility does not support YAML schema source "missing.yaml"`+"\n")
		assertPathsDoNotExist(t, targetPath, devPath)
	})

	t.Run("schema diff YAML before database-backed from", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		fromPath := filepath.Join(dir, "from.db")
		devPath := filepath.Join(dir, "dev.db")
		stdout, stderr, code := runAtlasBinary(
			compat,
			strictEnv,
			"schema", "diff",
			"--from", "sqlite://"+fromPath,
			"--to", "file://"+filepath.Join(dir, "missing.yaml"),
			"--dev-url", "sqlite://"+devPath,
		)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			`Error: load --to schema: Atlas Community Edition strict compatibility does not support YAML schema source "missing.yaml"`+"\n")
		assertPathsDoNotExist(t, fromPath, devPath)
	})

	t.Run("migrate diff desired YAML before dev connection and directory lock", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.Mkdir(migrationsDir, 0o700), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(
			"CREATE TABLE users (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)
		devPath := filepath.Join(dir, "dev.db")
		stdout, stderr, code := runAtlasBinary(
			compat,
			strictEnv,
			"migrate", "diff", "next",
			"--dir", "file://"+migrationsDir,
			"--to", "file://"+filepath.Join(dir, "missing.yaml"),
			"--dev-url", "sqlite://"+devPath,
		)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			`Error: load --to schema: Atlas Community Edition strict compatibility does not support YAML schema source "missing.yaml"`+"\n")
		assertPathsDoNotExist(t,
			devPath,
			filepath.Join(dir, ".migrations.ptah-migrate-diff.lock"),
		)
	})

	t.Run("migrate diff current directory before dev connection and directory lock", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.Mkdir(migrationsDir, 0o700), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(
			"-- +ptah no_transaction\nCREATE TABLE users (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)
		desiredPath := filepath.Join(dir, "desired.sql")
		c.Assert(os.WriteFile(desiredPath, []byte(
			"CREATE TABLE users (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		devPath := filepath.Join(dir, "dev.db")
		stdout, stderr, code := runAtlasBinary(
			compat,
			strictEnv,
			"migrate", "diff", "next",
			"--dir", "file://"+migrationsDir,
			"--to", "file://"+desiredPath,
			"--dev-url", "sqlite://"+devPath,
		)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"Error: validate current migration directory: Atlas Community Edition strict compatibility does not support "+
				"Ptah migration directives in 1_init.sql\n")
		assertPathsDoNotExist(t,
			devPath,
			filepath.Join(dir, ".migrations.ptah-migrate-diff.lock"),
		)
	})
}

func TestStrictCompatPreflightsMigrationDesiredSourcesBeforeWork(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	tests := []struct {
		name string
		args func(strictMigrationPreflightFixture) []string
		gone func(strictMigrationPreflightFixture) []string
	}{
		{
			name: "schema apply before target connection and lock",
			args: func(f strictMigrationPreflightFixture) []string {
				return []string{
					"schema", "apply", "--url", "sqlite://" + f.target,
					"--to", "file://" + f.desired, "--dev-url", "sqlite://" + f.dev,
					"--auto-approve",
				}
			},
			gone: func(f strictMigrationPreflightFixture) []string {
				return []string{f.target, f.dev}
			},
		},
		{
			name: "schema diff before database-backed from",
			args: func(f strictMigrationPreflightFixture) []string {
				return []string{
					"schema", "diff", "--from", "sqlite://" + f.from,
					"--to", "file://" + f.desired, "--dev-url", "sqlite://" + f.dev,
				}
			},
			gone: func(f strictMigrationPreflightFixture) []string {
				return []string{f.from, f.dev}
			},
		},
		{
			name: "migrate diff before dev connection and directory lock",
			args: func(f strictMigrationPreflightFixture) []string {
				return []string{
					"migrate", "diff", "next", "--dir", "file://" + f.current,
					"--to", "file://" + f.desired, "--dev-url", "sqlite://" + f.dev,
				}
			},
			gone: func(f strictMigrationPreflightFixture) []string {
				return []string{f.dev, filepath.Join(f.root, ".current.ptah-migrate-diff.lock")}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newStrictMigrationPreflightFixture(t)
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				test.args(fixture)...,
			)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: load --to schema: Atlas Community Edition strict compatibility does not support "+
					"Ptah migration directives in 1_init.sql\n")
			assertPathsDoNotExist(t, test.gone(fixture)...)
		})
	}
}

func TestFullCompatDoesNotPreflightMigrationDesiredSources(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")

	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "apply",
		"--url", "sqlite://"+targetPath,
		"--to", "file://"+filepath.Join(dir, "missing"),
		"--tx-mode", "statement",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: invalid tx-mode \"statement\": expected file, all, or none\n")
	_, err := os.Stat(targetPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

type strictMigrationPreflightFixture struct {
	root    string
	current string
	desired string
	dev     string
	from    string
	target  string
}

func newStrictMigrationPreflightFixture(t *testing.T) strictMigrationPreflightFixture {
	c := qt.New(t)
	t.Helper()
	root := t.TempDir()
	desired := filepath.Join(root, "desired")
	current := filepath.Join(root, "current")
	c.Assert(os.Mkdir(desired, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(current, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(desired, "1_init.sql"), []byte(
		"-- +ptah\nCREATE TABLE users (id integer PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(current, "1_init.sql"), []byte(
		"CREATE TABLE users (id integer PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(desired, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	_, err = migratesum.WriteWithFormat(current, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return strictMigrationPreflightFixture{
		root: root, current: current, desired: desired,
		dev: filepath.Join(root, "dev.db"), from: filepath.Join(root, "from.db"),
		target: filepath.Join(root, "target.db"),
	}
}

func assertPathsDoNotExist(t *testing.T, paths ...string) {
	c := qt.New(t)
	t.Helper()
	for _, path := range paths {
		_, err := os.Stat(path)
		c.Assert(err, qt.ErrorIs, os.ErrNotExist, qt.Commentf("path %s", path))
	}
}

func TestStrictCompatRefusesExtendedMigrationContentBeforeImportWrites(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	sourceDir := filepath.Join(dir, "source")
	destinationDir := filepath.Join(dir, "destination")
	c.Assert(os.Mkdir(sourceDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(sourceDir, "20260811000001_users.sql"), []byte(
		"-- +goose Up\n"+
			`-- +ptah check name="ready" assert="SELECT 1"`+"\n"+
			"CREATE TABLE users (id integer);\n",
	), 0o600), qt.IsNil)

	args := []string{
		"migrate", "import",
		"--from", "file://" + sourceDir,
		"--to", "file://" + destinationDir,
		"--dir-format", "goose",
	}
	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support "+
			"Ptah pre-migration checks in 20260811000001_users.sql\n")
	_, statErr := os.Stat(destinationDir)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	entries, err := os.ReadDir(destinationDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
}

func TestStrictCompatRefusesUnenforceableSchemaApplyLintPolicy(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	devPath := filepath.Join(dir, "dev.db")
	desiredPath := filepath.Join(dir, "desired.sql")
	configPath := filepath.Join(dir, "atlas.hcl")

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+targetPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(),
		"CREATE TABLE keep_me (id integer PRIMARY KEY); CREATE TABLE drop_me (id integer PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)
	c.Assert(os.WriteFile(desiredPath,
		[]byte("CREATE TABLE keep_me (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(configPath, []byte(`env "local" {
  url = "sqlite://`+targetPath+`"
  dev = "sqlite://`+devPath+`"
  schema {
    src = ["file://`+desiredPath+`"]
  }
  lint {
    destructive {
      error = true
    }
  }
}
`), 0o600), qt.IsNil)
	args := []string{
		"schema", "apply",
		"--config", "file://" + configPath,
		"--env", "local",
		"--dry-run",
	}

	stdout, stderr, code := runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "DROP TABLE")
	c.Assert(stderr, qt.Contains, "planned changes are refused by the atlas.hcl lint policy")

	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility cannot enforce "+
			"atlas.hcl lint policy during schema apply\n")
}

func TestStrictCompatAllowsAtlasProjectGetenvInputs(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	databasePath := filepath.Join(dir, "project.db")
	configPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(`env "local" {
  url = getenv("PTAH_ATLAS_PROJECT_CONFIG_E2E_URL")
}
`), 0o600), qt.IsNil)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{
			"PTAH_ATLAS_STRICT_COMPAT=1",
			"PTAH_ATLAS_PROJECT_CONFIG_E2E_URL=sqlite://" + databasePath,
		},
		"schema", "inspect",
		"--config", "file://"+configPath,
		"--env", "local",
	)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Equals, "schema \"main\" {\n}\n")
	c.Assert(stderr, qt.Equals, "")
}

func TestStrictCompatSchemaCleanRefusesProObjectsBeforeDestruction(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	databasePath := filepath.Join(c.TempDir(), "clean.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(),
		"CREATE TABLE users (id integer PRIMARY KEY); "+
			"CREATE VIEW active_users AS SELECT id FROM users;")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "clean",
		"--url", "sqlite://"+databasePath,
		"--auto-approve",
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support cleaning live schema view "active_users"`+"\n")

	conn, err = dbschema.ConnectToDatabase(context.Background(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Tables[0].Name, qt.Equals, "users")
	c.Assert(schema.Views, qt.HasLen, 1)
	c.Assert(schema.Views[0].Name, qt.Equals, "active_users")

	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "inspect",
		"--url", "sqlite://"+databasePath,
		"--exclude", "view",
		"--format", "{{ sql . }}",
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support inspected schema views\n")

	desiredPath := filepath.Join(filepath.Dir(databasePath), "desired.sql")
	devPath := filepath.Join(filepath.Dir(databasePath), "dev.db")
	c.Assert(os.WriteFile(desiredPath,
		[]byte("CREATE TABLE users (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "apply",
		"--url", "sqlite://"+databasePath,
		"--to", "file://"+desiredPath,
		"--dev-url", "sqlite://"+devPath,
		"--auto-approve",
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support inspected schema views\n")

	schema, err = conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Views, qt.HasLen, 1)
}

func TestStrictCompatSchemaCleanRefusesCollateralTriggerDeletion(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	databasePath := filepath.Join(c.TempDir(), "clean-trigger.db")
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(),
		"CREATE TABLE users (id integer PRIMARY KEY); "+
			"CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 1; END;")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)
	args := []string{
		"schema", "clean",
		"--url", "sqlite://" + databasePath,
		"--auto-approve",
	}

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support cleaning live schema triggers\n")

	conn, err = dbschema.ConnectToDatabase(t.Context(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	rows, err := conn.QueryContext(t.Context(),
		"SELECT count(*) FROM sqlite_master WHERE name IN ('users', 'users_audit')")
	c.Assert(err, qt.IsNil)
	c.Assert(rows.Next(), qt.IsTrue)
	var objectCount int
	c.Assert(rows.Scan(&objectCount), qt.IsNil)
	c.Assert(objectCount, qt.Equals, 2)
	c.Assert(rows.Close(), qt.IsNil)
	dbschema.CloseAndWarn(conn)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "DROP TABLE")
	c.Assert(stderr, qt.Equals, "")

	conn, err = dbschema.ConnectToDatabase(t.Context(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	rows, err = conn.QueryContext(t.Context(),
		"SELECT count(*) FROM sqlite_master WHERE name IN ('users', 'users_audit')")
	c.Assert(err, qt.IsNil)
	c.Assert(rows.Next(), qt.IsTrue)
	c.Assert(rows.Scan(&objectCount), qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)
	c.Assert(rows.Close(), qt.IsNil)
	dbschema.CloseAndWarn(conn)
}

func TestStrictCompatSchemaInspectAndCleanRefusePostgresWriterOnlyObjects(t *testing.T) {
	c := qt.New(t)
	dbURL := strictCompatPostgresTestURL(t)

	schemaName := fmt.Sprintf("ptah_strict_clean_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	admin, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schemaName)
	c.Assert(err, qt.IsNil)

	scopedURL := postgresURLWithSearchPath(t, dbURL, schemaName)
	conn, err := dbschema.ConnectToDatabase(t.Context(), scopedURL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(),
		"CREATE PROCEDURE refresh_users() LANGUAGE SQL AS $$ SELECT 1 $$;")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	inspectArgs := []string{"schema", "inspect", "--url", scopedURL, "--format", "{{ json . }}"}
	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		inspectArgs...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support inspecting live schema procedure "refresh_users()"`+"\n")
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 1)

	stdout, stderr, code = runAtlasBinary(compat, nil, inspectArgs...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Not(qt.Equals), "")
	c.Assert(stderr, qt.Not(qt.Contains), "Error:")
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 1)

	conn, err = dbschema.ConnectToDatabase(t.Context(), scopedURL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE users (id SERIAL PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 3)

	args := []string{"schema", "clean", "--url", scopedURL, "--auto-approve"}
	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support cleaning live schema procedure "refresh_users()"`+"\n")
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 3)

	sequenceArgs := append(slices.Clone(args), "--include", "users_id_seq[type=sequence]")
	stdout, stderr, code = runAtlasBinary(compat, nil, sequenceArgs...)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, fmt.Sprintf(
		`owned sequence %q cannot be selected independently; select its owning table %q`,
		schemaName+".users_id_seq", schemaName+".users",
	))
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 3)

	excludeSequenceArgs := append(slices.Clone(args), "--exclude", "users_id_seq[type=sequence]")
	stdout, stderr, code = runAtlasBinary(compat, nil, excludeSequenceArgs...)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, fmt.Sprintf(
		`owned sequence %q cannot be excluded while its owning table %q is selected; exclude the table instead`,
		schemaName+".users_id_seq", schemaName+".users",
	))
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 3)

	tableArgs := append(slices.Clone(args), "--include", "users[type=table]")
	stdout, stderr, code = runAtlasBinary(compat, nil, tableArgs...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Schema clean completed successfully.")
	c.Assert(stderr, qt.Equals, "")
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 1)

	procedureArgs := append(slices.Clone(args), "--include", "refresh_users[type=procedure]")
	stdout, stderr, code = runAtlasBinary(compat, nil, procedureArgs...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Schema clean completed successfully.")
	c.Assert(stderr, qt.Equals, "")
	c.Assert(postgresStrictCleanObjectCount(t, scopedURL), qt.Equals, 0)
}

func TestStrictCompatSchemaInspectOmitsPostgresSystemBaselines(t *testing.T) {
	c := qt.New(t)
	dbURL := createDisposableDatabase(
		c,
		strictCompatPostgresTestURL(t),
		fmt.Sprintf("ptah_strict_baseline_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000),
	)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	args := []string{"schema", "inspect", "--url", dbURL}

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Not(qt.Contains), "plpgsql")
	c.Assert(stdout, qt.Not(qt.Contains), "PUBLIC")
	c.Assert(stderr, qt.Equals, "")

	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_INSPECT_ALL_BLOCKS=1"},
		args...,
	)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, "plpgsql")
	c.Assert(stdout, qt.Contains, "PUBLIC")
	c.Assert(stderr, qt.Not(qt.Contains), "Error:")
}

func TestStrictCompatSchemaCleanRevalidatesConfirmedSnapshotUnderRelationLock(t *testing.T) {
	c := qt.New(t)
	dbURL := createDisposableDatabase(
		c,
		strictCompatPostgresTestURL(t),
		fmt.Sprintf("ptah_strict_clean_snapshot_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000),
	)
	seedDatabase(c, dbURL, "CREATE TABLE users (id integer PRIMARY KEY)")
	commandURL, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := commandURL.Query()
	query.Set("lock_timeout", "5s")
	query.Set("statement_timeout", "10s")
	commandURL.RawQuery = query.Encode()
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdoutPath := filepath.Join(c.TempDir(), "stdout.txt")
	stdoutFile, err := os.Create(stdoutPath)
	c.Assert(err, qt.IsNil)
	defer func() { _ = stdoutFile.Close() }()
	stdinReader, stdinWriter := io.Pipe()
	defer func() { _ = stdinReader.Close() }()
	defer func() { _ = stdinWriter.Close() }()

	commandContext, cancelCommand := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancelCommand()
	var stderr bytes.Buffer
	command := exec.CommandContext(commandContext, compat, "schema", "clean", "--url", commandURL.String())
	command.Env = append(environmentWithoutPtahVariables(), "PTAH_ATLAS_STRICT_COMPAT=1")
	command.Stdin = stdinReader
	command.Stdout = stdoutFile
	command.Stderr = &stderr
	c.Assert(command.Start(), qt.IsNil)
	waitForFileContains(t, stdoutPath, "Type 'DELETE EVERYTHING'")
	seedDatabase(c, dbURL, "CREATE VIEW late_view AS SELECT id FROM users")
	_, err = io.WriteString(stdinWriter, "DELETE EVERYTHING\nYES I AM SURE\n")
	c.Assert(err, qt.IsNil)
	// os/exec waits for its stdin-copy goroutine. Close the pipe after the two
	// answers so Wait can return even when the child has already exited.
	c.Assert(stdinWriter.Close(), qt.IsNil)
	waitErr := command.Wait()
	c.Assert(commandContext.Err(), qt.IsNil, qt.Commentf("stderr=%q", stderr.String()))
	c.Assert(waitErr, qt.ErrorMatches, "exit status 1")
	c.Assert(stdoutFile.Close(), qt.IsNil)
	c.Assert(stderr.String(), qt.Matches,
		`(?s).*strict compatibility does not support cleaning live schema view "late_view".*`)
	c.Assert(postgresStrictCleanObjectCount(t, dbURL), qt.Equals, 1)
	c.Assert(postgresViewExists(t, dbURL, "late_view"), qt.IsTrue)
}

func waitForFileContains(t *testing.T, path, needle string) {
	c := qt.New(t)
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		if bytes.Contains(contents, []byte(needle)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in %s", needle, path)
}

func TestStrictCompatSchemaApplyAndDiffRefusePostgresWriterOnlyObjects(t *testing.T) {
	c := qt.New(t)
	dbURL := strictCompatPostgresTestURL(t)
	prefix := fmt.Sprintf("ptah_strict_sources_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	dbURL = createDisposableDatabase(c, dbURL, prefix+"_target")
	procedureSchema := prefix + "_procedure"
	emptySchema := prefix + "_empty"

	admin, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		for _, schema := range []string{procedureSchema, emptySchema} {
			_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
		dbschema.CloseAndWarn(admin)
	})
	for _, schema := range []string{procedureSchema, emptySchema} {
		_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema)
		c.Assert(err, qt.IsNil)
	}

	procedureURL := postgresURLWithSearchPath(t, dbURL, procedureSchema)
	emptyURL := postgresURLWithSearchPath(t, dbURL, emptySchema)
	conn, err := dbschema.ConnectToDatabase(t.Context(), procedureURL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(),
		"CREATE PROCEDURE refresh_users() LANGUAGE SQL AS $$ SELECT 1 $$;")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	wantPolicyError := `Atlas Community Edition strict compatibility does not support inspecting live schema procedure "refresh_users()"`

	t.Run("apply target before desired replay", func(t *testing.T) {
		c := qt.New(t)
		migrationDir := t.TempDir()
		replayDevURL := strictCompatPostgresDevURL(t)
		c.Assert(os.WriteFile(filepath.Join(migrationDir, "1_replayed.sql"), []byte(
			"CREATE TABLE replayed_before_target_validation (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "apply",
			"--url", procedureURL,
			"--to", "file://"+migrationDir,
			"--dev-url", replayDevURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+wantPolicyError+"\n")
		c.Assert(postgresTableExists(t, replayDevURL, "replayed_before_target_validation"), qt.IsFalse)
	})

	t.Run("apply target realm before desired schema replay", func(t *testing.T) {
		c := qt.New(t)
		migrationDir := t.TempDir()
		replayDevURL := strictCompatPostgresDevURL(t)
		tableName := prefix + "_replayed_scope"
		c.Assert(os.WriteFile(filepath.Join(migrationDir, "1_replayed.sql"), []byte(
			"CREATE SCHEMA IF NOT EXISTS "+procedureSchema+";\n"+
				"CREATE TABLE "+procedureSchema+"."+tableName+" (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "apply",
			"--url", emptyURL,
			"--to", "file://"+migrationDir,
			"--dev-url", replayDevURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+wantPolicyError+"\n")
		c.Assert(postgresSchemaTableExists(t, replayDevURL, procedureSchema, tableName), qt.IsFalse)
	})

	t.Run("apply target before lock acquisition", func(t *testing.T) {
		c := qt.New(t)
		lockConn, err := dbschema.ConnectToDatabase(t.Context(), procedureURL)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(lockConn)
		lock, err := atlasschema.AcquireApplyLock(t.Context(), lockConn, "", time.Second)
		c.Assert(err, qt.IsNil)
		defer func() {
			c.Assert(lock.Release(), qt.IsNil)
		}()

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "apply",
			"--url", procedureURL,
			"--to", emptyURL,
			"--lock-timeout", "100ms",
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+wantPolicyError+"\n")
	})

	t.Run("full apply classifies migration directory under lock", func(t *testing.T) {
		c := qt.New(t)
		migrationDir := t.TempDir()
		replayDevURL := strictCompatPostgresDevURL(t)
		devAdmin, err := dbschema.ConnectToDatabase(t.Context(), replayDevURL)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(devAdmin)
		_, err = devAdmin.ExecContext(t.Context(), "CREATE SCHEMA "+emptySchema)
		c.Assert(err, qt.IsNil)
		defer func() {
			_, _ = devAdmin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+emptySchema+" CASCADE")
		}()
		replayDevURL = postgresURLWithSearchPath(t, replayDevURL, emptySchema)
		c.Assert(os.WriteFile(filepath.Join(migrationDir, "1_late.sql"), []byte(
			"CREATE TABLE classified_under_lock (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)

		lockConn, err := dbschema.ConnectToDatabase(t.Context(), emptyURL)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(lockConn)
		lock, err := atlasschema.AcquireApplyLock(t.Context(), lockConn, "", time.Second)
		c.Assert(err, qt.IsNil)
		defer func() { _ = lock.Release() }()

		var stdoutBuffer, stderrBuffer bytes.Buffer
		command := exec.CommandContext(t.Context(), compat,
			"schema", "apply",
			"--url", emptyURL,
			"--to", "file://"+migrationDir,
			"--dev-url", replayDevURL,
			"--lock-timeout", "5s",
			"--dry-run",
		)
		command.Env = environmentWithoutPtahVariables()
		command.Stdout = &stdoutBuffer
		command.Stderr = &stderrBuffer
		c.Assert(command.Start(), qt.IsNil)

		waitForPostgresAdvisoryLockPoller(t, lockConn)
		_, err = migratesum.WriteWithFormat(migrationDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Release(), qt.IsNil)
		c.Assert(command.Wait(), qt.IsNil,
			qt.Commentf("stdout=%q stderr=%q", stdoutBuffer.String(), stderrBuffer.String()))
		c.Assert(stdoutBuffer.String(), qt.Contains, "CREATE TABLE")
		c.Assert(stderrBuffer.String(), qt.Equals, "")
	})

	t.Run("apply target", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "apply",
			"--url", procedureURL,
			"--to", emptyURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: "+wantPolicyError+"\n")
		c.Assert(postgresStrictCleanObjectCount(t, procedureURL), qt.Equals, 1)

		stdout, stderr, code = runAtlasBinary(
			compat,
			nil,
			"schema", "apply",
			"--url", procedureURL,
			"--to", emptyURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
		c.Assert(postgresStrictCleanObjectCount(t, procedureURL), qt.Equals, 1)
	})

	t.Run("diff database from", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "diff",
			"--from", procedureURL,
			"--to", emptyURL,
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: load --from schema: "+wantPolicyError+"\n")
	})

	t.Run("diff database to", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "diff",
			"--from", emptyURL,
			"--to", procedureURL,
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: load --to schema: "+wantPolicyError+"\n")
	})

	t.Run("diff live to before from replay", func(t *testing.T) {
		c := qt.New(t)
		migrationDir := t.TempDir()
		replayDevURL := strictCompatPostgresDevURL(t)
		const tableName = "replayed_before_live_to_validation"
		c.Assert(os.WriteFile(filepath.Join(migrationDir, "1_replayed.sql"), []byte(
			"CREATE TABLE "+tableName+" (id integer PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "diff",
			"--from", "file://"+migrationDir,
			"--to", procedureURL,
			"--dev-url", replayDevURL,
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: load --to schema: "+wantPolicyError+"\n")
		c.Assert(postgresTableExists(t, replayDevURL, tableName), qt.IsFalse)
	})

	t.Run("diff replayed migration directory", func(t *testing.T) {
		c := qt.New(t)
		migrationDir := t.TempDir()
		replayDevURL := strictCompatPostgresDevURL(t)
		collationName := prefix + "_replayed"
		c.Assert(os.WriteFile(filepath.Join(migrationDir, "1_collation.sql"), []byte(
			"CREATE COLLATION "+collationName+" FROM \"C\";\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(migrationDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"schema", "diff",
			"--from", "file://"+migrationDir,
			"--to", emptyURL,
			"--dev-url", replayDevURL,
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			`Error: load --from schema: --from "file://`+migrationDir+`": `+
				`Atlas Community Edition strict compatibility does not support inspecting live schema collation "`+
				collationName+`"`+"\n")

		stdout, stderr, code = runAtlasBinary(
			compat,
			nil,
			"schema", "diff",
			"--from", "file://"+migrationDir,
			"--to", emptyURL,
			"--dev-url", replayDevURL,
		)
		c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
		c.Assert(stderr, qt.Equals, "")
	})

	t.Run("migrate diff live desired source", func(t *testing.T) {
		c := qt.New(t)
		currentDir := t.TempDir()
		migrateDevURL := strictCompatPostgresDevURL(t)
		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"migrate", "diff", "next",
			"--dir", "file://"+currentDir,
			"--to", procedureURL,
			"--dev-url", migrateDevURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: load --to schema: "+wantPolicyError+"\n")
		entries, err := os.ReadDir(currentDir)
		c.Assert(err, qt.IsNil)
		c.Assert(entries, qt.HasLen, 0)
	})

	t.Run("migrate diff replayed current source", func(t *testing.T) {
		c := qt.New(t)
		currentDir := t.TempDir()
		migrateDevURL := strictCompatPostgresDevURL(t)
		collationName := prefix + "_current"
		c.Assert(os.WriteFile(filepath.Join(currentDir, "1_collation.sql"), []byte(
			"CREATE COLLATION "+collationName+" FROM \"C\";\n",
		), 0o600), qt.IsNil)
		_, err := migratesum.WriteWithFormat(currentDir, migrator.MigrationDirFormatAtlas)
		c.Assert(err, qt.IsNil)

		stdout, stderr, code := runAtlasBinary(
			compat,
			[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			"migrate", "diff", "next",
			"--dir", "file://"+currentDir,
			"--to", emptyURL,
			"--dev-url", migrateDevURL,
			"--dry-run",
		)
		c.Assert(code, qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			`Error: Atlas Community Edition strict compatibility does not support inspecting live schema collation "`+
				collationName+`"`+"\n")
		entries, err := os.ReadDir(currentDir)
		c.Assert(err, qt.IsNil)
		c.Assert(entries, qt.HasLen, 2)
	})
}

func postgresTableExists(t *testing.T, dbURL, table string) bool {
	c := qt.New(t)
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var exists bool
	err = conn.QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`, table).Scan(&exists)
	c.Assert(err, qt.IsNil)
	return exists
}

func postgresSchemaTableExists(t *testing.T, dbURL, schema, table string) bool {
	c := qt.New(t)
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var exists bool
	err = conn.QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`, schema, table).Scan(&exists)
	c.Assert(err, qt.IsNil)
	return exists
}

func waitForPostgresAdvisoryLockPoller(t *testing.T, conn *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var polling bool
		err := conn.QueryRowContext(t.Context(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND query LIKE '%pg_try_advisory_lock%'
			)`).Scan(&polling)
		c.Assert(err, qt.IsNil)
		if polling {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for schema apply to poll the advisory lock")
}

func strictCompatPostgresTestURL(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for strict schema-clean runtime coverage")
	}
	return dbURL
}

func strictCompatPostgresDevURL(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("PTAH_ATLAS_ORACLE_POSTGRES_DEV_URL")
	if dbURL == "" {
		t.Skip("PTAH_ATLAS_ORACLE_POSTGRES_DEV_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL dev URL required for strict migrate-diff runtime coverage")
	}
	return dbURL
}

func postgresURLWithSearchPath(t *testing.T, rawURL, schema string) string {
	c := qt.New(t)
	t.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func postgresStrictCleanObjectCount(t *testing.T, dbURL string) int {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(t.Context(), `
		SELECT
			(SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'users') +
			(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = current_schema() AND c.relname = 'users_id_seq' AND c.relkind = 'S') +
			(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
			 WHERE n.nspname = current_schema() AND p.proname = 'refresh_users' AND p.prokind = 'p')`,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func postgresViewExists(t *testing.T, dbURL, view string) bool {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var exists bool
	err = conn.QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1
			FROM pg_class relation
			JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = current_schema()
			  AND relation.relname = $1
			  AND relation.relkind = 'v'
		)`, view).Scan(&exists)
	c.Assert(err, qt.IsNil)
	return exists
}

func TestStrictCompatSchemaInspectValidatesMigrationBeforeDevReset(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(
		`-- +ptah check name="ready" assert="SELECT 1"`+"\n"+
			"CREATE TABLE replayed_users (id integer PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	devPath := filepath.Join(dir, "dev.db")
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE sentinel (id integer PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	stdout, stderr, code := runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		"schema", "inspect",
		"--url", "file://"+migrationsDir,
		"--dev-url", "sqlite://"+devPath,
	)
	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support "+
			"Ptah pre-migration checks in 1_init.sql\n")

	conn, err = dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devPath)
	c.Assert(err, qt.IsNil)
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Tables[0].Name, qt.Equals, "sentinel")
	dbschema.CloseAndWarn(conn)

	stdout, stderr, code = runAtlasBinary(
		compat,
		nil,
		"schema", "inspect",
		"--url", "file://"+migrationsDir,
		"--dev-url", "sqlite://"+devPath,
	)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `table "replayed_users"`)
	c.Assert(stderr, qt.Equals, "")
}

func runAtlasBinary(binary string, additions []string, args ...string) (stdout, stderr string, code int) {
	cmd := exec.Command(binary, args...)
	cmd.Env = append(environmentWithoutPtahVariables(), additions...)
	var stdoutBuffer bytes.Buffer
	var stderrBuffer bytes.Buffer
	cmd.Stdout = &stdoutBuffer
	cmd.Stderr = &stderrBuffer
	err := cmd.Run()
	if err == nil {
		return stdoutBuffer.String(), stderrBuffer.String(), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return stdoutBuffer.String(), stderrBuffer.String(), exitErr.ExitCode()
	}
	return stdoutBuffer.String(), stderrBuffer.String(), -1
}

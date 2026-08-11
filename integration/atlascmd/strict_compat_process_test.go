//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

const atlasCommunityInstallBody = `To install the non-community version of Atlas, use the following command:

	curl -sSf https://atlasgo.sh | sh

Or, visit the website to see all installation options:

	https://atlasgo.io/docs#installation

`

func TestStrictCompatProcessMatchesCommunityGates(t *testing.T) {
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
			wantStdout: "'atlas schema plan' is not supported by the community version.\n\n" + atlasCommunityInstallBody,
		},
		{
			name:       "schema plan execution",
			args:       []string{"schema", "plan"},
			wantStderr: "Abort: 'atlas schema plan' is not supported by the community version.\n\n" + atlasCommunityInstallBody,
			wantCode:   1,
		},
		{
			name:       "schema apply plan execution",
			args:       []string{"schema", "apply", "--plan", "file://missing.plan"},
			wantStderr: "Abort: 'atlas schema apply --plan' is not supported by the community version.\n\n" + atlasCommunityInstallBody,
			wantCode:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				test.args...,
			)

			qt.Assert(t, code, qt.Equals, test.wantCode)
			qt.Assert(t, stdout, qt.Equals, test.wantStdout)
			qt.Assert(t, stderr, qt.Equals, test.wantStderr)
		})
	}
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
			stdout, stderr, code := runAtlasBinary(compat, test.env, "schema", "plan", "--help")

			qt.Assert(t, code, qt.Equals, 0)
			qt.Assert(t, stdout, qt.Contains, "Atlas OSS `atlas schema plan` command path.")
			qt.Assert(t, stdout, qt.Not(qt.Contains), "not supported by the community version")
			qt.Assert(t, stderr, qt.Equals, "")
		})
	}
}

func TestStrictCompatProcessRejectsInvalidPolicyBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	tests := [][]string{{"--help"}, {"version"}, {"unknown-command"}}

	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT="},
				args...,
			)

			qt.Assert(t, code, qt.Equals, 1)
			qt.Assert(t, stdout, qt.Equals, "")
			qt.Assert(t, stderr, qt.Equals,
				`Error: invalid boolean value "" for PTAH_ATLAS_STRICT_COMPAT`+"\n")
		})
	}
}

func TestStrictCompatProcessRejectsExtensionEnvironmentBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	for _, assignment := range []string{
		"PTAH_ATLAS_INSPECT_ALL_BLOCKS=1",
		"PTAH_URL=sqlite://must-not-be-ignored",
	} {
		t.Run(strings.SplitN(assignment, "=", 2)[0], func(t *testing.T) {
			name := strings.SplitN(assignment, "=", 2)[0]
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{
					"PTAH_ATLAS_STRICT_COMPAT=1",
					assignment,
				},
				"version",
			)

			qt.Assert(t, code, qt.Equals, 1)
			qt.Assert(t, stdout, qt.Equals, "")
			qt.Assert(t, stderr, qt.Equals,
				"Error: PTAH_ATLAS_STRICT_COMPAT does not allow "+name+"\n")
		})
	}
}

func TestNativeProcessIgnoresStrictCompatEnvironment(t *testing.T) {
	c := qt.New(t)
	native := buildSchemaInspectBinary(c, "ptah", "go.5x5.cz/ptah/cmd/ptah")

	stdout, stderr, code := runAtlasBinary(
		native,
		[]string{"PTAH_ATLAS_STRICT_COMPAT="},
		"version",
	)

	qt.Assert(t, code, qt.Equals, 0)
	qt.Assert(t, stdout, qt.Contains, "Version:")
	qt.Assert(t, stderr, qt.Equals, "")
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
	qt.Assert(t, code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	qt.Assert(t, stdout, qt.Contains, "CREATE VIEW")
	qt.Assert(t, stderr, qt.Equals, "")

	stdout, stderr, code = runAtlasBinary(
		compat,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
		args...,
	)
	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Equals,
		"Error: load --to schema: Atlas Community Edition strict compatibility does not support desired schema views\n")
}

func TestStrictCompatRefusesProInspectTemplateFunctionsBeforeSourceWork(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	for _, function := range []string{"hcl", "split", "write"} {
		t.Run(function, func(t *testing.T) {
			format := `{{ sql . | ` + function + ` }}`
			stdout, stderr, code := runAtlasBinary(
				compat,
				[]string{"PTAH_ATLAS_STRICT_COMPAT=1"},
				"schema", "inspect",
				"--url", "unknown://source-must-not-be-resolved",
				"--format", format,
			)

			qt.Assert(t, code, qt.Equals, 1)
			qt.Assert(t, stdout, qt.Equals, "")
			qt.Assert(t, stderr, qt.Equals,
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

	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support desired schema views\n")
	_, statErr := os.Stat(devPath)
	qt.Assert(t, statErr, qt.ErrorIs, os.ErrNotExist)
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

	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Contains, `unsupported top-level block "wibble"`)
	_, statErr := os.Stat(devPath)
	qt.Assert(t, statErr, qt.ErrorIs, os.ErrNotExist)
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
	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Equals,
		`Error: Atlas Community Edition strict compatibility does not support YAML schema source "schema.yaml"`+"\n")
	_, statErr := os.Stat(devPath)
	qt.Assert(t, statErr, qt.ErrorIs, os.ErrNotExist)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	qt.Assert(t, code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	qt.Assert(t, stdout, qt.Contains, "CREATE TABLE")
	qt.Assert(t, stdout, qt.Contains, "users")
	qt.Assert(t, stderr, qt.Equals, "")
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

	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Contains,
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
	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Equals,
		"Error: schemahcl: for_each does not support list of string type\n")
	_, statErr := os.Stat(databasePath)
	qt.Assert(t, statErr, qt.ErrorIs, os.ErrNotExist)

	stdout, stderr, code = runAtlasBinary(compat, nil, args...)
	qt.Assert(t, code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	qt.Assert(t, stdout, qt.Equals, "schema \"main\" {\n}\n")
	qt.Assert(t, stderr, qt.Equals, "")
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

	qt.Assert(t, code, qt.Equals, 1)
	qt.Assert(t, stdout, qt.Equals, "")
	qt.Assert(t, stderr, qt.Equals,
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

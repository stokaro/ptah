package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/dbschema"
)

func TestCompatCommand_MigrateLintDevURLReplaysMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	writeAtlasLintDevURLFile(c, migrationsDir, "1_create_atlas_lint_dev_url.sql",
		"CREATE TABLE atlas_lint_dev_url (id INTEGER PRIMARY KEY);\n")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + migrationsDir,
		"--dev-url", "sqlite://" + devDBPath,
		"--latest", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	// The no-format default now renders Atlas's migration-analysis text report.
	c.Assert(out.String(), qt.Contains, "Analyzing changes until version 1 (1 migration in total):")
	c.Assert(out.String(), qt.Contains, "  -- analyzing version 1\n    -- no diagnostics found\n")
	c.Assert(out.String(), qt.Contains, "  -- 1 version ok\n")
	assertAtlasLintDevURLSQLiteTableCount(c, devDBPath, "atlas_lint_dev_url", 0)
}

// TestCompatCommand_MigrateLintRoutesADockerDevURLToTheProvisioner replaced a
// test asserting that `migrate lint` refused every docker:// dev URL. Since
// stokaro/ptah#844 it provisions one, and measured against the pinned community
// binary v1.3.0 on 2026-08-13 both now exit 0 on `--dev-url
// docker://postgres/16/dev`.
//
// The URL below is a docker one this build will not start, so the row measures
// the routing without a container runtime. That refusal is itself measured:
// `docker://sqlite/3/dev` makes the pinned binary answer `unsupported docker
// image "sqlite"` and exit 1, so provisioning it would be exiting 0 where that
// binary exits 1.
func TestCompatCommand_MigrateLintRoutesADockerDevURLToTheProvisioner(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	writeAtlasLintDevURLFile(c, migrationsDir, "1_create_atlas_lint_dev_url.sql",
		"CREATE TABLE atlas_lint_dev_url_docker (id INTEGER PRIMARY KEY);\n")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + migrationsDir,
		"--dev-url", "docker://sqlite/3/dev",
		"--latest", "1",
	})

	err := executeAtlasTestCommand(cmd)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(out.String(), qt.Contains, `unsupported docker image "sqlite"`)
}

// The opt-in is set because this fixture lints a destructive migration with no
// dev database: the env's lint policy is what the test measures, and requiring
// --dev-url on the compatibility surface (stokaro/ptah#1231 case 2) would
// otherwise change the subject.
func TestCompatCommand_MigrateLintUsesAtlasProjectEnvPolicy(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeAtlasLintDevURLFile(c, migrationsDir, "1_drop_column.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeAtlasLintDevURLFile(c, migrationsDir, "1_drop_column.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    destructive {
      error = false
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "ci",
		"--latest", "1",
		"--format", "{{ json . }}",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"rule":"DS103"`)
	c.Assert(out.String(), qt.Contains, `"severity":"warning"`)
}

func writeAtlasLintDevURLFile(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func assertAtlasLintDevURLSQLiteTableCount(c *qt.C, dbPath, table string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

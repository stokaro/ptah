package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompatExplicitSQLiteURLValidatesVirtualDropToggleBeforeProjectConfig(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(`env "broken" { url = }`), 0o600), qt.IsNil)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "schema apply target",
			args: []string{
				"schema", "apply",
				"--url", "sqlite://" + filepath.Join(t.TempDir(), "target.db"),
				"--config", "file://" + configPath,
			},
		},
		{
			name: "schema diff current database",
			args: []string{
				"schema", "diff",
				"--from", "sqlite://" + filepath.Join(t.TempDir(), "current.db"),
				"--config", "file://" + configPath,
			},
		},
		{
			name: "migrate diff dev database",
			args: []string{
				"migrate", "diff", "toggle_order",
				"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
				"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
				"--config", "file://" + configPath,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := runCompatCommand(t, test.args...)

			c.Assert(err, qt.ErrorMatches,
				`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
				qt.Commentf("%s", out))
			c.Assert(out, qt.Not(qt.Contains), "atlas.hcl")
		})
	}
}

func TestCompatExplicitPostgresURLLeavesSQLiteToggleToSQLiteCommands(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(`env "broken" { url = }`), 0o600), qt.IsNil)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "schema apply target",
			args: []string{
				"schema", "apply",
				"--url", "postgres://localhost/database",
				"--config", "file://" + configPath,
			},
		},
		{
			name: "schema diff current database",
			args: []string{
				"schema", "diff",
				"--from", "postgres://localhost/database",
				"--config", "file://" + configPath,
			},
		},
		{
			name: "migrate diff dev database",
			args: []string{
				"migrate", "diff", "toggle_order",
				"--dev-url", "postgres://localhost/database",
				"--dir", "file://" + filepath.Join(t.TempDir(), "migrations"),
				"--config", "file://" + configPath,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := runCompatCommand(t, test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
			c.Assert(out, qt.Contains, "atlas.hcl")
		})
	}
}

func TestCompatSchemaDiffExplicitSQLiteURLValidatesVirtualDropToggleBeforeEarlyReturn(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+filepath.Join(t.TempDir(), "current.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--export",
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "--export")
}

func TestCompatSchemaDiffExplicitPostgresURLKeepsExportRefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--export",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	c.Assert(out, qt.Contains, "--export")
}

func TestCompatSchemaApplyExplicitSQLiteURLValidatesVirtualDropToggleBeforePreRunRefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--dry-run", "--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "dry-run auto-approve")
}

func TestCompatSchemaApplyExplicitPostgresURLKeepsPreRunRefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--dry-run", "--auto-approve",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	c.Assert(err.Error(), qt.Contains, "dry-run auto-approve")
	c.Assert(out, qt.Equals, "")
}

func TestCompatSchemaApplyExplicitSQLiteURLValidatesVirtualDropToggleBeforeArgsRefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply", "unexpected",
		"--url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "unexpected positional arguments")
}

func TestCompatSchemaApplyExplicitPostgresURLKeepsArgsRefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply", "unexpected",
		"--url", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	c.Assert(err.Error(), qt.Contains, "unexpected positional arguments")
	c.Assert(out, qt.Contains, "unexpected positional arguments")
}

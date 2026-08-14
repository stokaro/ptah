package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompatExplicitSQLiteURLValidatesVirtualDropToggleBeforeProjectConfig(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "atlas.hcl")
	qt.Assert(t, os.WriteFile(configPath, []byte(`env "broken" { url = }`), 0o600), qt.IsNil)

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
			out, err := runCompatCommand(t, test.args...)

			qt.Assert(t, err, qt.ErrorMatches,
				`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
				qt.Commentf("%s", out))
			qt.Assert(t, out, qt.Not(qt.Contains), "atlas.hcl")
		})
	}
}

func TestCompatExplicitPostgresURLLeavesSQLiteToggleToSQLiteCommands(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	configPath := filepath.Join(t.TempDir(), "atlas.hcl")
	qt.Assert(t, os.WriteFile(configPath, []byte(`env "broken" { url = }`), 0o600), qt.IsNil)

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
			out, err := runCompatCommand(t, test.args...)

			qt.Assert(t, err, qt.IsNotNil)
			qt.Assert(t, err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
			qt.Assert(t, out, qt.Contains, "atlas.hcl")
		})
	}
}

func TestCompatSchemaDiffExplicitSQLiteURLValidatesVirtualDropToggleBeforeEarlyReturn(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "sqlite://"+filepath.Join(t.TempDir(), "current.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--export",
	)

	qt.Assert(t, err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	qt.Assert(t, out, qt.Not(qt.Contains), "--export")
}

func TestCompatSchemaDiffExplicitPostgresURLKeepsExportRefusal(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "diff",
		"--from", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--export",
	)

	qt.Assert(t, err, qt.IsNotNil)
	qt.Assert(t, err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	qt.Assert(t, out, qt.Contains, "--export")
}

func TestCompatSchemaApplyExplicitSQLiteURLValidatesVirtualDropToggleBeforePreRunRefusal(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--dry-run", "--auto-approve",
	)

	qt.Assert(t, err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	qt.Assert(t, out, qt.Not(qt.Contains), "dry-run auto-approve")
}

func TestCompatSchemaApplyExplicitPostgresURLKeepsPreRunRefusal(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply",
		"--url", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
		"--dry-run", "--auto-approve",
	)

	qt.Assert(t, err, qt.IsNotNil)
	qt.Assert(t, err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	qt.Assert(t, err.Error(), qt.Contains, "dry-run auto-approve")
	qt.Assert(t, out, qt.Equals, "")
}

func TestCompatSchemaApplyExplicitSQLiteURLValidatesVirtualDropToggleBeforeArgsRefusal(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply", "unexpected",
		"--url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
	)

	qt.Assert(t, err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`,
		qt.Commentf("%s", out))
	qt.Assert(t, out, qt.Not(qt.Contains), "unexpected positional arguments")
}

func TestCompatSchemaApplyExplicitPostgresURLKeepsArgsRefusal(t *testing.T) {
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	out, err := runCompatCommand(t,
		"schema", "apply", "unexpected",
		"--url", "postgres://localhost/database",
		"--to", "file://"+filepath.Join(t.TempDir(), "desired.sql"),
	)

	qt.Assert(t, err, qt.IsNotNil)
	qt.Assert(t, err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
	qt.Assert(t, err.Error(), qt.Contains, "unexpected positional arguments")
	qt.Assert(t, out, qt.Contains, "unexpected positional arguments")
}

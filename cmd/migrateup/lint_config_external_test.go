package migrateup_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/dbschema"
)

func TestMigrateUp_InvalidLintPolicyPreventsExecution(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		args    []string
		wantErr string
	}{
		{
			name:    "unsupported configured dialect",
			policy:  "dialect: oracle\n",
			wantErr: `.*unsupported lint dialect "oracle": expected postgres.*`,
		},
		{
			name:    "configured dialect differs from database",
			policy:  "dialect: postgres\n",
			wantErr: `.*lint dialect "postgres" does not match database dialect "sqlite".*`,
		},
		{
			name:    "bypass still validates policy",
			policy:  "dialect: oracle\n",
			args:    []string{"--allow-destructive"},
			wantErr: `.*unsupported lint dialect "oracle": expected postgres.*`,
		},
		{
			name:    "bypass still validates registered selectors",
			policy:  "disabled-rules:\n  - ZZ404\n",
			args:    []string{"--allow-destructive"},
			wantErr: `.*rule selector "ZZ404" does not match any registered rule.*`,
		},
		{
			name:    "malformed exclusion glob",
			policy:  "rules:\n  DS101:\n    exclude:\n      - '[legacy/**'\n",
			wantErr: `.*rule DS101 has invalid exclude pattern "\[legacy/\*\*": syntax error in pattern.*`,
		},
		{
			name:    "non-normalized exclusion glob",
			policy:  "rules:\n  DS101:\n    exclude:\n      - '**/../**'\n",
			wantErr: `.*rule DS101 has invalid exclude pattern "\*\*/\.\./\*\*": pattern must be a normalized slash-separated path.*`,
		},
		{
			name:    "parent-directory exclusion glob",
			policy:  "rules:\n  DS101:\n    exclude:\n      - '../legacy/**'\n",
			wantErr: `.*rule DS101 has invalid exclude pattern "\.\./legacy/\*\*": pattern must not contain \. or \.\. path segments.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"), []byte(test.policy), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_create_users.up.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
				0o600,
			), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_create_users.down.sql"),
				[]byte("DROP TABLE users;\n"),
				0o600,
			), qt.IsNil)

			dbURL := "sqlite://" + filepath.Join(c.TempDir(), "target.db")
			cmd := migrateup.NewMigrateUpCommand()
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			args := []string{
				"--db-url", dbURL,
				"--migrations-dir", dir,
			}
			cmd.SetArgs(append(args, test.args...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			var count int
			err = conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&count)
			c.Assert(err, qt.IsNil)
			c.Assert(count, qt.Equals, 0)
		})
	}
}

func TestMigrateUp_ValidLintPolicyAppliesMigration(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"), []byte("dialect: sqlite\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.down.sql"),
		[]byte("DROP TABLE users;\n"),
		0o600,
	), qt.IsNil)

	dbURL := "sqlite://" + filepath.Join(c.TempDir(), "target.db")
	cmd := migrateup.NewMigrateUpCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

func TestMigrateUp_InvalidLintSelectorRejectedWithoutPendingMigrations(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	configPath := filepath.Join(dir, ".ptah-lint.yaml")
	c.Assert(os.WriteFile(configPath, []byte("dialect: sqlite\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.down.sql"),
		[]byte("DROP TABLE users;\n"),
		0o600,
	), qt.IsNil)
	dbURL := "sqlite://" + filepath.Join(c.TempDir(), "target.db")

	first := migrateup.NewMigrateUpCommand()
	first.SetArgs([]string{"--db-url", dbURL, "--migrations-dir", dir})
	c.Assert(first.Execute(), qt.IsNil)
	c.Assert(os.WriteFile(configPath, []byte("rules:\n  ZZ404:\n    severity: error\n"), 0o600), qt.IsNil)

	second := migrateup.NewMigrateUpCommand()
	second.SetArgs([]string{"--db-url", dbURL, "--migrations-dir", dir})
	err := second.Execute()

	c.Assert(err, qt.ErrorMatches, `.*rule selector "ZZ404" does not match any registered rule.*`)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

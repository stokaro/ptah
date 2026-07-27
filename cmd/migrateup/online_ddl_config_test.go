package migrateup_test

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrateup"
)

func TestMigrateUpCommandUsesNamedEnvOnlineDDLConfig(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(
		os.WriteFile(
			filepath.Join(migrationsDir, "000001_create_users.up.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(
		os.WriteFile(
			filepath.Join(migrationsDir, "000001_create_users.down.sql"),
			[]byte("DROP TABLE users;\n"),
			0o600,
		),
		qt.IsNil,
	)
	dbURL := (&url.URL{
		Scheme: "sqlite",
		Path:   filepath.Join(t.TempDir(), "ptah.db"),
	}).String()
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Sprintf(`env:
  local:
    url: %s
    migration:
      dir: %s
    online_ddl:
      tool: ghost
      threshold_rows: 250000
`, dbURL, migrationsDir)
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)

	var output bytes.Buffer
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"--config", configPath,
		"--env", "local",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "Online DDL: tool=ghost threshold_rows=250000")
}

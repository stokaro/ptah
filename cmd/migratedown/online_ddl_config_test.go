package migratedown_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateDownCommandUsesNamedEnvOnlineDDLConfig(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
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
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	mig, err := migrator.NewFSMigrator(conn, os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Sprintf(`env:
  local:
    url: %s
    migration:
      dir: %s
    online_ddl:
      tool: pt-osc
      threshold_rows: 500000
`, dbURL, migrationsDir)
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)

	var output bytes.Buffer
	cmd := migratedown.NewMigrateDownCommand()
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"--config", configPath,
		"--env", "local",
		"--target", "0",
		"--confirm",
		"--dry-run",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "Online DDL: tool=pt-osc threshold_rows=500000")
}

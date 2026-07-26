package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateApplyWithAtlasProjectExecutionOrderIdentifier(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	c.Assert(os.Mkdir("migrations", 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join("migrations", "20260101000000_create_widgets.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	sum, sumErr := atlascompat.ComputeSum(os.DirFS("migrations"), migrator.MigrationDirFormatAtlas)
	c.Assert(sumErr, qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join("migrations", atlascompat.AtlasSumFileName),
			sum.Bytes(),
			0o600,
		),
		qt.IsNil,
	)
	dbPath := filepath.Join(root, "apply.db")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir        = "file://migrations"
    format     = atlas
    exec_order = LINEAR_SKIP
    tx_mode    = "file"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"migrate", "apply", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output.String()))
	conn, connectErr := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(connectErr, qt.IsNil)
	defer conn.Close()
	var tableCount int
	queryErr := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'widgets'`,
	).Scan(&tableCount)
	c.Assert(queryErr, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 1)
}

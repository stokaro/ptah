package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrate"
)

func TestMigratePlanTextOutputContainsSQLNotASTPointers(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`), 0o600), qt.IsNil)

	var out bytes.Buffer
	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--root-dir", modelsDir,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `CREATE TABLE "users"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "[0x")
	c.Assert(out.String(), qt.Not(qt.Contains), "&{")
}

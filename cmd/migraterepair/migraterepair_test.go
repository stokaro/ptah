package migraterepair_test

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"github.com/stokaro/ptah/cmd/migraterepair"
)

func TestMigrateRepairCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migraterepair.NewMigrateRepairCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "repair")
	c.Assert(cmd.Short, qt.Contains, "Repair dirty migration metadata")
	c.Assert(cmd.Flag("db-url"), qt.IsNotNil)
	c.Assert(cmd.Flag("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flag("version"), qt.IsNotNil)
	c.Assert(cmd.Flag("force"), qt.IsNotNil)
	c.Assert(cmd.Flag("resume-from"), qt.IsNotNil)
}

func TestMigrateRepairCommand_AtlasFormatUsesRepairSemantics(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(migrationsDir, "1_create_users.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	dbPath := filepath.Join(root, "state.db")
	cmd := migraterepair.NewMigrateRepairCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "1",
		"--force",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output.String()))
	c.Assert(output.String(), qt.Equals, "Repaired migration 1\n")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})
	var revisionType, applied, total int
	err = db.QueryRow(
		`SELECT type, applied, total FROM atlas_schema_revisions WHERE version = '1'`,
	).Scan(&revisionType, &applied, &total)
	c.Assert(err, qt.IsNil)
	c.Assert(revisionType, qt.Equals, 2)
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 1)
}

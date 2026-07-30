//go:build !windows

package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/pathguard"
)

func TestGenerateDiff_RejectsPreparedDirectoryReplacementBeforeDevReset(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o755), qt.IsNil)
	opened, err := pathguard.OpenDirectory(migrationsDir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	prepared, err := atlasmigrate.PrepareDiffDirectory(
		t.Context(),
		migrationsDir,
		opened,
		time.Second,
	)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Rename(migrationsDir, filepath.Join(root, "captured")), qt.IsNil)
	c.Assert(os.Mkdir(migrationsDir, 0o755), qt.IsNil)
	schemaPath := filepath.Join(root, "schema.sql")
	c.Assert(
		os.WriteFile(
			schemaPath,
			[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)
	conn := connectSQLite(c, filepath.Join(root, "dev.db"))
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	_, err = conn.ExecContext(
		t.Context(),
		"CREATE TABLE protected_users (id INTEGER PRIMARY KEY)",
	)
	c.Assert(err, qt.IsNil)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:       migrationsDir,
		Directory: prepared,
		Desired:   localDesiredSet(c, "file://"+schemaPath),
		Name:      "must_not_reset",
	})

	c.Assert(err, qt.ErrorMatches, `migration directory changed during migrate diff planning: .*opened directory path was replaced`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(sqliteTableExists(c, conn, "protected_users"), qt.IsTrue)
}

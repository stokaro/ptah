package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestWriteDataMigrationFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	// Seed an ordinary migration so ptah.sum starts with prior content that must
	// be preserved and extended, not clobbered.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE t;\n"), 0o600), qt.IsNil)

	upPath, downPath, err := generator.WriteDataMigrationFiles(dir, 2, "seed data",
		"INSERT INTO t (id) VALUES (1);\n", "DELETE FROM t WHERE id = 1;\n")
	c.Assert(err, qt.IsNil)

	// The pair uses ordinary migration file names, not checkpoint names.
	c.Assert(filepath.Base(upPath), qt.Equals, "0000000002_seed_data.up.sql")
	c.Assert(filepath.Base(downPath), qt.Equals, "0000000002_seed_data.down.sql")

	parsed, err := migrator.ParseMigrationFileName(filepath.Base(upPath))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.IsCheckpoint, qt.IsFalse)

	upContent, err := os.ReadFile(upPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upContent), qt.Contains, "INSERT INTO t")
	downContent, err := os.ReadFile(downPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downContent), qt.Contains, "DELETE FROM t")

	// ptah.sum was rewritten and now covers both the prior and the new pair.
	sum, err := os.ReadFile(filepath.Join(dir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "0000000001_init.up.sql")
	c.Assert(string(sum), qt.Contains, "0000000002_seed_data.up.sql")
	c.Assert(string(sum), qt.Contains, "0000000002_seed_data.down.sql")

	// Writing the same version again refuses rather than overwriting.
	_, _, err = generator.WriteDataMigrationFiles(dir, 2, "seed data", "x", "y")
	c.Assert(err, qt.ErrorMatches, `migration files for version 2 already exist`)
}

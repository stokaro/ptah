package migratetests_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// writeUpSuffixDownFixture fills migrationsDir with two migrations named as
// `ptah migrations generate` names them, a ten-digit version and an .up.sql
// file beside its .down.sql file, hashes the directory as `migrate hash` does,
// and applies it to dbPath through `migrate apply`, which records each file's
// atlas.sum hash. The native default reads such a directory as a Ptah one.
func writeUpSuffixDownFixture(c *qt.C, migrationsDir, dbPath string) {
	c.Helper()
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	for name, sql := range map[string]string{
		"1795117800_init.up.sql":        "CREATE TABLE up_suffix_users (id INTEGER PRIMARY KEY);",
		"1795117800_init.down.sql":      "DROP TABLE up_suffix_users;",
		"1795117900_add_audit.up.sql":   "CREATE TABLE up_suffix_audit (id INTEGER PRIMARY KEY);",
		"1795117900_add_audit.down.sql": "DROP TABLE up_suffix_audit;",
	} {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(sql+"\n"), 0o600), qt.IsNil)
	}
	atlastest.WriteAtlasApplyProjectSum(c, migrationsDir)

	apply := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	apply.SetOut(&out)
	apply.SetErr(&out)
	apply.SetArgs([]string{"migrate", "apply", "--url", "sqlite://" + dbPath, "--dir", "file://" + migrationsDir})
	c.Assert(apply.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}

// TestCompatCommand_MigrateDownReadsAnUpSuffixDirectoryAsApplyDid rolls back a
// directory whose files end in .up.sql. The forwarded rollback reads it in the
// Atlas layout `migrate apply` read it in; read as a Ptah directory, it compares
// the atlas.sum hash each row holds with the hex digest of the file and refuses
// with `checksum mismatch`.
func TestCompatCommand_MigrateDownReadsAnUpSuffixDirectoryAsApplyDid(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "up-suffix.db")
	writeUpSuffixDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1795117800",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "up_suffix_audit"), qt.Equals, 0)
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "up_suffix_users"), qt.Equals, 1)
}

// TestCompatCommand_MigrateDownRefusesTheNativeDirectoryFormatFlag holds the
// layout to the one apply reads: the flag that would choose another is the
// native command's, and this surface refuses it.
func TestCompatCommand_MigrateDownRefusesTheNativeDirectoryFormatFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "up-suffix-flag.db")
	writeUpSuffixDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1795117800",
		"--dir-format", "ptah",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `.*dir-format.*`)
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "up_suffix_audit"), qt.Equals, 1)
}

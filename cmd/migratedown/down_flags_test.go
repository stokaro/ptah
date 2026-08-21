package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratetag"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/dbschema"
)

// `migrate down --to-tag` and `--skip-checks` were recorded waivers until
// stokaro/ptah#1621. Each waiver named a hosted dependency the flag does not
// have: tags resolve against the local tag namespace, and down bodies have
// carried real pre-migration checks since stokaro/ptah#1715.

const (
	parentUpSQL   = "CREATE TABLE parent (id INTEGER PRIMARY KEY);\n"
	parentDownSQL = "DROP TABLE parent;\n"
	childUpSQL    = "CREATE TABLE child (id INTEGER PRIMARY KEY);\n"
	childDownSQL  = "DROP TABLE child;\n"
	// childDownCheckedSQL aborts the rollback while the table has rows, which
	// is what --skip-checks has to get past.
	childDownCheckedSQL = `-- +ptah check name="child_empty" assert="SELECT count(*) = 0 FROM child" on_fail=abort
DROP TABLE child;
`
)

// twoMigrationDir writes a directory with two migrations, the second carrying
// downSQL, and applies both to a fresh database.
func twoMigrationDir(c *qt.C, downSQL string) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_parent.sql":      parentUpSQL,
		"1_parent.down.sql": parentDownSQL,
		"2_child.sql":       childUpSQL,
		"2_child.down.sql":  downSQL,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(root, "app.db")

	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

func runTag(args ...string) (string, error) {
	cmd := migratetag.NewMigrateTagCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestMigrateDownToTagStopsWhereTheTagPoints is the flag end to end.
func TestMigrateDownToTagStopsWhereTheTagPoints(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v1.0.0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// The catalog, not the report: only the tables say where the rollback
	// actually stopped.
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "child")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "parent")
}

// TestMigrateDownRefusesAnUnknownTag keeps a typo from rolling everything back.
//
// An unresolved tag must not fall through to the numeric default: --target
// defaults to 0, so "ignore the tag" would revert the whole history where the
// operator asked for a bounded rollback.
func TestMigrateDownRefusesAnUnknownTag(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	before := tableCensus(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v9.9.9", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "migration tag not found")
	// The row count is the assertion that matters: the message alone would
	// pass even if the refusal came after the damage.
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// TestMigrateDownRefusesBothTargetAndTag keeps two names for where to stop
// from being resolved by a silent precedence rule.
func TestMigrateDownRefusesBothTargetAndTag(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)
	before := tableCensus(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v1.0.0", "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "both name where to stop")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// TestMigrateDownRunsDownChecksByDefault is the control for the next test.
//
// Without it, --skip-checks would pass against a rollback nothing was blocking,
// which proves nothing about skipping.
func TestMigrateDownRunsDownChecksByDefault(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownCheckedSQL)
	insertChildRow(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "child_empty")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "child")
}

// TestMigrateDownSkipChecksBypassesABlockingDownCheck is the flag, measured
// against the control above: same directory, same row, one flag apart.
func TestMigrateDownSkipChecksBypassesABlockingDownCheck(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownCheckedSQL)
	insertChildRow(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--skip-checks", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "child")
}

// TestMigrationTagListsWhatWasRecorded covers the verb that fills the
// namespace, including the ordering two entries are needed to show.
func TestMigrationTagListsWhatWasRecorded(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v2.0.0", "--db-url", "sqlite://"+dbPath, "--version", "2")
	c.Assert(err, qt.IsNil)
	_, err = runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)

	out, err := runTag("--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(slices.Index(splitLines(out), "TAG     VERSION  RECORDED") >= 0, qt.IsTrue,
		qt.Commentf("%s", out))
	c.Assert(indexOfPrefix(out, "v1.0.0") < indexOfPrefix(out, "v2.0.0"), qt.IsTrue,
		qt.Commentf("%s", out))
}

// TestMigrationTagWithNoNameAndAVersionIsRefused keeps a command that would
// list instead of recording from reading as success.
func TestMigrationTagWithNoNameAndAVersionIsRefused(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)

	out, err := runTag("--db-url", "sqlite://"+dbPath, "--version", "1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "needs a tag name")
}

// TestMigrationTagDefaultsToTheCurrentVersion covers the no---version form.
func TestMigrationTagDefaultsToTheCurrentVersion(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)

	out, err := runTag("release", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Tagged version 2 as release.")
}

// insertChildRow makes the child table non-empty so its down check fails.
func insertChildRow(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO child (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
}

func splitLines(s string) []string { return strings.Split(strings.TrimRight(s, "\n"), "\n") }

func indexOfPrefix(out, prefix string) int {
	for i, line := range splitLines(out) {
		if strings.HasPrefix(line, prefix) {
			return i
		}
	}
	return -1
}

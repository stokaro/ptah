package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// TestCompatMigrateImport_LiquibaseConventionalRoundTrip covers the command
// boundary that exposed #1241 item 8. The mixed source is discriminating: the
// old converter imported only 1_numbered.sql and silently skipped changelog.sql.
// The destination must validate and apply as Atlas format without any foreign
// format query, proving that the adapter emitted a self-contained Atlas
// directory rather than another Liquibase directory under new names.
func TestCompatMigrateImport_LiquibaseConventionalRoundTrip(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	dbPath := filepath.Join(root, "apply.db")
	writeConvertedApplyDir(c.TB, source, map[string]string{
		"1_numbered.sql": `--liquibase formatted sql
--changeset numbered:first
CREATE TABLE numbered_table (id INTEGER PRIMARY KEY);
--rollback DROP TABLE numbered_table;
`,
		"changelog.sql": `--liquibase formatted sql
--changeset conventional:second
CREATE TABLE conventional_table (id INTEGER PRIMARY KEY);
--rollback DROP TABLE conventional_table;
`,
	})

	stdout, stderr, err := compatImport(source, target, "liquibase")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	_, statErr := os.Stat(filepath.Join(target, "1_numbered_first.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "2_conventional_second.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)

	validateOut, validateErrOut, validateErr := runCompat(
		"migrate", "validate", "--dir", "file://"+target,
	)
	c.Assert(validateErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", validateOut, validateErrOut))
	c.Assert(validateOut, qt.Equals, "")
	c.Assert(validateErrOut, qt.Equals, "")

	applyOut, applyErrOut, applyErr := compatApply(target, dbPath)
	c.Assert(applyErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applyOut, applyErrOut))
	c.Assert(sqliteTableCount(c.TB, dbPath, "numbered_table"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, dbPath, "conventional_table"), qt.Equals, 1)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, dbPath), qt.DeepEquals, []string{"1", "2"})
}

// TestCompatMigrateImport_LiquibasePadsGlobalVersions is the order control for
// a changeset stream that crosses from one digit to two. Without padding,
// atlas.sum orders 10 and 11 before 1; applying the destination then attempts
// step 10 before step 1 creates ordered_steps and fails.
func TestCompatMigrateImport_LiquibasePadsGlobalVersions(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	dbPath := filepath.Join(root, "apply.db")
	writeConvertedApplyDir(c.TB, source, map[string]string{
		"changelog.sql": `--liquibase formatted sql
--changeset order:step-1
CREATE TABLE ordered_steps (step INTEGER PRIMARY KEY);
INSERT INTO ordered_steps (step) VALUES (1);
--changeset order:step-2
INSERT INTO ordered_steps (step) VALUES (2);
--changeset order:step-3
INSERT INTO ordered_steps (step) VALUES (3);
--changeset order:step-4
INSERT INTO ordered_steps (step) VALUES (4);
--changeset order:step-5
INSERT INTO ordered_steps (step) VALUES (5);
--changeset order:step-6
INSERT INTO ordered_steps (step) VALUES (6);
--changeset order:step-7
INSERT INTO ordered_steps (step) VALUES (7);
--changeset order:step-8
INSERT INTO ordered_steps (step) VALUES (8);
--changeset order:step-9
INSERT INTO ordered_steps (step) VALUES (9);
--changeset order:step-10
INSERT INTO ordered_steps (step)
SELECT 10 WHERE (SELECT count(*) FROM ordered_steps) = 9;
--changeset order:step-11
INSERT INTO ordered_steps (step)
SELECT 11 WHERE (SELECT count(*) FROM ordered_steps) = 10;
`,
	})

	stdout, stderr, err := compatImport(source, target, "liquibase")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	_, statErr := os.Stat(filepath.Join(target, "01_order_step_1.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "09_order_step_9.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "10_order_step_10.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "11_order_step_11.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "1_order_step_1.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)

	validateOut, validateErrOut, validateErr := runCompat(
		"migrate", "validate", "--dir", "file://"+target,
	)
	c.Assert(validateErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", validateOut, validateErrOut))
	c.Assert(validateOut, qt.Equals, "")
	c.Assert(validateErrOut, qt.Equals, "")

	applyOut, applyErrOut, applyErr := compatApply(target, dbPath)
	c.Assert(applyErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applyOut, applyErrOut))
	c.Assert(applyOut, qt.Contains, "Migration complete. Current version: 11")
	c.Assert(sqliteRowCount(c.TB, dbPath, "ordered_steps"), qt.Equals, 11)
	c.Assert(sqliteAtlasRevisionVersionsNumeric(c.TB, dbPath), qt.DeepEquals, []string{
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
	})
}

func TestCompatMigrateImport_LiquibaseMixedInvalidRefusesBeforeTargetCreation(t *testing.T) {
	c := qt.New(t)
	source, target := importDirs(c.TB)
	writeConvertedApplyDir(c.TB, source, map[string]string{
		"1_headerless.sql": "CREATE TABLE skipped_table (id INTEGER PRIMARY KEY);\n",
		"changelog.sql": `--liquibase formatted sql
--changeset conventional:valid
CREATE TABLE valid_table (id INTEGER PRIMARY KEY);
`,
	})

	stdout, stderr, err := compatImport(source, target, "liquibase")

	c.Assert(err, qt.ErrorMatches, `parse liquibase source file 1_headerless\.sql: no liquibase formatted-SQL changelogs .* found`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Matches, `Error: parse liquibase source file 1_headerless\.sql: no liquibase formatted-SQL changelogs .* found\n`)
	assertNothingImported(c.TB, target)
}

func sqliteAtlasRevisionVersionsNumeric(tb testing.TB, dbPath string) []string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(
		context.Background(),
		"SELECT version FROM atlas_schema_revisions ORDER BY CAST(version AS INTEGER)",
	)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

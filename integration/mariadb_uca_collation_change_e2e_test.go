//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// mariaDBUCAEngines are the MariaDB lines whose default collation for utf8 is
// utf8mb3_uca1400_ai_ci, a UCA 14.0.0 collation the server lists with no
// character set.
var mariaDBUCAEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MariaDB 12.3", engine: dbtarget.MariaDBAdmin},
	{name: "MariaDB 11.8", engine: dbtarget.MariaDB118},
}

// mariaDBUCARows change table t's default to a UCA 14.0.0 collation of utf8.
// Column n names no collation, so it takes the table's.
var mariaDBUCARows = []struct {
	name string
	// from builds the table; to is the schema file applied to it.
	from string
	to   string
	// compat and community are the plans each binary prints.
	compat    string
	community string
	// column is the collation n holds after ptah-compat applies its plan.
	column string
}{
	{
		name:      "utf8mb4 to utf8",
		from:      "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4;",
		to:        "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8;\n",
		compat:    "ALTER TABLE `t` COLLATE utf8mb3_uca1400_ai_ci, MODIFY COLUMN `n` varchar(20) NULL;",
		community: "ALTER TABLE `t` COLLATE utf8mb3_uca1400_ai_ci, MODIFY COLUMN `n` varchar(20) CHARSET utf8mb3 NULL;",
		column:    "utf8mb3_uca1400_ai_ci",
	},
	{
		name:      "utf8mb4 to a utf8 collation other than the default",
		from:      "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4;",
		to:        "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8 COLLATE=utf8mb3_uca1400_as_cs;\n",
		compat:    "ALTER TABLE `t` COLLATE utf8mb3_uca1400_as_cs, MODIFY COLUMN `n` varchar(20) NULL;",
		community: "ALTER TABLE `t` COLLATE utf8mb3_uca1400_as_cs, MODIFY COLUMN `n` varchar(20) CHARSET utf8mb3 NULL;",
		column:    "utf8mb3_uca1400_as_cs",
	},
	{
		name:      "latin1 to utf8",
		from:      "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=latin1;",
		to:        "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8;\n",
		compat:    "ALTER TABLE `t` CHARSET utf8mb3, COLLATE utf8mb3_uca1400_ai_ci, MODIFY COLUMN `n` varchar(20) NULL;",
		community: "ALTER TABLE `t` CHARSET utf8mb4, COLLATE utf8mb3_uca1400_ai_ci, MODIFY COLUMN `n` varchar(20) CHARSET utf8mb3 NULL;",
		column:    "utf8mb3_uca1400_ai_ci",
	},
}

// TestCompatSchemaApplyChangesAMariaDBTableToAUCACollationE2E applies each
// change with ptah-compat, reads column n back and plans again. The plan
// restates n without a character set, so n takes the table's new collation,
// and the second plan is empty. The table's CHARSET, where the plan writes
// one, is the collation's own.
//
// The community binary writes `CHARSET utf8mb3` on the column. Where the table
// takes a collation other than the default of utf8mb3, that clause gives n the
// default instead, and every later plan restates n. Where the table moves from
// latin1, the binary writes the dev database's character set beside the
// collation, and MariaDB refuses the statement with Error 1253.
func TestCompatSchemaApplyChangesAMariaDBTableToAUCACollationE2E(t *testing.T) {
	for _, engine := range mariaDBUCAEngines {
		for _, row := range mariaDBUCARows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				database, url := scratch.builtFrom(c, row.from)
				_, devURL := scratch.database(c, "uca_dev")
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(row.to), 0o600), qt.IsNil)
				to := "file://" + filepath.ToSlash(schema)

				applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "-u", url, "--to", to, "--dev-url", devURL, "--auto-approve")
				again := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "-u", url, "--to", to, "--dev-url", devURL, "--dry-run")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(plannedAlters(applied.Stdout, database), qt.DeepEquals, []string{row.compat}, qt.Commentf("%s", applied.Stdout))
				c.Assert(collationOfN(c, scratch, database), qt.Equals, row.column)
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(plannedAlters(again.Stdout, database), qt.HasLen, 0, qt.Commentf("%s", again.Stdout))
			})
		}
	}
}

// TestMariaDBUCACollationChangeOnTheCommunityBinaryE2E holds the measurement
// ptah-compat is compared with: the plans the pinned community binary prints
// for the same files on the same servers.
func TestMariaDBUCACollationChangeOnTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, engine := range mariaDBUCAEngines {
		for _, row := range mariaDBUCARows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, devURL := scratch.database(c, "uca_dev")
				from, to := writeSchemaFiles(c, "schema.sql", row.from+"\n", row.to)

				output, err := exec.CommandContext(c.Context(), reference, "schema", "diff", // #nosec G204 G702 -- operator-provided pinned reference binary
					"--from", from, "--to", to, "--dev-url", referenceMySQLURL(c, devURL)).CombinedOutput()

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(plannedAlters(string(output), "app"), qt.DeepEquals, []string{row.community}, qt.Commentf("%s", output))
			})
		}
	}
}

// collationOfN answers the collation column n of table t holds in database.
func collationOfN(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var collation string
	err := scratch.admin.QueryRowContext(c.Context(),
		"SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't' AND COLUMN_NAME = 'n'",
		database,
	).Scan(&collation)
	c.Assert(err, qt.IsNil)
	return collation
}

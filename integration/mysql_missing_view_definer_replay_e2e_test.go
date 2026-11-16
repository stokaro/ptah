//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// missingViewDefiner is an account no test creates, so a view naming it as
// its definer names an account the server lacks.
const missingViewDefiner = "ptah_missing_definer"

// missingViewDefinerHistory creates a table and a SQL SECURITY DEFINER view
// whose definer is [missingViewDefiner]. An administrative account creates such
// a view with a note, and the server then refuses to open it: LOCK TABLES on
// it answers 1449 on MySQL and 1446 on MariaDB.
const missingViewDefinerHistory = "CREATE TABLE `docs` (`id` int NOT NULL, PRIMARY KEY (`id`));\n" +
	"CREATE DEFINER=`" + missingViewDefiner + "`@`%` SQL SECURITY DEFINER VIEW `my_docs` AS SELECT `id` FROM `docs`;\n"

// missingViewDefinerDesired keeps the table, drops the view and adds a table,
// so the step has a change to write.
const missingViewDefinerDesired = "CREATE TABLE docs (id int PRIMARY KEY);\nCREATE TABLE other (id int PRIMARY KEY);\n"

// TestReplayCleansAViewWhoseDefinerIsMissingE2E replays, on a MySQL-family dev
// database, a history whose view names a definer account the server lacks, the
// way a hand-written `DEFINER=` clause for a production account does. The
// cleanup after the replay locks the views it drops, and the server refuses to
// open this one, so the cleanup drops it on its own. Both surfaces write the
// next step, exit 0 and leave the dev database empty.
func TestReplayCleansAViewWhoseDefinerIsMissingE2E(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			var accounts int
			err := scratch.admin.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM mysql.user WHERE user = ?", missingViewDefiner,
			).Scan(&accounts)
			c.Assert(err, qt.IsNil)
			c.Assert(accounts, qt.Equals, 0, qt.Commentf("the definer the history names must not exist"))
			devName, dev := scratch.database(c, "mvd_dev")
			work := c.TempDir()
			desired := filepath.Join(work, "desired.sql")
			writeLintE2EFile(c, work, "desired.sql", missingViewDefinerDesired)

			compatDir := filepath.Join(work, "compat")
			c.Assert(os.MkdirAll(compatDir, 0o755), qt.IsNil)
			writeLintE2EFile(c, compatDir, "20240101000000_init.sql", missingViewDefinerHistory)
			output, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(compatDir))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))

			output, err = runCompatVerb("migrate", "diff", "second",
				"--dir", "file://"+filepath.ToSlash(compatDir),
				"--to", "file://"+filepath.ToSlash(desired), "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(mySQLUserObjectCount(c, c.Context(), scratch.admin, devName), qt.Equals, 0)
			c.Assert(missingDefinerFiles(c, compatDir), qt.HasLen, 3)

			nativeDir := filepath.Join(work, "native")
			c.Assert(os.MkdirAll(nativeDir, 0o755), qt.IsNil)
			writeLintE2EFile(c, nativeDir, "1700000000_init.up.sql", missingViewDefinerHistory)
			writeLintE2EFile(c, nativeDir, "1700000000_init.down.sql", "DROP VIEW `my_docs`;\nDROP TABLE `docs`;\n")

			runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", dev,
				"--schema-file", desired, "--migrations-dir", nativeDir, "--name", "second")

			c.Assert(mySQLUserObjectCount(c, c.Context(), scratch.admin, devName), qt.Equals, 0)
			c.Assert(missingDefinerFiles(c, nativeDir), qt.HasLen, 4)
		})
	}
}

// missingDefinerFiles names the files of dir: the history, the hash file where
// the format has one, and the step the command wrote.
func missingDefinerFiles(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

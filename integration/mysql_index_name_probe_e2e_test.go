//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
)

// A comparison that meets a non-ASCII index name asks a MySQL or MariaDB
// server which index names collide, with a temporary table. On a whole server
// the session selects no database, and the probe answered `Error 1046 (3D000):
// No database selected`; through a database URL it asked about the primary
// key's index, which the server refuses as a key name with `Error 1280`
// (stokaro/ptah#6849). The server holds t with an index named ä, the desired
// schema declares v, and both runs plan the drop and the creation.
func TestMySQLFamilyIndexNameProbeE2E(t *testing.T) {
	for _, engine := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, target := scratch.builtAs(c, "probe", "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, KEY `ä` (a))")
			server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
			c.Assert(err, qt.IsNil)
			dir := c.TempDir()
			single := filepath.Join(dir, "single.sql")
			whole := filepath.Join(dir, "whole.sql")
			c.Assert(os.WriteFile(single, []byte("CREATE TABLE v (id INT NOT NULL PRIMARY KEY);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(whole, []byte("CREATE DATABASE `"+database+"`;\nCREATE TABLE `"+database+"`.v (id INT NOT NULL PRIMARY KEY);\n"),
				0o600), qt.IsNil)

			named := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", single, "--dry-run")
			unnamed := runPtahIn(c, "schema", "apply", "--db-url", server, "--schema-file", whole, "--schemas", database, "--dry-run")

			c.Assert(named.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", named.Stdout, named.Stderr))
			c.Assert(named.Stdout, qt.Contains, "DROP TABLE `t`;")
			c.Assert(unnamed.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", unnamed.Stdout, unnamed.Stderr))
			c.Assert(unnamed.Stdout, qt.Contains, "DROP TABLE `"+database+"`.`t`;")
		})
	}
}

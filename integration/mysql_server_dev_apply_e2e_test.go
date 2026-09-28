//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// schema apply beside a whole MySQL or MariaDB dev server, a --dev-url that
// names no database (stokaro/ptah#3885). The pinned community binary v1.3.0
// plans on such a server; Ptah plans the same, and rehearses the plan there
// before it touches the target. Every test ends with the dev server empty.

// devApplyUnrunnable is realm with a CHECK the server refuses when it creates
// the table: MySQL answers ERROR 3814 and MariaDB ERROR 1901 to RAND() in a
// CHECK, so a rehearsal that ran the plan reports it before the target is
// touched.
func devApplyUnrunnable(c *qt.C, realm, table string) string {
	c.Helper()
	contents, err := os.ReadFile(realm)
	c.Assert(err, qt.IsNil)
	block := "table \"" + table + "\" {\n"
	c.Assert(strings.Count(string(contents), block), qt.Equals, 1)
	unrunnable := strings.Replace(string(contents), block,
		block+"  check \"rand_check\" {\n    expr = \"id < RAND()\"\n  }\n", 1)
	path := filepath.Join(c.TempDir(), "unrunnable.hcl")
	c.Assert(os.WriteFile(path, []byte(unrunnable), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return path
}

// devApplyDatabaseSQL is a SQL file creating app with t(id, name).
func devApplyDatabaseSQL(c *qt.C, app string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "realm.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE DATABASE `"+app+"`;\n"+
		"CREATE TABLE `"+app+"`.t (id int PRIMARY KEY, name varchar(20));\n"), 0o600), qt.IsNil)
	return path
}

// devApplyRealmSQL is devDiffRealm written as SQL: app with t(id, name), and
// more with m(id).
func devApplyRealmSQL(c *qt.C, app, more string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "realm.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE DATABASE `"+app+"`;\n"+
		"CREATE TABLE `"+app+"`.t (id int PRIMARY KEY, name varchar(20));\n"+
		"CREATE DATABASE `"+more+"`;\n"+
		"CREATE TABLE `"+more+"`.m (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return path
}

// columnsOf lists the columns of table in database, in order, on the server
// behind engine.
func columnsOf(c *qt.C, server mysqlServerAccount, database, table string) string {
	c.Helper()
	var columns string
	c.Assert(server.admin.QueryRowContext(c.Context(),
		"SELECT COALESCE(GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION), '') FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", database, table).Scan(&columns), qt.IsNil)
	return columns
}

// TestSchemaApplyAppliesAServerOnADevServerE2E applies a realm to a whole
// server beside a whole dev server. The pinned binary does the same, measured
// on MySQL 8.4.11 and MariaDB 11.8.9: the plan creates more and adds t.name.
// Ptah rehearses the plan on the dev server first, and the dev server is left
// empty.
func TestSchemaApplyAppliesAServerOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app, more := server.names["app"], server.names["more"]
			realm := devDiffRealm(c, app, more)
			dev := dbtarget.URL(c, engine.dev)

			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", server.url,
				"--schema-file", realm, "--dev-url", dev, "--dry-run")
			fromSQL, fromSQLErr := runCompatVerb("schema", "apply", "--url", server.url,
				"--to", "file://"+devApplyRealmSQL(c, app, more), "--dev-url", dev, "--dry-run")
			applied, appliedErr := runCompatVerb("schema", "apply", "--url", server.url, "--to", "file://"+realm,
				"--dev-url", dev, "--auto-approve")

			c.Assert(nativeErr, qt.IsNil, qt.Commentf("%s", native))
			c.Assert(native, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+more+"`")
			c.Assert(fromSQLErr, qt.IsNil, qt.Commentf("%s", fromSQL))
			c.Assert(fromSQL, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+more+"`")
			c.Assert(appliedErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(databasesOn(c, engine.admin, more), qt.Equals, 1)
			c.Assert(columnsOf(c, server, app, "t"), qt.Equals, "id,name")
			c.Assert(databasesOn(c, engine.dev, app, more), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyAppliesOneDatabaseOnADevServerE2E applies a document
// declaring one database to that database beside a whole dev server, which the
// pinned binary plans too. The plan is rehearsed in a database of the target's
// name, created on the dev server and dropped after it.
func TestSchemaApplyAppliesOneDatabaseOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app := server.names["app"]
			dev := dbtarget.URL(c, engine.dev)

			applied, err := runCompatVerb("schema", "apply", "--url", server.url+app,
				"--to", "file://"+devDiffOneSchema(c, app), "--dev-url", dev, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(columnsOf(c, server, app, "t"), qt.Equals, "id,name")
			c.Assert(databasesOn(c, engine.dev, app), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyRehearsesOnADevServerE2E refuses a plan the dev server
// refuses, for a whole server and for one database, before the target is
// touched. The pinned binary does not rehearse, and stops on the target with
// the same server error.
func TestSchemaApplyRehearsesOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app, more := server.names["app"], server.names["more"]
			dev := dbtarget.URL(c, engine.dev)

			wholeOut, wholeErr := runCompatVerb("schema", "apply", "--url", server.url,
				"--to", "file://"+devApplyUnrunnable(c, devDiffRealm(c, app, more), "m"),
				"--dev-url", dev, "--auto-approve")
			oneOut, oneErr := runCompatVerb("schema", "apply", "--url", server.url+app,
				"--to", "file://"+devApplyUnrunnable(c, devDiffOneSchema(c, app), "t"),
				"--dev-url", dev, "--auto-approve")

			c.Assert(wholeErr, qt.ErrorMatches, `(?s).*dev database simulation failed during plan.*rand.*`,
				qt.Commentf("%s", wholeOut))
			c.Assert(oneErr, qt.ErrorMatches, `(?s).*dev database simulation failed during plan.*rand.*`,
				qt.Commentf("%s", oneOut))
			c.Assert(databasesOn(c, engine.admin, more), qt.Equals, 0)
			c.Assert(columnsOf(c, server, app, "t"), qt.Equals, "id")
			c.Assert(databasesOn(c, engine.dev, app, more), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyRefusesOnADevServerE2E refuses what the pinned binary refuses
// beside a dev server, in its words, measured on MySQL 8.4.11 and MariaDB
// 11.8.9: the target server as the dev server, which it finds not clean, and
// SQL beside a target naming one database. Native ptah rehearses only a run
// that applies and has no check of the dev server before it, so its refusal is
// the rehearsal's claim of the dev server. Nothing is left on either server.
func TestSchemaApplyRefusesOnADevServerE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := devDiffServer(c, engine.admin)
			app, more := server.names["app"], server.names["more"]
			realm := devDiffRealm(c, app, more)
			dev := dbtarget.URL(c, engine.dev)

			sameOut, sameErr := runCompatVerb("schema", "apply", "--url", server.url, "--to", "file://"+realm,
				"--dev-url", server.url, "--dry-run")
			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", server.url,
				"--schema-file", realm, "--dev-url", server.url, "--auto-approve")
			sqlOut, sqlErr := runCompatVerb("schema", "apply", "--url", server.url+app,
				"--to", "file://"+devApplyDatabaseSQL(c, app), "--dev-url", dev, "--dry-run")

			c.Assert(sameErr, qt.ErrorMatches, `(?s).*connected database is not clean: found schema .*`,
				qt.Commentf("%s", sameOut))
			c.Assert(nativeErr, qt.ErrorMatches, `(?s).*connected database is not clean: found schema .*`,
				qt.Commentf("%s", native))
			c.Assert(sqlErr, qt.ErrorMatches,
				`(?s).*cannot diff a database connection with a schema "`+app+`".*`, qt.Commentf("%s", sqlOut))
			c.Assert(databasesOn(c, engine.admin, more), qt.Equals, 0)
			c.Assert(databasesOn(c, engine.dev, app, more), qt.Equals, 0)
		})
	}
}

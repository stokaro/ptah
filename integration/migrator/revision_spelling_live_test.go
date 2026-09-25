//go:build integration

package migrator_test

import (
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// These tests run the statements a *migrator.RevisionSpellingError carries on
// the engine they were rendered for. The refusal is decided in Ptah and has
// its unit tests; whether its remedy is SQL the server accepts is not, and
// ClickHouse, which keeps the version in the primary key, refuses the UPDATE
// every other engine takes.

// respellFixture is one engine's spelling of the fixture: the DDL that creates
// and drops a table, and the revision table the history is kept in, empty for
// the layout's default placement.
type respellFixture struct {
	createTable   string
	dropTable     string
	revisionTable string
	suffix        string
}

func respellDirectory(f respellFixture, versions [3]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for index, name := range []string{"a", "b", "c"} {
		table := "spelled_" + name + "_" + f.suffix
		base := versions[index] + "_" + name
		fsys[base+".up.sql"] = &fstest.MapFile{Data: []byte(fmt.Sprintf(f.createTable, table) + ";\n")}
		fsys[base+".down.sql"] = &fstest.MapFile{Data: []byte(fmt.Sprintf(f.dropTable, table) + ";\n")}
	}
	return fsys
}

func newRespellMigrator(c *qt.C, conn *dbschema.DatabaseConnection, f respellFixture, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).WithMigrationsTable("", f.revisionTable)
}

func respellVersions(c *qt.C, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT version FROM "+table+" ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// respellRoundTrip writes the history 1, 2, 10, meets it with the directory
// 001, 002, 010, and checks that a rollback is refused before it runs any down
// migration. It then runs the statements the refusal carries and checks that
// the history reads as applied and rolls back one step, row included.
func respellRoundTrip(c *qt.C, conn *dbschema.DatabaseConnection, f respellFixture) {
	c.Helper()
	legacy := newRespellMigrator(c, conn, f, respellDirectory(f, [3]string{"1", "2", "10"}))
	c.Assert(legacy.MigrateUp(c.Context()), qt.IsNil)
	m := newRespellMigrator(c, conn, f, respellDirectory(f, [3]string{"001", "002", "010"}))
	lastTable := "spelled_c_" + f.suffix

	var spelling *migrator.RevisionSpellingError
	c.Assert(m.MigrateDown(c.Context()), qt.ErrorAs, &spelling)
	c.Assert(spelling.Rows, qt.DeepEquals, []migrator.RevisionSpelling{
		{Version: 1, Recorded: "1", File: "001"},
		{Version: 2, Recorded: "2", File: "002"},
		{Version: 10, Recorded: "10", File: "010"},
	})
	_, err := conn.ExecContext(c.Context(), "SELECT count(*) FROM "+lastTable)
	c.Assert(err, qt.IsNil, qt.Commentf("the refused rollback dropped %s", lastTable))

	for _, statement := range spelling.Statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	c.Assert(respellVersions(c, conn, spelling.Table), qt.DeepEquals, []string{"001", "002", "010"})

	status, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.MissingMigrations, qt.HasLen, 0)
	c.Assert(m.MigrateDown(c.Context()), qt.IsNil)
	c.Assert(respellVersions(c, conn, spelling.Table), qt.DeepEquals, []string{"001", "002"})
	_, err = conn.ExecContext(c.Context(), "SELECT count(*) FROM "+lastTable)
	c.Assert(err, qt.IsNotNil)
}

// dropRespellTables removes what a run against a shared database left: the
// three fixture tables and the revision table.
func dropRespellTables(c *qt.C, conn *dbschema.DatabaseConnection, f respellFixture, dropIfExists string) {
	c.Helper()
	for _, table := range []string{"spelled_a_", "spelled_b_", "spelled_c_"} {
		_, err := conn.ExecContext(c.Context(), fmt.Sprintf(dropIfExists, table+f.suffix))
		c.Check(err, qt.IsNil)
	}
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(dropIfExists, f.revisionTable))
	c.Check(err, qt.IsNil)
}

// TestRevisionSpelling_PostgreSQLRespellsInPlace runs through a URL that pins
// no search_path, so the history sits in the atlas_schema_revisions schema and
// the statements have to name it.
func TestRevisionSpelling_PostgreSQLRespellsInPlace(t *testing.T) {
	c := qt.New(t)
	conn := connectPlacement(c, newPlacementDatabase(c, ""))
	respellRoundTrip(c, conn, respellFixture{
		createTable: "CREATE TABLE %s (id integer PRIMARY KEY)",
		dropTable:   "DROP TABLE %s",
		suffix:      "pg",
	})
}

func TestRevisionSpelling_MySQLRespellsInPlace(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	suffix := fmt.Sprint(time.Now().UnixNano())
	f := respellFixture{
		createTable:   "CREATE TABLE %s (id integer PRIMARY KEY)",
		dropTable:     "DROP TABLE %s",
		revisionTable: "respell_revisions_" + suffix,
		suffix:        suffix,
	}
	defer dropRespellTables(c, conn, f, "DROP TABLE IF EXISTS %s")
	respellRoundTrip(c, conn, f)
}

// TestRevisionSpelling_ClickHouseReinsertsTheRow covers the engine the UPDATE
// does not reach: the version is the table's primary key, so the refusal
// carries an INSERT of the respelled row and a DELETE of the old one.
func TestRevisionSpelling_ClickHouseReinsertsTheRow(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(t, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	suffix := fmt.Sprint(time.Now().UnixNano())
	f := respellFixture{
		createTable:   "CREATE TABLE %s (id Int32) ENGINE = MergeTree ORDER BY id",
		dropTable:     "DROP TABLE %s SYNC",
		revisionTable: "respell_revisions_" + suffix,
		suffix:        suffix,
	}
	defer dropRespellTables(c, conn, f, "DROP TABLE IF EXISTS %s SYNC")
	respellRoundTrip(c, conn, f)
}

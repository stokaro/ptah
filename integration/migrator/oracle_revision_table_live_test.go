//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// TestOracleMigratorAppliesAndRollsBackLive applies two migrations to Oracle
// and rolls the second one back, in both revision table formats.
//
// The migration bodies are valid Oracle and are not what this measures. The
// revision table is: without its Oracle spelling, the server refuses Ptah's own
// bookkeeping DDL before any body runs. Measured on Oracle Free 23.26.3.0.0:
//
//	ptah:  ORA-03076: unexpected item DEFAULT in a column definition or inline constraint
//	atlas: ORA-03062: missing comma or right parenthesis
//
// The rollback runs on a second migrator, so Initialize meets a revision table
// that already exists, which is the path every run after the first one takes
// (stokaro/ptah#3298).
func TestOracleMigratorAppliesAndRollsBackLive(t *testing.T) {
	tests := []struct {
		name   string
		format migrator.RevisionTableFormat
		table  string
	}{
		{name: "ptah revision table", format: migrator.RevisionTableFormatPtah, table: "ptah_3298_revisions"},
		{name: "atlas revision table", format: migrator.RevisionTableFormatAtlas, table: "ptah_3298_atlas_revisions"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := connectOracle3298(c)
			dropOracle3298Tables(c, conn, "ptah_3298_widgets", "ptah_3298_gadgets", `"`+test.table+`"`)

			applier := newOracle3298Migrator(c, conn, oracle3298Migrations(), test.format, test.table)
			c.Assert(applier.MigrateUp(ctx), qt.IsNil)
			c.Assert(oracle3298TableCount(c, conn, "PTAH_3298_WIDGETS"), qt.Equals, 1)
			c.Assert(oracle3298TableCount(c, conn, "PTAH_3298_GADGETS"), qt.Equals, 1)
			applied, err := applier.GetAppliedMigrations(ctx)
			c.Assert(err, qt.IsNil)
			c.Assert(applied, qt.DeepEquals, []int64{1, 2})

			roller := newOracle3298Migrator(c, conn, oracle3298Migrations(), test.format, test.table)
			c.Assert(roller.MigrateDown(ctx), qt.IsNil)
			c.Assert(oracle3298TableCount(c, conn, "PTAH_3298_WIDGETS"), qt.Equals, 1)
			c.Assert(oracle3298TableCount(c, conn, "PTAH_3298_GADGETS"), qt.Equals, 0)
			applied, err = roller.GetAppliedMigrations(ctx)
			c.Assert(err, qt.IsNil)
			c.Assert(applied, qt.DeepEquals, []int64{1})
		})
	}
}

// TestOracleMigratorKeepsEveryStatementItRanLive pins what a failed body leaves
// behind on Oracle, which is the DDL transaction class internal/ddltx assigns.
//
// The third statement fails. The INSERT before it is still there afterwards,
// and the revision row says two statements were applied, so a retry resumes at
// the statement that failed. That is the NoTransaction contract: the Oracle
// writer opens no transaction, so each statement commits as it runs. Under the
// MySQL family's ImplicitCommit contract the INSERT would be rolled back with
// the file's transaction, and the migrator proves that through an InnoDB and
// sql_mode preflight Oracle cannot answer (stokaro/ptah#3298).
func TestOracleMigratorKeepsEveryStatementItRanLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := connectOracle3298(c)
	const table = "ptah_3298_durable_revisions"
	dropOracle3298Tables(c, conn, "ptah_3298_durable", `"`+table+`"`)
	fsys := fstest.MapFS{
		"0000000001_durable.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE ptah_3298_durable (id NUMBER(10) PRIMARY KEY);\n" +
				"INSERT INTO ptah_3298_durable (id) VALUES (1);\n" +
				"INSERT INTO ptah_3298_durable (id) VALUES (1);\n",
		)},
		"0000000001_durable.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE ptah_3298_durable PURGE;\n")},
	}
	mig := newOracle3298Migrator(c, conn, fsys, migrator.RevisionTableFormatPtah, table)

	err := mig.MigrateUp(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s).*ORA-00001.*`)
	var rows int
	c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM ptah_3298_durable").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
	revisions, err := mig.GetRevisions(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[0].Applied, qt.Equals, 2)
	c.Assert(revisions[0].Total, qt.Equals, 3)
	c.Assert(revisions[0].Dirty, qt.IsTrue)
}

func oracle3298Migrations() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_widgets.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE ptah_3298_widgets (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50) NOT NULL);\n",
		)},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE ptah_3298_widgets PURGE;\n")},
		"0000000002_gadgets.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE ptah_3298_gadgets (id NUMBER(10) PRIMARY KEY);\n",
		)},
		"0000000002_gadgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE ptah_3298_gadgets PURGE;\n")},
	}
}

func connectOracle3298(c *qt.C) *dbschema.DatabaseConnection {
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func newOracle3298Migrator(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
	format migrator.RevisionTableFormat,
	table string,
) *migrator.Migrator {
	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(format).WithMigrationsTable("", table)
}

// oracle3298TableCount counts the tables the connected account owns under one
// catalog name, which for an unquoted identifier is its upper-case fold.
func oracle3298TableCount(c *qt.C, conn *dbschema.DatabaseConnection, name string) int {
	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM user_tables WHERE table_name = :1", name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// dropOracle3298Tables removes the named tables now and again when the test
// ends, because the server is shared. A table that is not there answers
// ORA-00942, which is the state this wants, so the error is not read.
func dropOracle3298Tables(c *qt.C, conn *dbschema.DatabaseConnection, tables ...string) {
	drop := func() {
		for _, table := range tables {
			_, _ = conn.ExecContext(context.Background(), "DROP TABLE "+table+" PURGE")
		}
	}
	drop()
	c.Cleanup(drop)
}

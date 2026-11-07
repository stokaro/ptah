//go:build integration

package planreport_test

import (
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // the driver the MySQL family's read back opens with

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

const (
	coveringTable = "ptah_planreport_covering"
	coveringQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '" + coveringTable + "'"
	// coveringSchema declares a table the MySQL family cannot host. The refusal
	// is the selected validator's.
	coveringSchema = "CREATE TABLE " + coveringTable + " (id INT PRIMARY KEY, a INT, b INT, UNIQUE (a) INCLUDE (b));\n"
	// reservedRoleSchema declares a role PostgreSQL reserves. The refusal is the
	// comparison's own check of the declaration.
	reservedRoleSchema = "CREATE ROLE pg_monitor;\nCREATE TABLE docs (id integer PRIMARY KEY);\n"
	tablesNamedDocs    = "SELECT count(*)::text FROM pg_class WHERE relname = 'docs'"
)

// mysqlFamilyTarget returns the address ptah connects with and a connection to
// read the server back through. A refused plan changes nothing, so the shared
// test database needs no copy.
func mysqlFamilyTarget(c *qt.C, engine dbtarget.Engine) (string, *sql.DB) {
	c.Helper()
	db, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return dbtarget.URL(c, engine), db
}

// assertDeclarationRefused holds one document to the refusal and to the plan file
// it does not save.
func assertDeclarationRefused(c *qt.C, result clirun.Result, got document, command verb, output, wantMessage string) {
	c.Helper()
	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(got.ContractVersion, qt.Equals, command.contract)
	c.Assert(got.Outcome, qt.Equals, "refused")
	c.Assert(got.Refusal, qt.IsNotNil)
	c.Assert(got.Refusal.Code, qt.Equals, "declaration-refused")
	c.Assert(got.Error, qt.IsNotNil)
	c.Assert(got.Error.Message, qt.Matches, wantMessage)
	_, err := os.Stat(output)
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestDeclarationRefusalReportE2E_SelectedValidator runs a real plan of a covering
// UNIQUE constraint on the MySQL family, through both commands. The server is read
// back to show the table is not there.
func TestDeclarationRefusalReportE2E_SelectedValidator(t *testing.T) {
	tests := []struct {
		name        string
		engine      dbtarget.Engine
		wantMessage string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQL,
			wantMessage: `compare database schema: mysql does not support INCLUDE columns on UNIQUE constraint ` +
				`"a"; target postgres, yugabytedb, or cockroachdb`,
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDB,
			wantMessage: `compare database schema: mariadb does not support INCLUDE columns on UNIQUE constraint ` +
				`"a"; target postgres, yugabytedb, or cockroachdb`,
		},
	}
	for _, test := range tests {
		for _, command := range verbs {
			t.Run(test.name+"/"+command.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := mysqlFamilyTarget(c, test.engine)

				result, got, output := run(c, command, url, coveringSchema)

				assertDeclarationRefused(c, result, got, command, output, test.wantMessage)
				c.Assert(readBack(c, db, coveringQuery), qt.Equals, "0")
			})
		}
	}
}

// TestDeclarationRefusalReportE2E_ComparisonCheck runs a real plan of a reserved
// role on PostgreSQL, through both commands. The server is read back to show that
// the table the same schema declares is not there.
func TestDeclarationRefusalReportE2E_ComparisonCheck(t *testing.T) {
	for _, command := range verbs {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			url, db := database(c)

			result, got, output := run(c, command, url, reservedRoleSchema)

			assertDeclarationRefused(c, result, got, command, output,
				`(?s)compare database schema: invalid schema diff: desired schema declares reserved PostgreSQL role "pg_monitor" .*`)
			c.Assert(readBack(c, db, tablesNamedDocs), qt.Equals, "0")
		})
	}
}

// TestDeclarationRefusalReportE2E_NoCodeWithoutTheRead holds what the code promises
// a caller: the target was read. The same refusal over a target the read refuses
// reports the read, and a schema refused while it is parsed names no code either.
func TestDeclarationRefusalReportE2E_NoCodeWithoutTheRead(t *testing.T) {
	const parsedTwice = "CREATE TABLE docs (id integer PRIMARY KEY);\nCREATE TABLE docs2 (id integer PRIMARY KEY);\n" +
		"CREATE INDEX same ON docs (id);\nCREATE INDEX same ON docs2 (id);\n"
	for _, command := range verbs {
		t.Run("the read refuses the target/"+command.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "unreadable.db")
			seedUnreadableSQLite(c, path)

			result, got, _ := run(c, command, "sqlite://"+path,
				"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT, UNIQUE (a) INCLUDE (b));\n")

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Outcome, qt.Equals, "failed")
			c.Assert(got.Refusal, qt.IsNil)
			c.Assert(got.Error.Message, qt.Matches, `(?s)read database schema: .*`)
		})
		t.Run("the schema is refused as it is parsed/"+command.name, func(t *testing.T) {
			c := qt.New(t)
			url, db := database(c)

			result, got, _ := run(c, command, url, parsedTwice)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Outcome, qt.Equals, "failed")
			c.Assert(got.Refusal, qt.IsNil)
			c.Assert(got.Error.Message, qt.Matches, `(?s)error parsing schema file: .*`)
			c.Assert(readBack(c, db, tablesNamedDocs), qt.Equals, "0")
		})
	}
}

// seedUnreadableSQLite leaves a database at path that connects and that the schema
// reader refuses: an index on a virtual table's shadow storage.
func seedUnreadableSQLite(c *qt.C, path string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range []string{
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
		`CREATE INDEX my_idx ON docs_content(c0)`,
	} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

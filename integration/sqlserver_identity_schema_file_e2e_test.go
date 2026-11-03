//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// `ptah db read` describes a SQL Server identity column with T-SQL's IDENTITY
// property, `[id] INT IDENTITY(1,1) PRIMARY KEY`. Read back as a schema file,
// the description was refused with `unsupported column attribute: IDENTITY`,
// so the description of nearly any SQL Server database could be neither
// planned against that database nor applied to another one.

// sqlServerIdentityFixture is a table with an identity primary key, one with
// a seed and increment of its own written NOT NULL, and one with negative
// values written without NOT NULL, which the server records as NOT NULL all
// the same.
var sqlServerIdentityFixture = []string{
	"CREATE TABLE a (id int IDENTITY(1,1) PRIMARY KEY, msg nvarchar(10))",
	"CREATE TABLE b (id bigint IDENTITY(100,5) NOT NULL, x int)",
	"CREATE TABLE c (id int IDENTITY(-10,-2), x int)",
}

// sqlServerIdentityScratchDatabase is an empty database on the SQL Server the
// suite names, dropped when the test ends.
func sqlServerIdentityScratchDatabase(c *qt.C) string {
	c.Helper()
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer), drop, replaceSQLServerDatabase)
}

// sqlServerIdentityFacts reads each identity column's table, seed, increment
// and nullability from the catalog, ordered by table.
func sqlServerIdentityFacts(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT OBJECT_NAME(ic.object_id) + '.' + ic.name + ' ' +
			CAST(ic.seed_value AS varchar(40)) + ' ' + CAST(ic.increment_value AS varchar(40)) + ' ' +
			CASE WHEN ic.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END
		FROM sys.identity_columns ic
		JOIN sys.tables t ON t.object_id = ic.object_id
		WHERE t.is_ms_shipped = 0
		ORDER BY OBJECT_NAME(ic.object_id) COLLATE Latin1_General_BIN`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var facts []string
	for rows.Next() {
		var fact string
		c.Assert(rows.Scan(&fact), qt.IsNil)
		facts = append(facts, fact)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return facts
}

// TestSQLServerDescribedIdentityRoundTripE2E describes a database with `ptah
// db read`, plans the description against the database it came from, and
// applies it to an empty database with `ptah schema apply`. The plan is empty,
// and both catalogs record the same seed, increment and nullability for every
// identity column.
func TestSQLServerDescribedIdentityRoundTripE2E(t *testing.T) {
	c := qt.New(t)
	source := sqlServerIdentityScratchDatabase(c)
	target := sqlServerIdentityScratchDatabase(c)
	db, err := sql.Open("sqlserver", source)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range sqlServerIdentityFixture {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	read := clirun.Run(c, clirun.Ptah, clirun.Options{}, "db", "read", "--db-url", source)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "[id] BIGINT IDENTITY(100,5) NOT NULL")
	schema := filepath.Join(c.TempDir(), "described.sql")
	c.Assert(os.WriteFile(schema, []byte(read.Stdout), 0o600), qt.IsNil)

	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", source, "--schema-file", schema)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(plan.ExitCode, qt.Equals, 0,
		qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	want := sqlServerIdentityFacts(c, source)
	c.Assert(want, qt.DeepEquals, []string{"a.id 1 1 NOT NULL", "b.id 100 5 NOT NULL", "c.id -10 -2 NOT NULL"})
	c.Assert(sqlServerIdentityFacts(c, target), qt.DeepEquals, want)
}

// sqlServerIdentitySchemaFile writes IDENTITY in the spellings a person
// writes: bare, with signed values, and without NOT NULL.
const sqlServerIdentitySchemaFile = `CREATE TABLE a (id int IDENTITY PRIMARY KEY, msg nvarchar(10));
CREATE TABLE b (id bigint IDENTITY(+100, 5), x int);
CREATE TABLE c (id int IDENTITY ( -10 , -2 ), x int);
`

// TestSQLServerSchemaApplyCreatesIdentityE2E applies a schema file that writes
// IDENTITY by hand to an empty database, reads each identity column back from
// sys.identity_columns, and plans the file again. An identity column written
// without NOT NULL is NOT NULL on the server, and read as nullable it was
// planned as `ALTER COLUMN ... NULL`, which SQL Server refuses with Msg 8147.
func TestSQLServerSchemaApplyCreatesIdentityE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerIdentityScratchDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerIdentitySchemaFile), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", target, "--schema-file", schema)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerIdentityFacts(c, target), qt.DeepEquals,
		[]string{"a.id 1 1 NOT NULL", "b.id 100 5 NOT NULL", "c.id -10 -2 NOT NULL"})
	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
}

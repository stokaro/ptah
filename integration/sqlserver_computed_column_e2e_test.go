//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// sqlServerComputedModels declares a computed column of each kind a Go
// annotation states. d_none names no kind, which SQL Server stores; d_stored
// writes stored bare and d_kind generated_kind="STORED", both stored too;
// d_virtual and d_case are computed on read. The type and nullability SQL
// Server derives from an expression differ from two declarations: d_none is
// declared BIGINT and is an int column, and d_case is declared NVARCHAR(1)
// and nullable and is VARCHAR(1) NOT NULL.
const sqlServerComputedModels = `package models

//ptah:schema:table name="totals"
type Total struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
	//ptah:schema:field name="amount" type="INT" not_null
	Amount int
	//ptah:schema:field name="s" type="NVARCHAR(20)"
	S string
	//ptah:schema:field name="d_none" type="BIGINT" generated="amount * 2"
	DNone int
	//ptah:schema:field name="d_stored" type="INT" generated="AMOUNT_STORED" stored
	DStored int
	//ptah:schema:field name="d_virtual" type="INT" generated="amount - 1" stored="VIRTUAL_STORED"
	DVirtual int
	//ptah:schema:field name="d_kind" type="NVARCHAR(20)" generated="upper(s)" generated_kind="STORED"
	DKind string
	//ptah:schema:field name="d_case" type="NVARCHAR(1)" generated="CASE WHEN amount > 0 THEN 'p' ELSE 'n' END" generated_kind="VIRTUAL"
	DCase string
}
`

// sqlServerComputedColumnsWant is what sys.computed_columns holds for
// sqlServerComputedModels: each column's name, is_persisted and the
// definition SQL Server stores, rewritten from the declaration.
var sqlServerComputedColumnsWant = []string{
	"d_none 1 ([amount]*(2))",
	"d_stored 1 ([amount]+(1))",
	"d_virtual 0 ([amount]-(1))",
	"d_kind 1 (upper([s]))",
	"d_case 0 (case when [amount]>(0) then 'p' else 'n' end)",
}

// sqlServerComputedScratchDatabase is an empty database on the SQL Server the
// suite names, dropped when the test ends.
func sqlServerComputedScratchDatabase(c *qt.C) string {
	c.Helper()
	const drop = "ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s"
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer), drop, replaceSQLServerDatabase)
}

// writeSQLServerComputedModels writes sqlServerComputedModels with the stored
// expression of d_stored and the stored value of d_virtual, and returns the
// directory.
func writeSQLServerComputedModels(c *qt.C, storedExpression, virtualStored string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "models")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	source := strings.NewReplacer("AMOUNT_STORED", storedExpression, "VIRTUAL_STORED", virtualStored).Replace(sqlServerComputedModels)
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// sqlServerComputedColumns reads each computed column of every user table as
// its name, is_persisted and stored definition, in table and column order.
func sqlServerComputedColumns(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT cc.name + ' ' + CAST(cc.is_persisted AS varchar(1)) + ' ' + cc.definition
		FROM sys.computed_columns cc
		JOIN sys.tables t ON t.object_id = cc.object_id
		WHERE t.is_ms_shipped = 0
		ORDER BY t.name, cc.column_id`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var columns []string
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// TestSQLServerComputedColumnsConvergeE2E generates a migration for a computed
// column of each kind, runs it, reads is_persisted back, and generates again,
// which writes nothing.
//
// A column declared STORED is PERSISTED, and one declared VIRTUAL is computed
// on read. The next generate compares each kind as the server reports it, each
// expression in the form SQL Server stores, `([amount]+(1))` for `amount + 1`,
// and leaves the type and nullability SQL Server derives uncompared. Without
// any of the three it plans a change SQL Server cannot make, and the generate
// fails with `unsupported changes: generated`, or writes an ALTER COLUMN on a
// computed column, which SQL Server refuses with Msg 4928.
func TestSQLServerComputedColumnsConvergeE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerComputedScratchDatabase(c)
	models := writeSQLServerComputedModels(c, "amount + 1", "false")
	migrations := filepath.Join(c.TempDir(), "migrations")

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "computed")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)
	again := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "again")

	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
	c.Assert(sqlServerComputedColumns(c, database), qt.DeepEquals, sqlServerComputedColumnsWant)
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Contains, "no migration files generated")
}

// TestSQLServerComputedColumnChangesAreStillReportedE2E is the control for the
// convergence above: a changed expression, and a column changed from computed
// on read to stored, are still changes. SQL Server plans neither, so the
// generate refuses each by name rather than writing nothing.
func TestSQLServerComputedColumnChangesAreStillReportedE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerComputedScratchDatabase(c)
	migrations := filepath.Join(c.TempDir(), "migrations")
	generated := runPtahIn(c, "migrations", "generate", "--db-url", database,
		"--root-dir", writeSQLServerComputedModels(c, "amount + 1", "false"), "--migrations-dir", migrations, "--name", "computed")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)
	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))

	expression := runPtahIn(c, "migrations", "generate", "--db-url", database,
		"--root-dir", writeSQLServerComputedModels(c, "amount + 2", "false"), "--migrations-dir", migrations, "--name", "expression")
	kind := runPtahIn(c, "migrations", "generate", "--db-url", database,
		"--root-dir", writeSQLServerComputedModels(c, "amount + 1", "true"), "--migrations-dir", migrations, "--name", "kind")

	c.Assert(expression.ExitCode, qt.Not(qt.Equals), 0, historyComment(expression))
	c.Assert(expression.Stderr, qt.Contains, "totals.d_stored; unsupported changes: generated")
	c.Assert(kind.ExitCode, qt.Not(qt.Equals), 0, historyComment(kind))
	c.Assert(kind.Stderr, qt.Contains, "totals.d_virtual; unsupported changes: generated")
}

// sqlServerComputedFixture is a table with a computed column of each kind,
// written as T-SQL.
var sqlServerComputedFixture = []string{
	"CREATE TABLE totals (id int PRIMARY KEY, amount int NOT NULL, " +
		"d_stored AS (amount + 1) PERSISTED, d_virtual AS (amount - 1))",
}

// TestSQLServerDescribedComputedColumnsRoundTripE2E describes a database with
// `ptah db read` and with `ptah-compat schema inspect`, plans each
// description against the database it came from, and applies it to an empty
// database. `db read` writes a column computed on read as `AS (expr)` and a
// stored one as `AS (expr) PERSISTED`; `schema inspect` writes the kind in
// the `as` block. Each description reads back as the same two kinds: the plan
// is empty, and the copy holds the same columns. A description that carries
// no kind for d_virtual reads as the target's default, PERSISTED, and the
// plan refuses the kind change.
func TestSQLServerDescribedComputedColumnsRoundTripE2E(t *testing.T) {
	tests := []struct {
		name        string
		target      clirun.Target
		describe    []string
		file        string
		wantStored  string
		wantVirtual string
	}{
		{
			name: "db read", target: clirun.Ptah, describe: []string{"db", "read", "--db-url"}, file: "described.sql",
			wantStored:  "[d_stored] AS (([amount]+(1))) PERSISTED,",
			wantVirtual: "[d_virtual] AS (([amount]-(1)))\n",
		},
		{
			name: "schema inspect", target: clirun.Compat, describe: []string{"schema", "inspect", "-u"}, file: "described.hcl",
			wantStored:  "expr = \"([amount]+(1))\"\n      type = \"PERSISTED\"\n",
			wantVirtual: "expr = \"([amount]-(1))\"\n      type = \"VIRTUAL\"\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := sqlServerComputedScratchDatabase(c)
			target := sqlServerComputedScratchDatabase(c)
			createSQLServerComputedFixture(c, source)
			described := clirun.Run(c, test.target, clirun.Options{}, append(test.describe, source)...)
			c.Assert(described.ExitCode, qt.Equals, 0, historyComment(described))
			schema := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(schema, []byte(described.Stdout), 0o600), qt.IsNil)

			plan := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "plan", "--db-url", source, "--schema-file", schema)
			applied := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(described.Stdout, qt.Contains, test.wantStored)
			c.Assert(described.Stdout, qt.Contains, test.wantVirtual)
			c.Assert(plan.ExitCode, qt.Equals, 0, historyComment(plan))
			c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements")
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			want := []string{"d_stored 1 ([amount]+(1))", "d_virtual 0 ([amount]-(1))"}
			c.Assert(sqlServerComputedColumns(c, source), qt.DeepEquals, want)
			c.Assert(sqlServerComputedColumns(c, target), qt.DeepEquals, want)
		})
	}
}

// createSQLServerComputedFixture runs sqlServerComputedFixture on the
// database rawURL names.
func createSQLServerComputedFixture(c *qt.C, rawURL string) {
	c.Helper()
	db, err := sql.Open("sqlserver", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range sqlServerComputedFixture {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// TestSQLServerStoredNonDeterministicColumnIsRefusedE2E runs a migration
// whose stored computed column reads GETDATE(). SQL Server persists only a
// deterministic expression, and it alone decides which expressions are: it
// refuses the CREATE TABLE with Msg 4936. The run fails with that refusal and
// leaves no table behind.
func TestSQLServerStoredNonDeterministicColumnIsRefusedE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerComputedScratchDatabase(c)
	models := filepath.Join(c.TempDir(), "models")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(`package models

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
	//ptah:schema:field name="seen" type="DATETIME" generated="GETDATE()" stored
	Seen int
}
`), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "events")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)

	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Not(qt.Equals), 0, historyComment(up))
	c.Assert(up.Stderr, qt.Contains,
		"Computed column 'seen' in table 'events' cannot be persisted because the column is non-deterministic. (4936)")
	c.Assert(catalogReadBack(c, database, []string{
		"SELECT name FROM sys.tables WHERE name = 'events'",
	}), qt.DeepEquals, [][]string{{}})
}

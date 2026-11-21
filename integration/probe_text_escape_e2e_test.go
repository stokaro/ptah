//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// probeEscapeTable is the source of a table whose column amount carries
// ATTRIBUTES, the declaration whose text the probe writes into a statement.
const probeEscapeTable = `package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
	//ptah:schema:field name="amount" type="INT" not_null ATTRIBUTES
	Amount int
}
`

// probeEscapeView is the source of a table and of a view whose body is
// BODY.
const probeEscapeView = `package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
}

//ptah:schema:view name="v" body="BODY"
type V struct{}
`

// probeEscapeTarget is a scratch database of one engine: the URL ptah takes,
// and the driver and address a test reads the database back through.
type probeEscapeTarget struct {
	url, driver, driverURL string
}

// probeEscapeTargets creates a scratch database on each engine whose probes
// write declared text into a statement, dropped when the test ends.
var probeEscapeTargets = map[dbtarget.Engine]func(c *qt.C) probeEscapeTarget{
	dbtarget.SQLServer: func(c *qt.C) probeEscapeTarget {
		database := sqlServerComputedScratchDatabase(c)
		return probeEscapeTarget{url: database, driver: "sqlserver", driverURL: database}
	},
	dbtarget.PostgreSQL:  postgresFamilyProbeEscapeTarget(dbtarget.PostgreSQL, "DROP DATABASE IF EXISTS %s WITH (FORCE)"),
	dbtarget.CockroachDB: postgresFamilyProbeEscapeTarget(dbtarget.CockroachDB, "DROP DATABASE IF EXISTS %s CASCADE"),
	dbtarget.YugabyteDB:  postgresFamilyProbeEscapeTarget(dbtarget.YugabyteDB, "DROP DATABASE IF EXISTS %s"),
}

func postgresFamilyProbeEscapeTarget(engine dbtarget.Engine, drop string) func(c *qt.C) probeEscapeTarget {
	return func(c *qt.C) probeEscapeTarget {
		c.Helper()
		database := createdDevDialectDatabase(c, dbtarget.URL(c, engine), drop, renameInPath)
		return probeEscapeTarget{url: database, driver: "pgx", driverURL: postgresFamilyDriverURL(c, database)}
	}
}

// execProbeEscapeSQL runs each statement on target, one at a time.
func execProbeEscapeSQL(c *qt.C, target probeEscapeTarget, statements []string) {
	c.Helper()
	db, err := sql.Open(target.driver, target.driverURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// probeEscapeTablesMade counts the tables named made_by_plan on target, the
// table each declaration below tries to create.
func probeEscapeTablesMade(c *qt.C, target probeEscapeTarget) int {
	c.Helper()
	db, err := sql.Open(target.driver, target.driverURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var made int
	c.Assert(db.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'made_by_plan'").Scan(&made), qt.IsNil)
	return made
}

// TestPlanDoesNotRunDeclaredTextThatLeavesItsProbeE2E plans a declaration
// whose text closes the parentheses the probe writes it in, or ends the
// statement, and then commits a table of its own. Without the check the plan
// prints its migration and leaves made_by_plan behind on every engine here:
// the probe's transaction is rolled back, but the COMMIT in the text ended it
// first. The declaration is compared as written instead, and the plan says
// so.
//
// Each database holds the object the declaration is compared with, so the
// probe is asked; a probe that is never asked would pass this test without
// the check.
func TestPlanDoesNotRunDeclaredTextThatLeavesItsProbeE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		// live creates what the database holds.
		live []string
		// source is the annotated Go source the plan reads.
		source string
		// warning is the line the plan prints for the declaration.
		warning string
	}{
		{
			name:   "SQL Server computed column",
			engine: dbtarget.SQLServer,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL, g AS (amount + 1) PERSISTED)"},
			source: `package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
	//ptah:schema:field name="amount" type="INT" not_null
	Amount int
	//ptah:schema:field name="g" type="INT" generated="amount + 1)); COMMIT; CREATE TABLE dbo.made_by_plan (x int); BEGIN TRANSACTION; SAVE TRANSACTION ptah_generated_probe; SELECT ((1" stored
	G int
}
`,
			warning: "warning: generated column dbo.t.g is compared as written",
		},
		{
			name:   "SQL Server CHECK without a semicolon",
			engine: dbtarget.SQLServer,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source: probeEscapeSource(probeEscapeTable, "ATTRIBUTES",
				`check="amount > 0)) COMMIT CREATE TABLE dbo.made_by_plan (x int) BEGIN TRANSACTION SAVE TRANSACTION ptah_check_probe SELECT ((1"`),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			// A lexer that ends a block comment at the first */ reads the
			// parentheses as part of a string; SQL Server nests comments
			// and reads them as SQL.
			name:   "SQL Server CHECK behind a nested comment",
			engine: dbtarget.SQLServer,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source: probeEscapeSource(probeEscapeTable, "ATTRIBUTES",
				`check="amount > 0 /* /* */ ' */ )) COMMIT CREATE TABLE dbo.made_by_plan (x int) BEGIN TRANSACTION SAVE TRANSACTION ptah_check_probe SELECT ((1 /* ' */"`),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			name:   "PostgreSQL CHECK",
			engine: dbtarget.PostgreSQL,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source: probeEscapeSource(probeEscapeTable, "ATTRIBUTES",
				`check="amount > 0)); COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; SAVEPOINT ptah_check_probe; SELECT ((1"`),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			name:    "PostgreSQL view",
			engine:  dbtarget.PostgreSQL,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			// The server parses the statement before it runs; the lexer cannot
			// tell the body's semicolons from the ones after END.
			name:    "PostgreSQL SQL-standard routine body",
			engine:  dbtarget.PostgreSQL,
			live:    []string{"CREATE FUNCTION f() RETURNS integer LANGUAGE sql BEGIN ATOMIC SELECT 1; END"},
			source:  probeEscapeRoutine,
			warning: "warning: routine f is compared as written",
		},
		{
			name:    "CockroachDB view",
			engine:  dbtarget.CockroachDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			name:   "YugabyteDB CHECK",
			engine: dbtarget.YugabyteDB,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source: probeEscapeSource(probeEscapeTable, "ATTRIBUTES",
				`check="amount > 0)); COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; SAVEPOINT ptah_check_probe; SELECT ((1"`),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			name:    "YugabyteDB view",
			engine:  dbtarget.YugabyteDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeViewBody),
			warning: "warning: view v is compared as written",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			execProbeEscapeSQL(c, target, test.live)
			models := filepath.Join(c.TempDir(), "models")
			c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(test.source), 0o600), qt.IsNil)

			plan := runPtahIn(c, "migrations", "plan", "--db-url", target.url, "--root-dir", models)

			c.Assert(probeEscapeTablesMade(c, target), qt.Equals, 0, historyComment(plan))
			c.Assert(plan.Stderr, qt.Contains, test.warning, historyComment(plan))
		})
	}
}

// probeEscapeViewBody ends the view's statement with a semicolon and commits
// a table of its own.
//
// Each PostgreSQL-family text commits twice. PostgreSQL and CockroachDB run a
// statement after the first COMMIT in an implicit transaction, and the BEGIN
// that follows takes it into the transaction the probe rolls back; the second
// COMMIT ends it first.
const probeEscapeViewBody = "SELECT 1 AS x; COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; SAVEPOINT ptah_view_probe; SELECT 1 AS x"

// probeEscapeRoutine is the source of a function whose SQL-standard body ends
// at END and commits a table of its own after it.
const probeEscapeRoutine = `package models

//ptah:schema:function name="f" returns="integer" language="sql" body="BEGIN ATOMIC SELECT 1; END; COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; SAVEPOINT ptah_routine_probe; SELECT 1"
type F struct{}
`

// probeEscapeSource is source with placeholder replaced by value.
func probeEscapeSource(source, placeholder, value string) string {
	return strings.Replace(source, placeholder, value, 1)
}

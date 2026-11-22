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
		{
			name:    "PostgreSQL view behind a name outside ASCII",
			engine:  dbtarget.PostgreSQL,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeNonASCIIViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			name:    "CockroachDB view behind a name outside ASCII",
			engine:  dbtarget.CockroachDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeNonASCIIViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			name:    "YugabyteDB view behind a name outside ASCII",
			engine:  dbtarget.YugabyteDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeNonASCIIViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			name:    "PostgreSQL CHECK behind a name outside ASCII",
			engine:  dbtarget.PostgreSQL,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source:  probeEscapeSource(probeEscapeTable, "ATTRIBUTES", probeEscapeNonASCIICheck),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			name:    "YugabyteDB CHECK behind a name outside ASCII",
			engine:  dbtarget.YugabyteDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))"},
			source:  probeEscapeSource(probeEscapeTable, "ATTRIBUTES", probeEscapeNonASCIICheck),
			warning: "warning: CHECK t_amount_check on t is compared as written",
		},
		{
			// CockroachDB reads b'...' as a byte string, whose backslash
			// escapes the quote after it.
			name:   "CockroachDB view behind a byte string",
			engine: dbtarget.CockroachDB,
			live:   []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source: probeEscapeSource(probeEscapeView, "BODY",
				`SELECT b'\\'' AS x; COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; SAVEPOINT ptah_view_probe; SELECT 1 AS x --'`),
			warning: "warning: view v is compared as written",
		},
		{
			// CockroachDB continues an escape string on the next line, in
			// escape mode.
			name:    "CockroachDB view behind a continued string",
			engine:  dbtarget.CockroachDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeContinuedViewBody),
			warning: "warning: view v is compared as written",
		},
		{
			name:    "YugabyteDB view behind a continued string",
			engine:  dbtarget.YugabyteDB,
			live:    []string{"CREATE TABLE t (id INT PRIMARY KEY)", "CREATE VIEW v AS SELECT 1 AS x"},
			source:  probeEscapeSource(probeEscapeView, "BODY", probeEscapeContinuedViewBody),
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

// probeEscapeNonASCIIViewBody ends the view's statement where the
// PostgreSQL family does: its servers read every character outside ASCII as
// part of a name, so `x€$a$` is a name, and a lexer that ends the name at the
// euro sign reads a dollar quote from there to the end of the text.
const probeEscapeNonASCIIViewBody = "SELECT 1 AS x€$a$; COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; " +
	"SAVEPOINT ptah_view_probe; SELECT 1 AS y€$a$"

// probeEscapeNonASCIICheck is a CHECK that closes the probe's parentheses
// behind a name a lexer could end at the euro sign. It names no column, so it
// is named, to pair it with the constraint the database holds.
const probeEscapeNonASCIICheck = `check="xmlelement(name x€$a$) IS NOT NULL)); COMMIT; CREATE TABLE public.made_by_plan (x int); ` +
	`COMMIT; BEGIN; SAVEPOINT ptah_check_probe; SELECT ((xmlelement(name y€$a$) IS NOT NULL" check_name="t_amount_check"`

// probeEscapeContinuedViewBody ends the view's statement inside a string
// continued on the next line: CockroachDB and YugabyteDB read the second
// segment in the escape mode of the first, so `\'` is a quote and the next
// quote ends the string. The annotation spells the line break and the
// backslash with escapes.
const probeEscapeContinuedViewBody = `SELECT E'a'\n'\\'' AS x; COMMIT; CREATE TABLE public.made_by_plan (x int); COMMIT; BEGIN; ` +
	`SAVEPOINT ptah_view_probe; SELECT 1 AS x --'`

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

// probeBackslashTable is the source of a table whose CHECK compares with a
// string that ends in a backslash. The annotation spells the backslash twice,
// and the CHECK holds one.
const probeBackslashTable = `package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
	//ptah:schema:field name="code" type="TEXT" not_null
	Code string
	//ptah:schema:field name="amount" type="INT" not_null check="amount >= 0 AND code <> 'C:\\'" check_name="t_amount_check"
	Amount int
}
`

// TestPlanConvergesOnAStringThatEndsInABackslashE2E plans a CHECK the
// database already holds, whose string ends in a backslash. The probe
// transaction reads strings as standard, so the probe runs and the plan has
// nothing to do. Read as an escape as well, the backslash would leave the
// string open, the probe would not run, and the CHECK compared as written
// would be dropped and added again on every run. CockroachDB stores the CHECK
// as `(code != e'C:\\'::text)`, which only the probe's answer matches.
func TestPlanConvergesOnAStringThatEndsInABackslashE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			execProbeEscapeSQL(c, target, []string{
				`CREATE TABLE t (id INT PRIMARY KEY, code TEXT NOT NULL, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount >= 0 AND code <> 'C:\'))`,
			})
			models := filepath.Join(c.TempDir(), "models")
			c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(probeBackslashTable), 0o600), qt.IsNil)

			plan := runPtahIn(c, "migrations", "plan", "--db-url", target.url, "--root-dir", models)

			c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements", historyComment(plan))
			c.Assert(plan.Stderr, qt.Not(qt.Contains), "compared as written", historyComment(plan))
		})
	}
}

// TestPlanConvergesOnTextThatEndsInALineCommentE2E plans a view and a CHECK
// the database already holds, whose declared text ends in a line comment.
// Every probe statement writes a line break after the declared text, so the
// comment ends there, the probe runs, and the plan has nothing to do. Read as
// open to the end of the statement, the comment would keep the probe from
// running, and the object compared as written would be planned again on every
// run.
func TestPlanConvergesOnTextThatEndsInALineCommentE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
		live   []string
		source string
	}{
		{name: "PostgreSQL view", engine: dbtarget.PostgreSQL, live: probeCommentView, source: probeCommentViewSource},
		{name: "CockroachDB view", engine: dbtarget.CockroachDB, live: probeCommentView, source: probeCommentViewSource},
		{name: "YugabyteDB view", engine: dbtarget.YugabyteDB, live: probeCommentView, source: probeCommentViewSource},
		{name: "PostgreSQL CHECK", engine: dbtarget.PostgreSQL, live: probeCommentCheck, source: probeCommentCheckSource},
		{name: "CockroachDB CHECK", engine: dbtarget.CockroachDB, live: probeCommentCheck, source: probeCommentCheckSource},
		{name: "YugabyteDB CHECK", engine: dbtarget.YugabyteDB, live: probeCommentCheck, source: probeCommentCheckSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			execProbeEscapeSQL(c, target, test.live)
			models := filepath.Join(c.TempDir(), "models")
			c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(test.source), 0o600), qt.IsNil)

			plan := runPtahIn(c, "migrations", "plan", "--db-url", target.url, "--root-dir", models)

			c.Assert(plan.Stdout, qt.Contains, "Safety: no executable migration statements", historyComment(plan))
			c.Assert(plan.Stderr, qt.Not(qt.Contains), "compared as written", historyComment(plan))
		})
	}
}

// probeCommentView and probeCommentViewSource are a view whose body ends in
// a line comment, as the database holds it and as it is declared.
var probeCommentView = []string{
	"CREATE TABLE t (id INT PRIMARY KEY)",
	"CREATE VIEW v AS SELECT id FROM t WHERE id > 0 -- positive rows only\n",
}

const probeCommentViewSource = `package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INT" primary
	ID int
}

//ptah:schema:view name="v" body="SELECT id FROM t WHERE id > 0 -- positive rows only"
type V struct{}
`

// probeCommentCheck and probeCommentCheckSource are a CHECK whose declared
// text ends in a line comment, and the constraint the database holds for it.
var probeCommentCheck = []string{
	"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))",
}

var probeCommentCheckSource = probeEscapeSource(probeEscapeTable, "ATTRIBUTES",
	`check="amount > 0 -- must be positive" check_name="t_amount_check"`)

// probeBoundLive is a list-partitioned table on PostgreSQL. One partition's
// bound took a value of a sequence when the partition was created, and the
// server stores the value; the other's bound is a constant.
var probeBoundLive = []string{
	"CREATE SEQUENCE s",
	"CREATE TABLE ev (k bigint NOT NULL) PARTITION BY LIST (k)",
	"CREATE TABLE ev_1 PARTITION OF ev FOR VALUES IN (nextval('s'))",
	"CREATE TABLE ev_2 PARTITION OF ev FOR VALUES IN (2)",
}

// probeBoundDesired declares probeBoundLive: the first bound as written, and
// the second in a spelling only the server's normalization matches with the
// one it stores, `'2'`.
const probeBoundDesired = `CREATE SEQUENCE s;
CREATE TABLE ev (k bigint NOT NULL) PARTITION BY LIST (k);
CREATE TABLE ev_1 PARTITION OF ev FOR VALUES IN (nextval('s'));
CREATE TABLE ev_2 PARTITION OF ev FOR VALUES IN ('2'::bigint);
`

// probeBoundSequenceValue is the last value the sequence s gave.
func probeBoundSequenceValue(c *qt.C, target probeEscapeTarget) int64 {
	c.Helper()
	db, err := sql.Open(target.driver, target.driverURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var value int64
	c.Assert(db.QueryRowContext(c.Context(), "SELECT last_value FROM s").Scan(&value), qt.IsNil)
	return value
}

// TestPlanDoesNotEvaluateAPartitionBoundThatCallsAFunctionE2E plans a
// partition whose declared bound calls nextval. PostgreSQL evaluates a bound
// when it creates a partition, so the probe that spells the bound would take
// a value of the sequence, which no rollback returns. The bound is compared
// as written instead, the plan says so, and the sequence has given one value,
// the one the live partition took.
//
// The other partition's bound is a constant spelled with a cast. It plans
// nothing and warns about nothing, which it can only do through the probe:
// compared as written, `'2'::bigint` is not the `'2'` the server stores.
func TestPlanDoesNotEvaluateAPartitionBoundThatCallsAFunctionE2E(t *testing.T) {
	c := qt.New(t)
	target := probeEscapeTargets[dbtarget.PostgreSQL](c)
	execProbeEscapeSQL(c, target, probeBoundLive)
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(probeBoundDesired), 0o600), qt.IsNil)

	plan := runPtahIn(c, "schema", "plan", "--dry-run", "--schema-file", desired, "--db-url", target.url)

	c.Assert(probeBoundSequenceValue(c, target), qt.Equals, int64(1), historyComment(plan))
	c.Assert(plan.Stderr, qt.Contains, "warning: partition bound of public.ev_1 is compared as written", historyComment(plan))
	c.Assert(plan.Stderr, qt.Not(qt.Contains), "public.ev_2", historyComment(plan))
	c.Assert(plan.Stdout+plan.Stderr, qt.Contains, "No changes planned, but 1 comparison limit remains unresolved.", historyComment(plan))
}

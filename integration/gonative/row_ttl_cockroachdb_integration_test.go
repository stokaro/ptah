//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// The live half of stokaro/ptah#1027: a CockroachDB row-level TTL applied,
// read back, changed, and removed, with the comparison asked between every step.
//
// Every fact here is read back through the public surface -- the dbschema
// reader's description and the statements the comparison and planner produce
// from it -- rather than off the SQL the test itself sent. That distinction is
// the point: before this change a declared TTL was dropped silently at the
// renderer, so a run that sent nothing at all reported success, and a test
// asserting its own statements would have passed against that build too.
//
// The convergence assertions are the ones the issue turns on. `qt.HasLen, 0` on
// the second comparison is what a build that could not read the policy back
// could never satisfy: it would find the declared TTL missing every time and
// re-issue it forever.

const rowTTLTable = "ptah_1027_crdb_sessions"

func TestCockroachDBRowLevelTTL_RoundTripsLive(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)

	// CREATE. The table is created from the declaration itself, so the WITH
	// clause under test is the one the renderer produced.
	declared := rowTTLDeclaration(&ast.RowTTLSpec{
		ExpirationExpression: "expires_at",
		JobCron:              "@daily",
		SelectBatchSize:      new(int64(500)),
	})
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

	created := readRowTTL(c, t, dsn)
	c.Assert(created, qt.DeepEquals, &ast.RowTTLSpec{
		ExpirationExpression: "expires_at",
		JobCron:              "@daily",
		SelectBatchSize:      new(int64(500)),
	})

	// CONVERGE. The same declaration against the table it just created must
	// plan nothing at all.
	c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)

	// CHANGE. The expression carries a quote of its own, which the catalog
	// stores in its escape-string form -- the shape that made this worth
	// asserting live rather than only in a decoder test. One knob is dropped at
	// the same time, which `SET` alone would leave in place.
	changed := rowTTLDeclaration(&ast.RowTTLSpec{
		ExpirationExpression: "expires_at + INTERVAL '1 hour'",
		JobCron:              "@hourly",
	})
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, changed))

	c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals, &ast.RowTTLSpec{
		ExpirationExpression: "expires_at + INTERVAL '1 hour'",
		JobCron:              "@hourly",
	})
	c.Assert(planRowTTLAgainstLive(c, t, dsn, changed), qt.HasLen, 0)

	// REMOVE. The whole policy goes in one statement, and the table stays.
	removed := rowTTLDeclaration(nil)
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, removed))

	c.Assert(readRowTTL(c, t, dsn), qt.IsNil)
	c.Assert(planRowTTLAgainstLive(c, t, dsn, removed), qt.HasLen, 0)
	c.Assert(rowTTLTableExists(c, db), qt.IsTrue,
		qt.Commentf("removing a retention policy must not remove the table"))
}

// TestCockroachDBRowLevelTTL_ATableWithoutOneReadsAsHavingNone is the control
// on every assertion above.
//
// Without it, a reader that reported a policy for every table -- or one that
// reported none for every table -- would satisfy half the round trip. It also
// covers the ordinary case, which is every table on every CockroachDB database
// that does not use this feature.
func TestCockroachDBRowLevelTTL_ATableWithoutOneReadsAsHavingNone(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)

	_, err = db.Exec(`CREATE TABLE ` + rowTTLTable +
		` (id INT8 PRIMARY KEY, expires_at TIMESTAMPTZ)`)
	c.Assert(err, qt.IsNil)

	c.Assert(readRowTTL(c, t, dsn), qt.IsNil)
	c.Assert(planRowTTLAgainstLive(c, t, dsn, rowTTLDeclaration(nil)), qt.HasLen, 0)
}

// TestCockroachDBRowLevelTTL_IsReadBackVerbatim pins the round trip for the
// expression spellings the catalog could plausibly rewrite.
//
// Each one is applied, read back, and compared to zero difference. A server
// that normalized any of them would show up here as a plan that never empties,
// which is exactly how the two refused parameters were found.
func TestCockroachDBRowLevelTTL_IsReadBackVerbatim(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "a bare column", expression: "expires_at"},
		{name: "a parenthesized column", expression: "(expires_at)"},
		{name: "an upper-case column reference", expression: "EXPIRES_AT"},
		{name: "an explicit cast", expression: "expires_at::TIMESTAMPTZ"},
		{name: "arithmetic carrying a quoted interval", expression: "expires_at + INTERVAL '1 day'"},
		{name: "extra internal whitespace", expression: "expires_at  +  INTERVAL '2 days'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := skipIfNoCockroachDB(t)
			c := qt.New(t)
			db, err := sql.Open("pgx", dsn)
			c.Assert(err, qt.IsNil)
			defer db.Close()
			dropRowTTLTable(db)
			defer dropRowTTLTable(db)

			declared := rowTTLDeclaration(&ast.RowTTLSpec{ExpirationExpression: test.expression})
			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

			c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals,
				&ast.RowTTLSpec{ExpirationExpression: test.expression})
			c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)
		})
	}
}

// rowTTLDeclaration is the desired state these tests apply: one table with the
// given policy, or none.
func rowTTLDeclaration(spec *ast.RowTTLSpec) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Sessions", Name: rowTTLTable, RowTTL: spec}},
		Fields: []goschema.Field{
			{StructName: "Sessions", Name: "id", Type: "INT8", Primary: true},
			{StructName: "Sessions", Name: "expires_at", Type: "TIMESTAMPTZ", Nullable: true},
		},
	}
}

// planRowTTLAgainstLive re-reads the database and returns the statements the
// comparison would run against it.
//
// The read happens here rather than at the call site so that every plan is
// built from a description taken after the previous one was applied. Comparing
// against a description read once would assert about statements Ptah would
// emit, not about the state the previous ones reached.
func planRowTTLAgainstLive(
	c *qt.C, t *testing.T, dsn string, declared *goschema.Database,
) []string {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(err, qt.IsNil)

	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, live, info, nil)
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, declared, info.Dialect, info.Capabilities)
	c.Assert(err, qt.IsNil)
	return statements
}

// applyRowTTLPlan runs a plan statement by statement, so a failure names the
// statement that failed rather than the batch.
func applyRowTTLPlan(c *qt.C, db *sql.DB, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// readRowTTL returns the policy the live description reports for the table.
func readRowTTL(c *qt.C, t *testing.T, dsn string) *ast.RowTTLSpec {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(err, qt.IsNil)

	return rowTTLOf(live)
}

// rowTTLOf finds the table under test in a description. It returns nil for a
// description that does not carry it, which a caller asserting on the policy
// would then read as "no policy" -- so the table's presence is asserted
// separately by rowTTLTableExists.
func rowTTLOf(live *dbschematypes.DBSchema) *ast.RowTTLSpec {
	for _, table := range live.Tables {
		if table.Name == rowTTLTable {
			return table.RowTTL
		}
	}
	return nil
}

func rowTTLTableExists(c *qt.C, db *sql.DB) bool {
	c.Helper()
	var count int
	err := db.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, rowTTLTable,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func dropRowTTLTable(db *sql.DB) {
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + rowTTLTable + ` CASCADE`)
}

// TestCockroachDBRowLevelTTL_ExpireAfterRoundTripsLive covers the enabler
// stokaro/ptah#1027 refused and stokaro/ptah#1605 added.
//
// It is the interesting one because the server REWRITES what it stores: the
// declared `72 hours` is kept as `72:00:00`, so a comparison over the text
// would find a difference on every run and the plan would never empty. The
// convergence assertions below are what a text comparison could not satisfy.
func TestCockroachDBRowLevelTTL_ExpireAfterRoundTripsLive(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a spelling the server keeps", declared: "3 days"},
		{name: "hours the server keeps as a clock time", declared: "72 hours"},
		{name: "minutes the server pads into a clock time", declared: "5 minutes"},
		{name: "weeks the server folds into days", declared: "1 week"},
		{name: "months the server keeps as months", declared: "2 years 3 months"},
		{name: "an ISO-8601 duration", declared: "P1Y2M3D"},
		{name: "a fractional quantity", declared: "1.5 hours"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := skipIfNoCockroachDB(t)
			c := qt.New(t)
			db, err := sql.Open("pgx", dsn)
			c.Assert(err, qt.IsNil)
			defer db.Close()
			dropRowTTLTable(db)
			defer dropRowTTLTable(db)

			declared := rowTTLDeclaration(&ast.RowTTLSpec{ExpireAfter: test.declared})
			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

			// The stored spelling is the server's, not the declaration's, and
			// asserting it here is what makes the convergence below meaningful:
			// the two differ as text and still compare equal.
			live := readRowTTL(c, t, dsn)
			c.Assert(live, qt.IsNotNil)
			c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)
		})
	}
}

// TestCockroachDBRowLevelTTL_ExpireAfterHidesTheColumnItCreates pins the second
// half of stokaro/ptah#1605.
//
// `ttl_expire_after` adds a hidden crdb_internal_expiration column. A reader
// that described it would report a column nobody declared, and the comparator
// would plan a DROP COLUMN for a column the engine owns -- so the table would
// never converge however well the interval compared.
func TestCockroachDBRowLevelTTL_ExpireAfterHidesTheColumnItCreates(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)

	declared := rowTTLDeclaration(&ast.RowTTLSpec{ExpireAfter: "3 days"})
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

	// The column is really there: the assertion below is about the read, not
	// about the server having declined to create it.
	var hidden int
	c.Assert(db.QueryRow(
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_name = $1 AND column_name = 'crdb_internal_expiration'`, rowTTLTable,
	).Scan(&hidden), qt.IsNil)
	c.Assert(hidden, qt.Equals, 1)

	c.Assert(rowTTLColumnNames(c, t, dsn), qt.DeepEquals, []string{"id", "expires_at"})
	c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)
}

// TestCockroachDBRowLevelTTL_AKeylessTableHidesItsRowid covers the older leak
// the same filter closes.
//
// A CockroachDB table declaring no primary key gets a hidden `rowid`, and it
// reached descriptions long before row-level TTL existed: `ptah db read`
// reported `"rowid" bigint PRIMARY KEY NOT NULL DEFAULT unique_rowid()` as a
// third column of a two-column table. Nobody declared it and no other engine
// could replay it.
func TestCockroachDBRowLevelTTL_AKeylessTableHidesItsRowid(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)

	_, err = db.Exec(`CREATE TABLE ` + rowTTLTable + ` (a INT, b STRING)`)
	c.Assert(err, qt.IsNil)

	var hidden int
	c.Assert(db.QueryRow(
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_name = $1 AND column_name = 'rowid'`, rowTTLTable,
	).Scan(&hidden), qt.IsNil)
	c.Assert(hidden, qt.Equals, 1)

	c.Assert(rowTTLColumnNames(c, t, dsn), qt.DeepEquals, []string{"a", "b"})
}

// rowTTLColumnNames returns the columns the live description reports for the
// table under test, in order.
func rowTTLColumnNames(c *qt.C, t *testing.T, dsn string) []string {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(err, qt.IsNil)

	for _, table := range live.Tables {
		if table.Name != rowTTLTable {
			continue
		}
		names := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
		return names
	}
	return nil
}

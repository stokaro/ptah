//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
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
	declared := rowTTLDeclaration(&crdbschema.Policy{
		ExpirationExpression: "expires_at",
		JobCron:              "@daily",
		SelectBatchSize:      new(int64(500)),
	})
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

	created := readRowTTL(c, t, dsn)
	c.Assert(created, qt.DeepEquals, &crdbschema.Policy{
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
	changed := rowTTLDeclaration(&crdbschema.Policy{
		ExpirationExpression: "expires_at + INTERVAL '1 hour'",
		JobCron:              "@hourly",
	})
	applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, changed))

	c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals, &crdbschema.Policy{
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
		// `:::` is CockroachDB's type annotation operator, and the catalog
		// writes one after a typed parameter's literal too: only that suffix
		// is the catalog's.
		{name: "a type annotation inside the expression", expression: "expires_at:::TIMESTAMPTZ"},
		{name: "an annotated literal carrying a quote", expression: "expires_at + '1 day':::INTERVAL"},
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

			declared := rowTTLDeclaration(&crdbschema.Policy{ExpirationExpression: test.expression})
			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

			c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals,
				&crdbschema.Policy{ExpirationExpression: test.expression})
			c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)
		})
	}
}

// rowTTLDeclaration is the desired state these tests apply: one table with the
// given policy, or none. It is declared through the Go annotation source, the
// surface authors use, so the platform.cockroachdb properties, their decoding
// by the CockroachDB owner and the source's coverage are all on the path: a
// table without the properties requests no TTL.
func rowTTLDeclaration(policy *crdbschema.Policy) *schemamodel.Database {
	var properties strings.Builder
	if policy != nil {
		for _, parameter := range policy.Parameters() {
			fmt.Fprintf(&properties, " platform.cockroachdb.%s=%s", parameter.Name, strconv.Quote(parameter.Value))
		}
	}
	source := "package entities\n\n//ptah:schema:table name=\"" + rowTTLTable + "\"" + properties.String() + "\n" +
		"type Sessions struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT8\" primary=\"true\"\n\tID int64\n" +
		"\t//ptah:schema:field name=\"expires_at\" type=\"TIMESTAMPTZ\"\n\tExpiresAt *time.Time\n" +
		"}\n"
	database := must.Must(goschema.ParseSource("sessions.go", source))
	return &database
}

// planRowTTLAgainstLive re-reads the database and returns the statements the
// comparison would run against it.
//
// The read happens here rather than at the call site so that every plan is
// built from a description taken after the previous one was applied. Comparing
// against a description read once would assert about statements Ptah would
// emit, not about the state the previous ones reached.
func planRowTTLAgainstLive(
	c *qt.C, t *testing.T, dsn string, declared *schemamodel.Database,
) []string {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)

	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, live, info, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
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
func readRowTTL(c *qt.C, t *testing.T, dsn string) *crdbschema.Policy {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)

	return rowTTLOf(live)
}

// rowTTLOf finds the table under test in a description. It returns nil for a
// description that does not carry it, which a caller asserting on the policy
// would then read as "no policy" -- so the table's presence is asserted
// separately by rowTTLTableExists.
func rowTTLOf(live *catalog.Database) *crdbschema.Policy {
	for _, table := range live.Tables {
		if table.Name != rowTTLTable {
			continue
		}
		observed, found, err := schemaext.FacetAs[*crdbschema.ObservedRowTTL](table.Facets, crdbschema.RowTTLKind)
		if err != nil || !found {
			return nil
		}
		return &observed.Policy
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

			declared := rowTTLDeclaration(&crdbschema.Policy{ExpireAfter: test.declared})
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

	declared := rowTTLDeclaration(&crdbschema.Policy{ExpireAfter: "3 days"})
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

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
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

// TestCockroachDBRowLevelTTL_RowStatsPollIntervalRoundTripsLive covers the knob
// stokaro/ptah#1027 refused and stokaro/ptah#1721 added.
//
// It is the second parameter whose value the server rewrites, and it rewrites
// it differently from `ttl_expire_after`: the duration is truncated to whole
// seconds and stored in Go's spelling, so a declared `600s` is kept as `10m0s`
// and a declared `1 month` as `720h0m0s`. A comparison over the text would find
// a difference on every run and the plan would never empty.
//
// Each row asserts more than convergence. The stored value is read back and
// checked against the form
// [ptah.run/internal/crdbduration] predicts, so a rewrite this package
// gets wrong fails here as a wrong VALUE rather than only as a plan that will
// not settle -- which is the difference between "something did not converge"
// and "the conversion is wrong, and here is what the server did instead".
func TestCockroachDBRowLevelTTL_RowStatsPollIntervalRoundTripsLive(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{name: "seconds the server re-expresses as minutes", declared: "600s", stored: "10m0s"},
		{name: "a Go duration the server keeps", declared: "2h45m30s", stored: "2h45m30s"},
		{name: "minutes spelled as an interval", declared: "5 minutes", stored: "5m0s"},
		{name: "a clock time", declared: "00:10:00", stored: "10m0s"},
		{name: "an ISO-8601 duration", declared: "PT10M", stored: "10m0s"},
		{name: "a day, folded into hours", declared: "1 day", stored: "24h0m0s"},
		{name: "a month, which is thirty days", declared: "1 month", stored: "720h0m0s"},
		{name: "twelve months, which is a year rather than twelve of those", declared: "12 months", stored: "8766h0m0s"},
		{name: "sub-second precision, truncated rather than rounded", declared: "2500ms", stored: "2s"},
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

			declared := rowTTLDeclaration(&crdbschema.Policy{
				ExpireAfter:          "1 hour",
				RowStatsPollInterval: test.declared,
			})
			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, declared))

			live := readRowTTL(c, t, dsn)
			c.Assert(live, qt.IsNotNil)
			c.Assert(live.RowStatsPollInterval, qt.Equals, test.stored)
			c.Assert(planRowTTLAgainstLive(c, t, dsn, declared), qt.HasLen, 0)
		})
	}
}

// TestCockroachDBRowLevelTTL_RowStatsPollIntervalBelowASecondIsRefused pins the
// case that makes this parameter more than a canonicalization.
//
// A value the server truncates to zero is stored NOWHERE AT ALL: the statement
// succeeds, the table carries no such parameter, and every later inspection
// reports it missing while the plan re-issues it forever.
//
// Ptah refuses such a declaration before any SQL, and the refusal itself is
// covered offline. What only a server can establish is the PREMISE it rests on,
// which is what this test makes the server demonstrate: the value is accepted
// and then kept nowhere. Without this the refusal would rest on a claim about
// an engine rather than on a measurement of one.
func TestCockroachDBRowLevelTTL_RowStatsPollIntervalBelowASecondIsRefused(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)

	_, err = db.Exec(`CREATE TABLE ` + rowTTLTable + ` (id INT8 PRIMARY KEY) ` +
		`WITH (ttl_expire_after = '1 hour', ttl_row_stats_poll_interval = '500ms')`)
	c.Assert(err, qt.IsNil)

	// The statement succeeded and the table carries no such parameter. Read
	// back through the same reader every other test here uses, so this is what
	// Ptah would see: a declaration that applied cleanly and is missing.
	live := readRowTTL(c, t, dsn)
	c.Assert(live, qt.IsNotNil)
	c.Assert(live.ExpireAfter, qt.Not(qt.Equals), "")
	c.Assert(live.RowStatsPollInterval, qt.Equals, "")
}

// TestCockroachDBRowLevelTTL_SwitchesItsEnablerLive moves a table from one
// enabler to the other.
//
// The server refuses any statement that leaves a TTL table with neither
// ttl_expire_after nor ttl_expiration_expression, so the plan has to set the
// new enabler before it resets the old one. A plan in the other order fails on
// its RESET, which is how this was found.
func TestCockroachDBRowLevelTTL_SwitchesItsEnablerLive(t *testing.T) {
	tests := []struct {
		name string
		from crdbschema.Policy
		to   crdbschema.Policy
	}{
		{
			name: "from an expression to an interval, dropping a knob",
			from: crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily", SelectBatchSize: new(int64(500))},
			to:   crdbschema.Policy{ExpireAfter: "3 days"},
		},
		{
			name: "from an interval to an expression",
			from: crdbschema.Policy{ExpireAfter: "3 days", JobCron: "@daily"},
			to:   crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"},
		},
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

			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, rowTTLDeclaration(&test.from)))
			c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals, &test.from)

			switched := rowTTLDeclaration(&test.to)
			applyRowTTLPlan(c, db, planRowTTLAgainstLive(c, t, dsn, switched))

			c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals, &test.to)
			c.Assert(planRowTTLAgainstLive(c, t, dsn, switched), qt.HasLen, 0)
		})
	}
}

// TestCockroachDBRowLevelTTL_RollsBackBesideAnRLSToggleLive applies both
// directions of a plan that sets a policy on a table whose row-level security
// is enabled in the same plan. The generator cannot project that table's
// reverse capture, and the TTL reverse does not need it, so the plan exists in
// both directions; this measures that each one runs and reaches its state.
func TestCockroachDBRowLevelTTL_RollsBackBesideAnRLSToggleLive(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	dropRowTTLTable(db)
	defer dropRowTTLTable(db)
	_, err = db.Exec(`CREATE TABLE ` + rowTTLTable + ` (id INT8 PRIMARY KEY, expires_at TIMESTAMPTZ)`)
	c.Assert(err, qt.IsNil)

	declared := must.Must(goschema.ParseSource("sessions.go", "package entities\n\n"+
		"//ptah:schema:rls:enable table=\""+rowTTLTable+"\"\n"+
		"//ptah:schema:table name=\""+rowTTLTable+"\" platform.cockroachdb.ttl_expire_after=\"3 days\"\n"+
		"type Sessions struct {\n"+
		"\t//ptah:schema:field name=\"id\" type=\"INT8\" primary=\"true\"\n\tID int64\n"+
		"\t//ptah:schema:field name=\"expires_at\" type=\"TIMESTAMPTZ\"\n\tExpiresAt *time.Time\n"+
		"}\n"))
	forward, reverse := planRowTTLBothWays(c, t, dsn, &declared)

	applyRowTTLPlan(c, db, forward)
	c.Assert(readRowTTL(c, t, dsn), qt.DeepEquals, &crdbschema.Policy{ExpireAfter: "3 days"})
	c.Assert(rowSecurity(c, db), qt.IsTrue)

	applyRowTTLPlan(c, db, reverse)
	c.Assert(readRowTTL(c, t, dsn), qt.IsNil)
	c.Assert(rowSecurity(c, db), qt.IsFalse)
}

// planRowTTLBothWays reads the database and returns the statements of both
// directions of the bidirectional plan to the declaration.
func planRowTTLBothWays(c *qt.C, t *testing.T, dsn string, declared *schemamodel.Database) (forward, reverse []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	info := conn.Info()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, live, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: declared, CurrentSchema: live, Dialect: info.Dialect, Capabilities: info.Capabilities,
	})
	c.Assert(err, qt.IsNil)
	options := planner.Options{Capabilities: info.Capabilities}
	forward, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, plan.Forward.Diff, info.Dialect, options)
	c.Assert(err, qt.IsNil)
	reverse, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, plan.Reverse.Diff, info.Dialect, options)
	c.Assert(err, qt.IsNil)
	return forward, reverse
}

// rowSecurity reports whether row-level security is enabled on the table.
func rowSecurity(c *qt.C, db *sql.DB) bool {
	c.Helper()
	var enabled bool
	c.Assert(db.QueryRow(`SELECT relrowsecurity FROM pg_class WHERE relname = $1`, rowTTLTable).Scan(&enabled), qt.IsNil)
	return enabled
}

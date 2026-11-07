//go:build integration

package atlasschema_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/rolepassword"
)

// These tests hold what a caller of `ptah schema apply --json` learns when a
// statement or a commit fails on a real server. They run the shipped binary,
// because the record is read out of the chain a driver's error travels up, and
// only a live server produces that error. They read the database back, because
// the document is a report about it.

// failureRowValue is what the record must not repeat. It sits in the statement
// that fails, and the servers that quote a row in their message quote it too.
const failureRowValue = "row-value@example.test"

// failureDocument is the document with its record kept as the bytes it was
// written in, so a key an engine has no value for is seen to be absent rather
// than decoded to zero.
type failureDocument struct {
	Outcome atlasschema.ApplyOutcome      `json:"outcome"`
	Units   []atlasschema.ApplyUnitReport `json:"units"`
	Error   *errorkind.Report             `json:"error"`
	Refusal *atlasschema.Refusal          `json:"refusal"`
	Failure json.RawMessage               `json:"failure"`
}

// failureTarget is a database of a test's own, and a direct handle on it with
// nothing of the command's in between.
type failureTarget struct {
	url  string
	conn *dbschema.DatabaseConnection
	// suffix makes each table name unique, on the one engine whose target is a
	// schema shared with other tests.
	suffix string
}

// newFailureTarget creates the database. Oracle has no database to create without
// an administrator's account, so it works in the target account's schema, under
// names only this test uses.
func newFailureTarget(c *qt.C, engine dbtarget.Engine) *failureTarget {
	c.Helper()
	address := dbtarget.URL(c, engine)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	target := address
	name := ""
	if engine != dbtarget.Oracle {
		// The MySQL family's test account may create only this prefix.
		name = "ptah_rt_failure_" + suffix
		_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+quotedFailureDatabase(engine, name))
		c.Assert(err, qt.IsNil)
		target = failureDatabaseURL(c, engine, address, name)
	}
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		if name != "" {
			_, dropErr := admin.ExecContext(context.Background(), dropFailureDatabase(engine, name))
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	return &failureTarget{url: target, conn: conn, suffix: suffix}
}

// quotedFailureDatabase is the database's name as the engine quotes it.
func quotedFailureDatabase(engine dbtarget.Engine, name string) string {
	switch engine {
	case dbtarget.MySQL, dbtarget.MariaDB, dbtarget.ClickHouse:
		return "`" + name + "`"
	case dbtarget.SQLServer:
		return "[" + name + "]"
	default:
		return `"` + name + `"`
	}
}

// dropFailureDatabase drops the database, ending the sessions a server keeps open
// on it.
func dropFailureDatabase(engine dbtarget.Engine, name string) string {
	quoted := quotedFailureDatabase(engine, name)
	switch engine {
	case dbtarget.PostgreSQL:
		return "DROP DATABASE IF EXISTS " + quoted + " WITH (FORCE)"
	case dbtarget.SQLServer:
		return "IF DB_ID('" + name + "') IS NOT NULL BEGIN ALTER DATABASE " + quoted +
			" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE " + quoted + " END"
	default:
		return "DROP DATABASE IF EXISTS " + quoted
	}
}

// failureDatabaseURL names the database in the engine's address. SQL Server keeps
// it in a query parameter, and the MySQL family's address does not parse as a URL.
func failureDatabaseURL(c *qt.C, engine dbtarget.Engine, address, name string) string {
	c.Helper()
	if engine == dbtarget.SQLServer {
		parsed, err := url.Parse(address)
		c.Assert(err, qt.IsNil)
		query := parsed.Query()
		query.Set("database", name)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	base, query, hasQuery := strings.Cut(address, "?")
	replaced := base[:strings.LastIndex(base, "/")+1] + name
	if hasQuery {
		return replaced + "?" + query
	}
	return replaced
}

// table is a table name of this target's own.
func (f *failureTarget) table(name string) string {
	return name + "_" + f.suffix
}

// exec runs the statements with nothing of the command's in between.
func (f *failureTarget) exec(c *qt.C, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// dropAfterwards drops the tables when the test ends, for the target whose
// database outlives it.
func (f *failureTarget) dropAfterwards(c *qt.C, tables ...string) {
	c.Helper()
	c.Cleanup(func() {
		for _, table := range tables {
			_, _ = f.conn.ExecContext(context.Background(), "DROP TABLE "+table)
		}
	})
}

// newSharedFailureTarget works in the database the engine's address names, under
// names only this test uses. It is for a command that touches only what a test
// names, so the database is the one a test account holds its grants on.
func newSharedFailureTarget(c *qt.C, engine dbtarget.Engine) *failureTarget {
	c.Helper()
	address := dbtarget.URL(c, engine)
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return &failureTarget{url: address, conn: conn, suffix: fmt.Sprintf("%d", time.Now().UnixNano())}
}

// tableExists asks the server whether a table is there.
func (f *failureTarget) tableExists(c *qt.C, table string) bool {
	c.Helper()
	_, err := f.conn.ExecContext(c.Context(), "SELECT id FROM "+table+" WHERE id = 0")
	return err == nil
}

// savedPlan plans against the target, so the plan's fingerprint is the target's,
// and replaces the planned statements with the ones given. A plan holds what it
// holds: the server's answer to them is what is measured here.
func (f *failureTarget) savedPlan(c *qt.C, desired string, statements ...string) string {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	path := filepath.Join(dir, "change.plan.json")
	c.Assert(os.WriteFile(schema, []byte(desired+";\n"), 0o600), qt.IsNil)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "plan", "--db-url", f.url, "--schema-file", schema, "--output", path)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	plan, _, err := atlasschema.DecodePlanFile(contents, path)
	c.Assert(err, qt.IsNil)
	plan.Destructive = false
	plan.Statements = nil
	for _, statement := range statements {
		plan.Statements = append(plan.Statements, atlasschema.PlanStatement{SQL: statement, Severity: "safe", Reason: "written by the test"})
	}
	edited, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path, edited, 0o600), qt.IsNil) // #nosec G703 -- the path is under the test's own temporary directory.
	return path
}

// apply runs `ptah schema apply --json` and decodes the document.
func (f *failureTarget) apply(c *qt.C, args ...string) (clirun.Result, failureDocument) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"schema", "apply", "--db-url", f.url, "--auto-approve", "--json"}, args...)...)
	var document failureDocument
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return result, document
}

// compactRecord is the record as one line, to compare with what the engine is
// expected to write.
func compactRecord(c *qt.C, record json.RawMessage) string {
	c.Helper()
	if len(record) == 0 {
		return ""
	}
	var compact bytes.Buffer
	c.Assert(json.Compact(&compact, record), qt.IsNil)
	return compact.String()
}

// The DDL each engine takes. ClickHouse enforces no unique key, so its table has
// no email column for the failing statement to name.
const (
	plainTableDDL      = "CREATE TABLE {t} (id INT PRIMARY KEY)"
	uniqueTableDDL     = "CREATE TABLE {t} (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE)"
	clickHouseTableDDL = "CREATE TABLE {t} (id Int32) ENGINE = MergeTree ORDER BY id"
)

// TestSchemaApplyFailureRecordOfASavedPlanE2E fails the second statement of three
// on each engine, in both modes that run statements on the target.
//
// The first statement says what the mode left behind, and the third that the run
// stopped. The record is the same whichever it is: PostgreSQL and SQL Server take
// the first statement back in `--tx-mode file`, and the engines whose schema
// statements outlive the rollback do not.
func TestSchemaApplyFailureRecordOfASavedPlanE2E(t *testing.T) {
	tests := []struct {
		name       string
		engine     dbtarget.Engine
		plainDDL   string
		seedDDL    string
		seedRow    string
		mode       string
		wantRecord string
		wantFirst  bool
	}{
		{
			name: "postgresql in a transaction", engine: dbtarget.PostgreSQL, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23505"}`,
		},
		{
			name: "postgresql without a transaction", engine: dbtarget.PostgreSQL, mode: "none",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23505"}`, wantFirst: true,
		},
		{
			name: "mysql, whose schema statements commit themselves", engine: dbtarget.MySQL, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23000","error_number":1062}`, wantFirst: true,
		},
		{
			name: "mysql without a transaction", engine: dbtarget.MySQL, mode: "none",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23000","error_number":1062}`, wantFirst: true,
		},
		{
			name: "mariadb, whose schema statements commit themselves", engine: dbtarget.MariaDB, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23000","error_number":1062}`, wantFirst: true,
		},
		{
			name: "mariadb without a transaction", engine: dbtarget.MariaDB, mode: "none",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23000","error_number":1062}`, wantFirst: true,
		},
		{
			name: "sql server, which has no SQLSTATE, in a transaction", engine: dbtarget.SQLServer, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"error_number":2627}`,
		},
		{
			name: "sql server without a transaction", engine: dbtarget.SQLServer, mode: "none",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"error_number":2627}`, wantFirst: true,
		},
		{
			name: "cockroachdb, whose schema statement outlives the rollback", engine: dbtarget.CockroachDB, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23505"}`, wantFirst: true,
		},
		{
			name: "yugabytedb, whose schema statement outlives the rollback", engine: dbtarget.YugabyteDB, mode: "file",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"sqlstate":"23505"}`, wantFirst: true,
		},
		{
			name: "oracle, which has no SQLSTATE", engine: dbtarget.Oracle, mode: "none",
			plainDDL: plainTableDDL, seedDDL: uniqueTableDDL, seedRow: "INSERT INTO {t} (id, email) VALUES (1, '" + failureRowValue + "')",
			wantRecord: `{"statement":2,"statements":3,"error_number":1}`, wantFirst: true,
		},
		{
			name: "clickhouse, which has no SQLSTATE", engine: dbtarget.ClickHouse, mode: "none",
			plainDDL: clickHouseTableDDL, seedDDL: clickHouseTableDDL,
			wantRecord: `{"statement":2,"statements":3,"error_number":16}`, wantFirst: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, test.engine)
			accounts, first, third, planned := f.table("accounts"), f.table("first_unit"), f.table("third_unit"), f.table("planned")
			f.dropAfterwards(c, accounts, first, third)
			named := func(template, table string) string { return strings.ReplaceAll(template, "{t}", table) }
			f.exec(c, named(test.seedDDL, accounts))
			f.exec(c, seedStatements(named(test.seedRow, accounts))...)
			// ClickHouse's table has no such column. Elsewhere the statement repeats
			// the seeded row's unique value.
			failing := fmt.Sprintf("INSERT INTO %s (id, email) VALUES (2, '%s')", accounts, failureRowValue)
			plan := f.savedPlan(c, named(test.plainDDL, planned),
				named(test.plainDDL, first), failing, named(test.plainDDL, third))

			result, document := f.apply(c, "--plan", plan, "--tx-mode", test.mode)

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Logf("message: %s", document.Error.Message)
			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
			c.Assert(compactRecord(c, document.Failure), qt.Equals, test.wantRecord)
			// The control: the statement and the message hold the value the record
			// does not, so the record is free of it by construction.
			c.Assert(document.Units[1].SQL, qt.Contains, failureRowValue)
			c.Assert(document.Error.Message, qt.Contains, failureRowValue)
			c.Assert(f.tableExists(c, first), qt.Equals, test.wantFirst)
			c.Assert(f.tableExists(c, third), qt.IsFalse)
		})
	}
}

// seedStatements leaves out the row of an engine whose fixture seeds none.
func seedStatements(row string) []string {
	if row == "" {
		return nil
	}
	return []string{row}
}

// TestSchemaApplyFailureRecordOfAComputedPlanE2E runs the path that plans in the
// same run. The planned statements hold the declared rows, and the last row
// repeats a unique value, so the failure is in a later unit of a plan that created
// two tables first.
func TestSchemaApplyFailureRecordOfAComputedPlanE2E(t *testing.T) {
	tests := []struct {
		name       string
		engine     dbtarget.Engine
		mode       string
		wantRecord string
		wantTables bool
	}{
		{
			name: "postgresql in a transaction", engine: dbtarget.PostgreSQL, mode: "file",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23505"}`,
		},
		{
			name: "postgresql without a transaction", engine: dbtarget.PostgreSQL, mode: "none",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23505"}`, wantTables: true,
		},
		{
			name: "mysql", engine: dbtarget.MySQL, mode: "file",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23000","error_number":1062}`, wantTables: true,
		},
		{
			name: "mysql without a transaction", engine: dbtarget.MySQL, mode: "none",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23000","error_number":1062}`, wantTables: true,
		},
		{
			name: "mariadb", engine: dbtarget.MariaDB, mode: "file",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23000","error_number":1062}`, wantTables: true,
		},
		{
			name: "mariadb without a transaction", engine: dbtarget.MariaDB, mode: "none",
			wantRecord: `{"statement":4,"statements":4,"sqlstate":"23000","error_number":1062}`, wantTables: true,
		},
		{
			name: "sql server in a transaction", engine: dbtarget.SQLServer, mode: "file",
			wantRecord: `{"statement":4,"statements":4,"error_number":2627}`,
		},
		{
			name: "sql server without a transaction", engine: dbtarget.SQLServer, mode: "none",
			wantRecord: `{"statement":4,"statements":4,"error_number":2627}`, wantTables: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, test.engine)
			accounts, notes := f.table("accounts"), f.table("notes")
			f.dropAfterwards(c, accounts, notes)
			root := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(root, "entities.go"), fmt.Appendf(nil, declaredRowsEntities, accounts, accounts, notes), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "rows.yaml"), fmt.Appendf(nil, "- id: 1\n  email: %[1]q\n- id: 2\n  email: %[1]q\n", failureRowValue), 0o600), qt.IsNil)

			result, document := f.apply(c, "--root-dir", root, "--tx-mode", test.mode)

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
			c.Assert(compactRecord(c, document.Failure), qt.Equals, test.wantRecord)
			c.Assert(document.Units, qt.HasLen, 4)
			c.Assert(document.Units[3].SQL, qt.Contains, failureRowValue)
			c.Assert(document.Error.Message, qt.Contains, failureRowValue)
			c.Assert(f.tableExists(c, notes), qt.Equals, test.wantTables)
		})
	}
}

// declaredRowsEntities declares a table with a unique column, the rows of it, and
// a second table. The fields of the format are the table's name twice and the
// second table's name.
const declaredRowsEntities = `package entities

//ptah:schema:table name="%s"
//ptah:schema:data table="%s" key="id" file="rows.yaml"
type Account struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="email" type="VARCHAR(100)" unique="true"
	Email string
}

//ptah:schema:table name="%s"
type Note struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
}
`

// TestSchemaApplyFailureRecordOfACommitE2E violates a deferred foreign key, which
// the server answers at the commit: every statement succeeded, so no position is
// to blame, and the code is the commit's.
func TestSchemaApplyFailureRecordOfACommitE2E(t *testing.T) {
	tests := []struct {
		name       string
		engine     dbtarget.Engine
		wantRecord string
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, wantRecord: `{"statement":0,"statements":2,"commit":true,"sqlstate":"23503"}`},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, wantRecord: `{"statement":0,"statements":2,"commit":true,"sqlstate":"23503"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, test.engine)
			parents, children, planned := f.table("parents"), f.table("children"), f.table("planned")
			f.exec(c, "CREATE TABLE "+parents+" (id INT PRIMARY KEY)",
				"CREATE TABLE "+children+" (id INT PRIMARY KEY, parent INT REFERENCES "+parents+" (id) DEFERRABLE INITIALLY DEFERRED, note VARCHAR(100))")
			plan := f.savedPlan(c, "CREATE TABLE "+planned+" (id INT PRIMARY KEY)",
				"INSERT INTO "+children+" (id, parent, note) VALUES (1, 999, '"+failureRowValue+"')",
				"INSERT INTO "+children+" (id, parent, note) VALUES (2, 999, '"+failureRowValue+"')")

			result, document := f.apply(c, "--plan", plan, "--tx-mode", "file")

			c.Logf("failure: %s", compactRecord(c, document.Failure))
			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
			c.Assert(compactRecord(c, document.Failure), qt.Equals, test.wantRecord)
			c.Assert(document.Error.Message, qt.Contains, "commit schema apply transaction: ")
			var rows int
			c.Assert(f.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM "+children).Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 0)
		})
	}
}

// TestSchemaApplyFailureRecordOfAStatementNeverSentE2E stops a run at a statement
// the writer refuses to send, after an earlier one went out. The run reached the
// database, and the record names where it stopped, with no code because no server
// answered.
func TestSchemaApplyFailureRecordOfAStatementNeverSentE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	first, role := f.table("first_unit"), f.table("role")
	plan := f.savedPlan(c, "CREATE TABLE "+f.table("planned")+" (id INT PRIMARY KEY)",
		"CREATE TABLE "+first+" (id INT PRIMARY KEY)",
		`CREATE ROLE "`+role+`" WITH LOGIN PASSWORD `+rolepassword.Reference(role))
	c.Cleanup(func() {
		_, _ = f.conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
	})

	result, document := f.apply(c, "--plan", plan, "--tx-mode", "none")

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
	c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
	c.Assert(compactRecord(c, document.Failure), qt.Equals, `{"statement":2,"statements":2}`)
	c.Assert(f.tableExists(c, first), qt.IsTrue)
}

// TestSchemaApplyNoFailureRecordBeforeDispatchE2E stops a run on a real server
// before its first statement leaves. A preflight and a rehearsal both name a
// statement, and a server answers the rehearsal's, on the dev database. Nothing
// reached the target, so none of them is a failure the target answered.
func TestSchemaApplyNoFailureRecordBeforeDispatchE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	accounts, planned := f.table("accounts"), f.table("planned")
	f.exec(c, strings.ReplaceAll(uniqueTableDDL, "{t}", accounts))

	t.Run("a statement the transaction mode refuses", func(t *testing.T) {
		c := qt.New(t)
		plan := f.savedPlan(c, "CREATE TABLE "+planned+" (id INT PRIMARY KEY)",
			"CREATE INDEX CONCURRENTLY "+accounts+"_email ON "+accounts+" (email)")

		_, document := f.apply(c, "--plan", plan, "--tx-mode", "file")

		c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(document.Refusal.Code, qt.Equals, atlasschema.RefusalTransactionPreflight)
		c.Assert(compactRecord(c, document.Failure), qt.Equals, "")
	})
	t.Run("a rehearsal the dev database refused", func(t *testing.T) {
		c := qt.New(t)
		dev := newFailureTarget(c, dbtarget.PostgreSQL)
		declared, notes := f.table("declared"), f.table("notes")
		root := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(root, "entities.go"), fmt.Appendf(nil, declaredRowsEntities, declared, declared, notes), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(root, "rows.yaml"), fmt.Appendf(nil, "- id: 1\n  email: %[1]q\n- id: 2\n  email: %[1]q\n", failureRowValue), 0o600), qt.IsNil)

		_, document := f.apply(c, "--root-dir", root, "--dev-url", dev.url)

		c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(document.Refusal.Code, qt.Equals, atlasschema.RefusalSimulationFailed)
		c.Assert(document.Error.Message, qt.Contains, failureRowValue)
		c.Assert(compactRecord(c, document.Failure), qt.Equals, "")
		c.Assert(f.tableExists(c, notes), qt.IsFalse)
	})
	t.Run("a plan of a target that moved", func(t *testing.T) {
		c := qt.New(t)
		plan := f.savedPlan(c, "CREATE TABLE "+planned+" (id INT PRIMARY KEY)", "SELECT 1")
		f.exec(c, "CREATE TABLE "+f.table("moved")+" (id INT PRIMARY KEY)")

		_, document := f.apply(c, "--plan", plan, "--tx-mode", "file")

		c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(document.Refusal.Code, qt.Equals, atlasschema.RefusalStalePlan)
		c.Assert(compactRecord(c, document.Failure), qt.Equals, "")
	})
}

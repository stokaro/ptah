//go:build integration

package runlog_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests hold the two logs a run leaves in the target to what a caller
// reads back: the rows `ptah schema apply` writes to ptah_schema_apply_log and
// the run reference `ptah migrations up` stores in its operation log. They run
// the shipped binary, because the flags and the --json document are half the
// claim, and they read the rows with a connection of their own, because the
// document is a report about them.

// runLogEngine is an engine the rows run on, and the spellings that differ
// between engines. Every field is data: no test picks one by engine.
type runLogEngine struct {
	name   string
	engine dbtarget.Engine
	// create and drop make and remove a database of the test's own, {name}
	// standing for it. An empty create is an engine whose test account cannot
	// create one: the test works in the account's schema, under names of its
	// own, and reads only the rows carrying its own run reference.
	create, drop string
	// ident is a table name as Ptah spells it, as a format with one %s.
	ident string
	// table creates a table, {t} standing for its name.
	table string
	// releasedLog is the operation log as Ptah v0.26.0 created it, the layout
	// with no run_reference and no actor_origin; {log} stands for its name.
	releasedLog string
	// releasedRow is a row v0.26.0 wrote to it.
	releasedRow string
}

// releasedLogColumns is the v0.26.0 operation log on every engine that takes
// the shared spelling, which every built-in engine but Oracle and SQL Server
// did.
const releasedLogColumns = `CREATE TABLE {log} (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    checksum VARCHAR(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (run_id, seq)
)`

// releasedRowValues is the released run's start entry, its time a literal
// every engine but Oracle reads as a timestamp.
const releasedRowValues = `INSERT INTO {log} (run_id, seq, operation, version, state, actor, actor_source, logged_at)
VALUES ('released-run', 1, 'up', 9, 'started', 'bob', 'provided', '2026-10-01 12:00:00')`

const plainTable = "CREATE TABLE {t} (id INT PRIMARY KEY)"

var (
	postgresRunLog = runLogEngine{
		name: "postgresql", engine: dbtarget.PostgreSQL,
		create: `CREATE DATABASE "{name}"`, drop: `DROP DATABASE IF EXISTS "{name}" WITH (FORCE)`,
		ident: `"%s"`, table: plainTable, releasedLog: releasedLogColumns, releasedRow: releasedRowValues,
	}
	mysqlRunLog = runLogEngine{
		name: "mysql", engine: dbtarget.MySQL,
		// The MySQL family's test account may create only this prefix.
		create: "CREATE DATABASE `{name}`", drop: "DROP DATABASE IF EXISTS `{name}`",
		ident: "`%s`", table: plainTable, releasedLog: releasedLogColumns, releasedRow: releasedRowValues,
	}
	mariadbRunLog = runLogEngine{
		name: "mariadb", engine: dbtarget.MariaDB,
		create: "CREATE DATABASE `{name}`", drop: "DROP DATABASE IF EXISTS `{name}`",
		ident: "`%s`", table: plainTable, releasedLog: releasedLogColumns, releasedRow: releasedRowValues,
	}
	clickhouseRunLog = runLogEngine{
		name: "clickhouse", engine: dbtarget.ClickHouse,
		create: "CREATE DATABASE `{name}`", drop: "DROP DATABASE IF EXISTS `{name}`",
		ident: "`%s`", table: "CREATE TABLE {t} (id Int32) ENGINE = MergeTree ORDER BY id",
		releasedLog: releasedLogColumns + " ENGINE = MergeTree", releasedRow: releasedRowValues,
	}
	cockroachRunLog = runLogEngine{
		name: "cockroachdb", engine: dbtarget.CockroachDB,
		create: `CREATE DATABASE "{name}"`, drop: `DROP DATABASE IF EXISTS "{name}" CASCADE`,
		ident: `"%s"`, table: plainTable, releasedLog: releasedLogColumns, releasedRow: releasedRowValues,
	}
	yugabyteRunLog = runLogEngine{
		name: "yugabytedb", engine: dbtarget.YugabyteDB,
		create: `CREATE DATABASE "{name}"`, drop: `DROP DATABASE IF EXISTS "{name}"`,
		ident: `"%s"`, table: plainTable, releasedLog: releasedLogColumns, releasedRow: releasedRowValues,
	}
	sqlserverRunLog = runLogEngine{
		name: "sql server", engine: dbtarget.SQLServer,
		create: "CREATE DATABASE [{name}]",
		drop: "IF DB_ID('{name}') IS NOT NULL BEGIN ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
			"DROP DATABASE [{name}] END",
		ident: "[%s]", table: plainTable,
		releasedLog: `CREATE TABLE {log} (
    run_id NVARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation NVARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state NVARCHAR(32) NOT NULL,
    actor NVARCHAR(256) NULL,
    actor_source NVARCHAR(32) NOT NULL,
    checksum NVARCHAR(64) NULL,
    logged_at DATETIME2 NOT NULL,
    error NVARCHAR(MAX) NULL,
    PRIMARY KEY (run_id, seq)
)`,
		releasedRow: releasedRowValues,
	}
	oracleRunLog = runLogEngine{
		name: "oracle", engine: dbtarget.Oracle,
		ident: `"%s"`, table: plainTable,
		releasedLog: `CREATE TABLE {log} (
    run_id VARCHAR2(64) NOT NULL,
    seq NUMBER(19) NOT NULL,
    operation VARCHAR2(32) NOT NULL,
    version NUMBER(19) NOT NULL,
    state VARCHAR2(32) NOT NULL,
    actor VARCHAR2(256) NULL,
    actor_source VARCHAR2(32) NOT NULL,
    checksum VARCHAR2(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error CLOB NULL,
    PRIMARY KEY (run_id, seq)
)`,
		releasedRow: `INSERT INTO {log} (run_id, seq, operation, version, state, actor, actor_source, logged_at)
VALUES ('released-run', 1, 'up', 9, 'started', 'bob', 'provided', TIMESTAMP '2026-10-01 12:00:00')`,
	}
)

// runLogEngines is every built-in engine the integration workflow serves.
var runLogEngines = []runLogEngine{
	postgresRunLog, mysqlRunLog, mariadbRunLog, clickhouseRunLog,
	cockroachRunLog, yugabyteRunLog, sqlserverRunLog, oracleRunLog,
}

// runLogTarget is the database a test runs against, and a direct handle on it
// with nothing of the command's in between.
type runLogTarget struct {
	engine runLogEngine
	url    string
	conn   *dbschema.DatabaseConnection
	// suffix makes each name the test creates its own.
	suffix string
}

// newRunLogTarget creates the test's own database where the engine's test
// account may.
func newRunLogTarget(c *qt.C, engine runLogEngine) *runLogTarget {
	c.Helper()
	address := dbtarget.URL(c, engine.engine)
	target := &runLogTarget{engine: engine, url: address, suffix: strconv.FormatInt(time.Now().UnixNano(), 10)}
	if engine.create != "" {
		target.url = ownDatabase(c, engine, address, "ptah_rt_runlog_"+target.suffix)
	}
	conn, err := dbschema.ConnectToDatabase(c.Context(), target.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	target.conn = conn
	return target
}

// ownDatabase creates the database and returns its address. It is dropped
// when the test ends.
func ownDatabase(c *qt.C, engine runLogEngine, address, name string) string {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), strings.ReplaceAll(engine.create, "{name}", name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), strings.ReplaceAll(engine.drop, "{name}", name))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	renamed, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	return renamed
}

// name is a table name of this test's own.
func (r *runLogTarget) name(base string) string {
	return base + "_" + r.suffix
}

// quoted is a table Ptah created, spelled as Ptah spells it.
func (r *runLogTarget) quoted(table string) string {
	return fmt.Sprintf(r.engine.ident, table)
}

// createTable is the engine's statement creating a table of that name.
func (r *runLogTarget) createTable(table string) string {
	return strings.ReplaceAll(r.engine.table, "{t}", table)
}

// exec runs the statements with nothing of the command's in between.
func (r *runLogTarget) exec(c *qt.C, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := r.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// dropAfterwards drops the tables when the test ends, for the target whose
// database outlives it. Each is spelled as its creator spelled it.
func (r *runLogTarget) dropAfterwards(c *qt.C, tables ...string) {
	c.Helper()
	c.Cleanup(func() {
		for _, table := range tables {
			_, _ = r.conn.ExecContext(context.Background(), "DROP TABLE "+table)
		}
	})
}

// tableExists asks the server whether a table is there.
func (r *runLogTarget) tableExists(c *qt.C, table string) bool {
	c.Helper()
	_, err := r.conn.ExecContext(c.Context(), "SELECT 1 FROM "+table+" WHERE 1 = 0")
	return err == nil
}

// newRunReference is a reference in the form Ptah Operator passes, its
// operation ID, and one no other run carries.
func newRunReference(c *qt.C) string {
	c.Helper()
	var random [32]byte
	_, err := rand.Read(random[:])
	c.Assert(err, qt.IsNil)
	return "sha256:" + hex.EncodeToString(random[:])
}

// attribution is what a caller names a run with.
func attribution(reference string) []string {
	return []string{"--actor", "alice@example.com", "--actor-origin", "admission", "--run-reference", reference}
}

// savedPlan plans against the target, so the plan's fingerprint is the
// target's, and replaces the planned statements with the ones given: what a
// plan holds is what it holds, and the server's answer to them is what is
// measured. It returns the plan file and the plan as written.
func (r *runLogTarget) savedPlan(c *qt.C, statements ...string) (string, atlasschema.PlanFile, []byte) {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	path := filepath.Join(dir, "change.plan.json")
	c.Assert(os.WriteFile(schema, []byte(r.createTable(r.name("planned"))+";\n"), 0o600), qt.IsNil)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "plan", "--db-url", r.url, "--schema-file", schema, "--output", path)
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
	return path, plan, edited
}

// apply runs `ptah schema apply --json` and decodes the document.
func (r *runLogTarget) apply(c *qt.C, args ...string) (clirun.Result, atlasschema.ApplyReport) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"schema", "apply", "--db-url", r.url, "--auto-approve", "--json"}, args...)...)
	var document atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return result, document
}

// applyLogRow is one row of ptah_schema_apply_log as a caller reads it, NULL
// read as empty or zero.
type applyLogRow struct {
	State          string
	Actor          string
	ActorSource    string
	ActorOrigin    string
	PlanName       string
	PlanDigest     string
	StatementCount int
	ErrorPosition  int
	ErrorStatement string
	Error          string
}

// applyLog reads the rows carrying the reference in the order the run wrote
// them, and the distinct run IDs they carry.
func (r *runLogTarget) applyLog(c *qt.C, reference string) ([]applyLogRow, []string) {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(), `SELECT run_id, state, actor, actor_source, actor_origin, plan_name,
plan_digest, statement_count, error_position, error_stmt, error
FROM `+r.quoted("ptah_schema_apply_log")+` WHERE run_reference = '`+reference+`' ORDER BY seq`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var (
		found  []applyLogRow
		runIDs []string
	)
	for rows.Next() {
		var (
			row                                                 applyLogRow
			runID                                               string
			actor, origin, name, planDigest, statement, message sql.NullString
			count                                               int64
			position                                            sql.NullInt64
		)
		c.Assert(rows.Scan(&runID, &row.State, &actor, &row.ActorSource, &origin, &name,
			&planDigest, &count, &position, &statement, &message), qt.IsNil)
		row.Actor, row.ActorOrigin, row.PlanName, row.PlanDigest = actor.String, origin.String, name.String, planDigest.String
		row.StatementCount, row.ErrorPosition = int(count), int(position.Int64)
		row.ErrorStatement, row.Error = statement.String, message.String
		found = append(found, row)
		if !slices.Contains(runIDs, runID) {
			runIDs = append(runIDs, runID)
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found, runIDs
}

// migrationsDir writes one migration creating a table of the test's own.
func (r *runLogTarget) migrationsDir(c *qt.C, table string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_notes.up.sql"), []byte(r.createTable(table)+";\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_notes.down.sql"), []byte("DROP TABLE "+table+";\n"), 0o600), qt.IsNil)
	return dir
}

// up runs `ptah migrations up --json` and decodes the document.
func (r *runLogTarget) up(c *qt.C, dir, revisions string, args ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append([]string{
		"migrations", "up", "--db-url", r.url, "--migrations-dir", dir, "--migrations-table", revisions, "--json",
	}, args...)...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// migrationLogRow is one row of an operation log as a caller reads it.
type migrationLogRow struct {
	Operation    string
	Version      int64
	State        string
	Actor        string
	ActorSource  string
	ActorOrigin  string
	RunReference string
}

// migrationLog reads the operation log's rows of one run, found by the run ID
// or the reference the where clause names, in the order the run wrote them.
func (r *runLogTarget) migrationLog(c *qt.C, log, where string) []migrationLogRow {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(), `SELECT operation, version, state, actor, actor_source, actor_origin,
run_reference FROM `+log+` WHERE `+where+` ORDER BY seq`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []migrationLogRow
	for rows.Next() {
		var (
			row                      migrationLogRow
			actor, origin, reference sql.NullString
		)
		c.Assert(rows.Scan(&row.Operation, &row.Version, &row.State, &actor, &row.ActorSource, &origin, &reference), qt.IsNil)
		row.Actor, row.ActorOrigin, row.RunReference = actor.String, origin.String, reference.String
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// logRecord is the part of `ptah migrations log --json` these tests read.
type logRecord struct {
	RunID        string `json:"run_id"`
	Version      int64  `json:"version"`
	Outcome      string `json:"outcome"`
	Actor        string `json:"actor"`
	ActorOrigin  string `json:"actor_origin"`
	RunReference string `json:"run_reference"`
}

// readLogDocument runs `ptah migrations log --json` and returns its records by
// the run reference they carry, a run given none under the empty string.
func (r *runLogTarget) readLogDocument(c *qt.C, revisions string) map[string]logRecord {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "log", "--json", "--db-url", r.url, "--migrations-table", revisions)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	var records []logRecord
	c.Assert(json.Unmarshal([]byte(result.Stdout), &records), qt.IsNil, qt.Commentf("%s", result.Stdout))
	byReference := make(map[string]logRecord, len(records))
	for _, record := range records {
		byReference[record.RunReference] = record
	}
	c.Assert(byReference, qt.HasLen, len(records), qt.Commentf("two records carry one reference: %s", result.Stdout))
	return byReference
}

// fileDigest is the digest of a plan file's bytes, as the run reads them.
func fileDigest(contents []byte) string {
	return digest.FromBytes(contents).String()
}

// inspect runs `ptah schema inspect --format sql` against the target.
func (r *runLogTarget) inspect(c *qt.C) string {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "inspect", "--db-url", r.url, "--format", "sql")
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	return result.Stdout
}

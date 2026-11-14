package schema_test

import (
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

const applyLogOperationID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// appliedLogRow is one row of ptah_schema_apply_log as a caller reads it, NULL
// read as empty or zero.
type appliedLogRow struct {
	State          string
	RunReference   string
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

// readApplyLog reads the schema apply log of a SQLite database in the order
// the apply wrote it.
func readApplyLog(c *qt.C, dbPath string) []appliedLogRow {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), `SELECT state, run_reference, actor, actor_source, actor_origin,
plan_name, plan_digest, statement_count, error_position, error_stmt, error
FROM ptah_schema_apply_log ORDER BY seq`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []appliedLogRow
	for rows.Next() {
		var (
			row                                                    appliedLogRow
			reference, actor, origin, name, planDigest, query, msg sql.NullString
			position                                               sql.NullInt64
		)
		c.Assert(rows.Scan(&row.State, &reference, &actor, &row.ActorSource, &origin,
			&name, &planDigest, &row.StatementCount, &position, &query, &msg), qt.IsNil)
		row.RunReference, row.Actor, row.ActorOrigin = reference.String, actor.String, origin.String
		row.PlanName, row.PlanDigest, row.ErrorStatement, row.Error = name.String, planDigest.String, query.String, msg.String
		row.ErrorPosition = int(position.Int64)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// attribution is the run attribution every row here passes.
var attribution = []string{
	"--actor", "alice@example.com", "--actor-origin", "admission", "--run-reference", applyLogOperationID,
}

// A computed plan that ran is recorded in the target as its start and its
// outcome, each with who the caller said it is and the caller's reference, and
// the document reports the reference as stored.
func TestSchemaApplyRecordsAComputedPlanInTheTarget_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)

	stdout, stderr, err := runSchemaStreams(append([]string{"apply",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--auto-approve", "--json"}, attribution...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.ApplyLog, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: applyLogOperationID})
	written := appliedLogRow{
		RunReference: applyLogOperationID, Actor: "alice@example.com", ActorSource: string(migrator.ActorProvided),
		ActorOrigin: "admission", StatementCount: 1,
	}
	started, applied := written, written
	started.State, applied.State = "started", "applied"
	c.Assert(readApplyLog(c, f.dbPath), qt.DeepEquals, []appliedLogRow{started, applied})
}

// A saved plan is recorded by the name it carries and the digest of the bytes
// the run read, the same two the document names.
func TestSchemaApplyRecordsWhichPlanFileItApplied_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)

	stdout, stderr, err := runSchemaStreams(append([]string{"apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json"}, attribution...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.ApplyLog, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: applyLogOperationID})
	rows := readApplyLog(c, f.dbPath)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[1].State, qt.Equals, "applied")
	c.Assert(rows[1].PlanName, qt.Equals, plan.Name)
	c.Assert(rows[1].PlanDigest, qt.Equals, fileDigest(c, planPath))
	c.Assert(rows[0].PlanDigest, qt.Equals, rows[1].PlanDigest)
}

// A plan whose second statement fails is recorded as failed, with the
// position and the text of the statement it stopped at and what the database
// said, and the run still exits with the apply's own failure.
func TestSchemaApplyRecordsWhereAFailedApplyStopped_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	plan.Statements = append(plan.Statements, plan.Statements[0])
	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, document, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaStreams(append([]string{"apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json"}, attribution...)...)

	c.Assert(err, qt.ErrorMatches, "(?s)apply schema changes: .*", qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
	c.Assert(report.ApplyLog, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: applyLogOperationID})
	rows := readApplyLog(c, f.dbPath)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[0].State, qt.Equals, "started")
	c.Assert(rows[1], qt.DeepEquals, appliedLogRow{
		State: "failed", RunReference: applyLogOperationID, Actor: "alice@example.com",
		ActorSource: string(migrator.ActorProvided), ActorOrigin: "admission",
		PlanName: plan.Name, PlanDigest: digest.FromBytes(document).String(), StatementCount: 2,
		ErrorPosition: 2, ErrorStatement: plan.StatementSQL()[1], Error: err.Error(),
	})
}

// A plan saved before any run created the log still applies after a failed run
// left one: retrying it is the same plan against the same schema, and the log
// is not part of that schema.
func TestSchemaApplyAppliesAPlanSavedBeforeTheLogExisted_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	failing, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	failing.Statements = append(slices.Clone(failing.Statements), failing.Statements[0])
	document, err := atlasschema.MarshalPlanFile(failing)
	c.Assert(err, qt.IsNil)
	failingPath := filepath.Join(f.dir, "failing.plan.json")
	c.Assert(os.WriteFile(failingPath, document, 0o600), qt.IsNil)
	_, stderr, err := runSchemaStreams("apply", "--db-url", f.dbURL, "--plan", failingPath, "--auto-approve", "--json")
	c.Assert(err, qt.ErrorMatches, "(?s)apply schema changes: .*", qt.Commentf("stderr:\n%s", stderr))
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"ptah_schema_apply_log", "users"})

	stdout, stderr, err := runSchemaStreams("apply", "--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"orders", "ptah_schema_apply_log", "users"})
}

// A run that sent no statement reached nothing, so it writes no entry, creates
// no table, and its document carries no log report, whichever way it ended.
func TestSchemaApplyRecordsNothingOfARunThatSentNothing_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		stdin       string
		wantOutcome atlasschema.ApplyOutcome
	}{
		{name: "a dry run", args: []string{"--dry-run"}, wantOutcome: atlasschema.ApplyOutcomeDryRun},
		{name: "a declined prompt", stdin: "no\n", wantOutcome: atlasschema.ApplyOutcomeCanceled},
		{name: "a failure before dispatch", args: []string{"--auto-approve", "--include", "no_such_table"}, wantOutcome: atlasschema.ApplyOutcomeFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPlanFixture(c)
			args := append([]string{"apply", "--db-url", f.dbURL, "--schema-file", f.schemaPath, "--json"}, attribution...)

			stdout, stderr, _ := runSchemaStreamsWithInput(test.stdin, append(args, test.args...)...)

			report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
			c.Assert(report.Outcome, qt.Equals, test.wantOutcome, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(report.ApplyLog, qt.IsNil)
			c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
		})
	}
}

// A refusal names a target that does not match the plan, so the plan never
// starts, and nothing is recorded of it.
func TestSchemaApplyRecordsNothingOfARefusedPlan_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	seedSQLite(c, f.dbPath, "CREATE TABLE drifted (id INTEGER PRIMARY KEY);")

	stdout, _, err := runSchemaStreams(append([]string{"apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json"}, attribution...)...)

	c.Assert(err, qt.ErrorMatches, "pre-planned migration is stale: .*")
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
	c.Assert(report.ApplyLog, qt.IsNil)
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"drifted", "users"})
}

// The project can keep no such table. The apply runs as it would, and the
// document does not report a log nothing writes.
func TestSchemaApplyRecordsNothingWhereTheProjectTurnedTheLogOff_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	config := writeSchemaSQLFile(c, f.dir, "ptah.yaml", "schema_apply:\n  log: false\n")

	stdout, stderr, err := runSchemaStreams(append([]string{"apply", "--config", config,
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--auto-approve", "--json"}, attribution...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.ApplyLog, qt.IsNil)
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"orders", "users"})
}

// A run attribution the log could not store as given is refused before the
// target is opened: the SQLite file it names is never created.
func TestSchemaApplyRefusesARunAttributionTheLogCannotStore_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{
			name:      "a reference longer than the column",
			args:      []string{"--run-reference", strings.Repeat("r", 256)},
			wantError: `--run-reference: invalid run attribution: a run reference is at most 255 characters, and this one has 256`,
		},
		{
			name:      "an origin that is not one word",
			args:      []string{"--actor", "alice@example.com", "--actor-origin", "Admission"},
			wantError: `--actor-origin: invalid run attribution: .*`,
		},
		{
			name:      "an origin with no actor",
			args:      []string{"--actor-origin", "admission"},
			wantError: `--actor-origin says where the --actor name came from, and no --actor was given`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dbPath := filepath.Join(dir, "never.db")
			schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
			args := append([]string{"apply", "--db-url", "sqlite://" + dbPath, "--schema-file", schemaPath,
				"--auto-approve", "--json"}, test.args...)

			stdout, _, err := runSchemaStreams(args...)

			c.Assert(err, qt.ErrorMatches, test.wantError)
			report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(report.ApplyLog, qt.IsNil)
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

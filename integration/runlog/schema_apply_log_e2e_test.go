//go:build integration

package runlog_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

// longStatement is longer than a MySQL TEXT column holds, 65,535 bytes, so a
// log that kept the statement in one would lose the entry that matters most.
const longStatement = 70_000

// A plan that ran is recorded as its start and its outcome, each with the
// caller's attribution and reference, the plan's name and the digest of the
// file the run read. No reader takes the log for a table of the schema.
func TestSchemaApplyLogRecordsAPlanThatRanE2E(t *testing.T) {
	for _, engine := range runLogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRunLogTarget(c, engine)
			notes := target.name("notes")
			target.dropAfterwards(c, notes)
			reference := newRunReference(c)
			planPath, plan, written := target.savedPlan(c, target.createTable(notes))

			result, document := target.apply(c, append([]string{"--plan", planPath}, attribution(reference)...)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
			c.Assert(document.ApplyLog, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: reference})
			rows, runIDs := target.applyLog(c, reference)
			c.Assert(runIDs, qt.HasLen, 1)
			entry := applyLogRow{
				Actor: "alice@example.com", ActorSource: string(migrator.ActorProvided), ActorOrigin: "admission",
				PlanName: plan.Name, PlanDigest: fileDigest(written), StatementCount: 1,
			}
			started, applied := entry, entry
			started.State, applied.State = string(migrator.SchemaApplyLogStarted), string(migrator.SchemaApplyLogApplied)
			c.Assert(rows, qt.DeepEquals, []applyLogRow{started, applied})
			inspected := strings.ToLower(target.inspect(c))
			c.Assert(inspected, qt.Contains, notes)
			c.Assert(inspected, qt.Not(qt.Contains), "ptah_schema_apply_log")
		})
	}
}

// A plan whose second statement fails is recorded where it stopped: the
// position, the whole statement and what the server said, the error the
// document reports. A run the engine took back is failed; a run whose first
// statement outlived the failure is unknown, as the document says.
func TestSchemaApplyLogRecordsWhereAFailedPlanStoppedE2E(t *testing.T) {
	tests := []struct {
		name        string
		engine      runLogEngine
		mode        string
		wantOutcome atlasschema.ApplyOutcome
		wantState   migrator.SchemaApplyLogState
	}{
		{
			name: "postgresql in a transaction", engine: postgresRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantState: migrator.SchemaApplyLogFailed,
		},
		{
			name: "postgresql without a transaction", engine: postgresRunLog, mode: "none",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "mysql, whose schema statements commit themselves", engine: mysqlRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "mariadb, whose schema statements commit themselves", engine: mariadbRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "clickhouse, which has no transaction", engine: clickhouseRunLog, mode: "none",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "cockroachdb, whose schema statement outlives the rollback", engine: cockroachRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "yugabytedb, whose schema statement outlives the rollback", engine: yugabyteRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
		{
			name: "sql server in a transaction", engine: sqlserverRunLog, mode: "file",
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantState: migrator.SchemaApplyLogFailed,
		},
		{
			name: "oracle, whose schema statements commit themselves", engine: oracleRunLog, mode: "none",
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantState: migrator.SchemaApplyLogUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRunLogTarget(c, test.engine)
			first, missing, third := target.name("first_unit"), target.name("missing"), target.name("third_unit")
			target.dropAfterwards(c, first, third)
			reference := newRunReference(c)
			failing := "INSERT INTO " + missing + " (id) VALUES ('" + strings.Repeat("x", longStatement) + "')"
			planPath, plan, written := target.savedPlan(c, target.createTable(first), failing, target.createTable(third))

			result, document := target.apply(c, append([]string{"--plan", planPath, "--tx-mode", test.mode},
				attribution(reference)...)...)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, test.wantOutcome)
			c.Assert(document.ApplyLog, qt.DeepEquals, &migrator.SchemaApplyLogReport{Written: 2, RunReference: reference})
			c.Assert(document.Error, qt.IsNotNil)
			rows, runIDs := target.applyLog(c, reference)
			c.Assert(runIDs, qt.HasLen, 1)
			entry := applyLogRow{
				Actor: "alice@example.com", ActorSource: string(migrator.ActorProvided), ActorOrigin: "admission",
				PlanName: plan.Name, PlanDigest: fileDigest(written), StatementCount: 3,
			}
			started, stopped := entry, entry
			started.State = string(migrator.SchemaApplyLogStarted)
			stopped.State, stopped.ErrorPosition, stopped.Error = string(test.wantState), 2, document.Error.Message
			stopped.ErrorStatement = failing
			c.Assert(rows, qt.HasLen, 2)
			c.Assert(rows[0], qt.DeepEquals, started)
			c.Assert(rows[1], qt.DeepEquals, stopped)
			c.Assert(target.tableExists(c, third), qt.IsFalse)
		})
	}
}

// A refused plan never started, so the run writes nothing: no entry, and no
// table to hold one. Each engine here gives the test a database of its own, in
// which no other run created the table.
func TestSchemaApplyLogKeepsNothingOfARefusedPlanE2E(t *testing.T) {
	engines := []runLogEngine{
		postgresRunLog, mysqlRunLog, mariadbRunLog, clickhouseRunLog,
		cockroachRunLog, yugabyteRunLog, sqlserverRunLog,
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRunLogTarget(c, engine)
			notes, drifted := target.name("notes"), target.name("drifted")
			planPath, _, _ := target.savedPlan(c, target.createTable(notes))
			target.exec(c, target.createTable(drifted))

			result, document := target.apply(c, append([]string{"--plan", planPath},
				attribution(newRunReference(c))...)...)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(document.ApplyLog, qt.IsNil)
			c.Assert(target.tableExists(c, target.quoted("ptah_schema_apply_log")), qt.IsFalse)
			c.Assert(target.tableExists(c, notes), qt.IsFalse)
			// The control: the same question finds the table the drift created.
			c.Assert(target.tableExists(c, drifted), qt.IsTrue)
		})
	}
}

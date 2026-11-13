//go:build integration

package runlog_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// runLogEntries is the start and the outcome one migration up writes with the
// attribution every row here passes.
func runLogEntries(reference string) []migrationLogRow {
	entry := migrationLogRow{
		Operation: "up", Version: 1, Actor: "alice@example.com", ActorSource: string(migrator.ActorProvided),
		ActorOrigin: "admission", RunReference: reference,
	}
	started, applied := entry, entry
	started.State, applied.State = string(migrator.MigrationLogStarted), string(migrator.MigrationLogApplied)
	return []migrationLogRow{started, applied}
}

// The reference and the origin a caller gives are on both entries of the run,
// as the server holds them, and both documents report them: the run's own and
// `ptah migrations log --json`.
func TestMigrationsUpStoresTheRunReferenceE2E(t *testing.T) {
	for _, engine := range runLogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRunLogTarget(c, engine)
			revisions, notes := target.name("runlog"), target.name("notes")
			log := target.quoted(revisions + "_log")
			target.dropAfterwards(c, notes, target.quoted(revisions), log)
			reference := newRunReference(c)

			result, document := target.up(c, target.migrationsDir(c, notes), revisions, attribution(reference)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{Written: 2, RunReference: reference})
			c.Assert(target.migrationLog(c, log, "run_reference = '"+reference+"'"), qt.DeepEquals, runLogEntries(reference))
			records := target.readLogDocument(c, revisions)
			c.Assert(records, qt.HasLen, 1)
			c.Assert(records[reference].Outcome, qt.Equals, string(migrator.MigrationLogApplied))
			c.Assert(records[reference].ActorOrigin, qt.Equals, "admission")
		})
	}
}

// An operation log a released Ptah created has neither column. The first run
// that writes to it adds both, keeps the row the release wrote, and the
// release's row reads back with neither, in the table and in the document.
func TestMigrationsUpUpgradesALogTableAReleaseCreatedE2E(t *testing.T) {
	for _, engine := range runLogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newRunLogTarget(c, engine)
			revisions, notes := target.name("runlog"), target.name("notes")
			log := target.quoted(revisions + "_log")
			target.dropAfterwards(c, notes, target.quoted(revisions), log)
			target.exec(c, strings.ReplaceAll(engine.releasedLog, "{log}", log), strings.ReplaceAll(engine.releasedRow, "{log}", log))
			reference := newRunReference(c)

			result, document := target.up(c, target.migrationsDir(c, notes), revisions, attribution(reference)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(document.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{Written: 2, RunReference: reference})
			c.Assert(target.migrationLog(c, log, "run_reference = '"+reference+"'"), qt.DeepEquals, runLogEntries(reference))
			c.Assert(target.migrationLog(c, log, "run_id = 'released-run'"), qt.DeepEquals, []migrationLogRow{{
				Operation: "up", Version: 9, State: string(migrator.MigrationLogStarted), Actor: "bob",
				ActorSource: string(migrator.ActorProvided),
			}})
			records := target.readLogDocument(c, revisions)
			c.Assert(records, qt.HasLen, 2)
			c.Assert(records[""].RunID, qt.Equals, "released-run")
			c.Assert(records[""].ActorOrigin, qt.Equals, "")
			c.Assert(records[reference].ActorOrigin, qt.Equals, "admission")
		})
	}
}

package migrator_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

func TestRunResultRetainsIndependentApprovedInputReferences(t *testing.T) {
	c := qt.New(t)
	plan := migrator.MigrationPlan{Inputs: []migrator.MigrationInput{{Version: 42, VersionKey: "42_rotate", StatementIndex: 2, StatementDigest: "sha256:statement",
		Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DB_PASSWORD"}}}}}
	result := migrator.NewRunResult(migrator.RunEvidence{Plan: &plan})
	plan.Inputs[0].Inputs[0].Name = "CHANGED"
	plan.Inputs[0].StatementDigest = "changed"
	c.Assert(result.Inputs, qt.HasLen, 1)
	c.Assert(result.Inputs[0].Inputs[0].Name, qt.Equals, "DB_PASSWORD")
	c.Assert(result.Inputs[0].StatementDigest, qt.Equals, "sha256:statement")
	c.Assert(result.Inputs[0].VersionKey, qt.Equals, "42_rotate")
}

// TestNewRunResult_ClassifiesFromTheEvidence pins the rule that decides what a
// run did. Every row carries evidence a caller could actually hold: a dirty row
// that committed nothing, one that committed some, a status that could not be
// read.
func TestNewRunResult_ClassifiesFromTheEvidence(t *testing.T) {
	runErr := errors.New("migration failed")
	tests := []struct {
		name     string
		evidence migrator.RunEvidence
		outcome  migrator.RunOutcome
	}{
		{
			name: "nothing pending",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				After:     &migrator.MigrationStatus{},
			},
			outcome: migrator.RunOutcomeUpToDate,
		},
		{
			name: "every selected migration applied",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1, 2}},
				After:     &migrator.MigrationStatus{AppliedMigrations: []int64{1, 2}},
			},
			outcome: migrator.RunOutcomeApplied,
		},
		{
			name: "dry run changes nothing",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1, 2}},
				After:     &migrator.MigrationStatus{},
				DryRun:    true,
			},
			outcome: migrator.RunOutcomeDryRun,
		},
		{
			name: "transactional failure committed nothing",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1, 2}},
				After: &migrator.MigrationStatus{
					AppliedMigrations: []int64{1},
					DirtyRevision:     &migrator.MigrationRevision{Version: 2, Applied: 0, Total: 3},
				},
				Err: runErr,
			},
			outcome: migrator.RunOutcomeFailed,
		},
		{
			name: "non-transactional failure committed some",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1, 2}},
				After: &migrator.MigrationStatus{
					AppliedMigrations: []int64{1},
					DirtyRevision:     &migrator.MigrationRevision{Version: 2, Applied: 2, Total: 3},
				},
				Err: runErr,
			},
			outcome: migrator.RunOutcomePartial,
		},
		{
			name: "dirty row that counts nothing at all",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1}},
				After: &migrator.MigrationStatus{
					DirtyRevision: &migrator.MigrationRevision{Version: 1},
				},
				Err: runErr,
			},
			outcome: migrator.RunOutcomeUnknown,
		},
		{
			name: "status could not be read",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1}},
				Err:       runErr,
			},
			outcome: migrator.RunOutcomeUnknown,
		},
		{
			name: "clean failure before anything was selected",
			evidence: migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1, 2}},
				After:     &migrator.MigrationStatus{},
				Err:       runErr,
			},
			outcome: migrator.RunOutcomeFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := migrator.NewRunResult(test.evidence)

			c.Assert(result.Outcome, qt.Equals, test.outcome)
			c.Assert(result.ContractVersion, qt.Equals, migrator.RunContractVersion)
			c.Assert(result.Direction, qt.Equals, migrator.MigrationDirectionUp)
		})
	}
}

// TestNewRunResult_AppliedNamesOnlyWhatWasSelected keeps the document about
// this run: a migration the history already held is not evidence that this run
// applied it.
func TestNewRunResult_AppliedNamesOnlyWhatWasSelected(t *testing.T) {
	c := qt.New(t)

	result := migrator.NewRunResult(migrator.RunEvidence{
		Plan: &migrator.MigrationPlan{Versions: []int64{2, 3}},
		After: &migrator.MigrationStatus{
			AppliedMigrations: []int64{1, 2, 3},
		},
	})

	c.Assert(result.Applied, qt.DeepEquals, []int64{2, 3})
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
}

// TestRunResultOverARealFailure measures the shapes the classification reads,
// against sqlite, rather than trusting the table above to describe them.
func TestRunResultOverARealFailure(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		outcome   migrator.RunOutcome
		applied   int
		statement int
	}{
		{
			name:      "transactional",
			header:    "",
			outcome:   migrator.RunOutcomeFailed,
			applied:   0,
			statement: 3,
		},
		{
			name:      "no transaction",
			header:    "-- atlas:txmode none\n",
			outcome:   migrator.RunOutcomePartial,
			applied:   2,
			statement: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := runSQLiteConnection(c, "run-"+test.name+".sqlite")
			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_broken.up.sql": {Data: []byte(test.header +
					"CREATE TABLE alpha (id INTEGER PRIMARY KEY);\n" +
					"CREATE TABLE beta (id INTEGER PRIMARY KEY);\n" +
					"NOT SQL;\n")},
				"0000000001_broken.down.sql": {Data: []byte("DROP TABLE alpha;\n")},
			})
			c.Assert(err, qt.IsNil)

			runErr := mig.MigrateUp(c.Context())
			c.Assert(runErr, qt.IsNotNil)
			after, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)

			result := migrator.NewRunResult(migrator.RunEvidence{
				Direction: migrator.MigrationDirectionUp,
				Plan:      &migrator.MigrationPlan{Versions: []int64{1}},
				After:     after,
				Err:       runErr,
			})

			c.Assert(result.Outcome, qt.Equals, test.outcome)
			c.Assert(result.Error, qt.IsNotNil)
			c.Assert(after.DirtyRevision, qt.IsNotNil)
			c.Assert(after.DirtyRevision.Applied, qt.Equals, test.applied)
			c.Assert(after.DirtyRevision.Total, qt.Equals, test.statement)
			// SQLite's driver reports no SQLSTATE, so the failure names the
			// statement and carries no code.
			c.Assert(result.Failure, qt.DeepEquals, &migrator.RunFailure{
				Version:    1,
				VersionKey: "1",
				Statement:  3,
				Statements: 3,
			})
		})
	}
}

// TestRunResultDocumentNamesTheFailedStatement pins the wire names a caller
// that must not read the error message parses instead.
func TestRunResultDocumentNamesTheFailedStatement(t *testing.T) {
	c := qt.New(t)
	conn := runSQLiteConnection(c, "run-failure-document.sqlite")
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\nINSERT INTO absent VALUES (1);\n")},
		"0000000002_second.down.sql": {Data: []byte("DROP TABLE second;\n")},
	})
	c.Assert(err, qt.IsNil)
	runErr := mig.MigrateUp(c.Context())
	c.Assert(runErr, qt.IsNotNil)
	after, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)

	encoded, err := json.Marshal(migrator.NewRunResult(migrator.RunEvidence{
		Plan:  &migrator.MigrationPlan{Versions: []int64{1, 2}},
		After: after,
		Err:   runErr,
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"failure":{"version":2,"version_key":"2","statement":2,"statements":2}`)
}

// TestRunResultNamesAFailedCommit fails the commit with a deferred foreign key,
// which SQLite answers there. Every statement ran, so the record names the commit
// and no position, and under one transaction for the run it names the last
// migration of the batch.
func TestRunResultNamesAFailedCommit(t *testing.T) {
	tests := []struct {
		name string
		mode migrator.MigrationTxMode
	}{
		{name: "the per-file transaction", mode: migrator.MigrationTxModeFile},
		{name: "one transaction for the run", mode: migrator.MigrationTxModeAll},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := runSQLiteConnection(c, "run-commit.sqlite")
			_, err := conn.ExecContext(c.Context(), "CREATE TABLE parents (id INTEGER PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(c.Context(), "CREATE TABLE children (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents (id) DEFERRABLE INITIALLY DEFERRED)")
			c.Assert(err, qt.IsNil)
			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_plain.up.sql":      {Data: []byte("CREATE TABLE plain (id INTEGER PRIMARY KEY);\n")},
				"0000000001_plain.down.sql":    {Data: []byte("DROP TABLE plain;\n")},
				"0000000002_deferred.up.sql":   {Data: []byte("INSERT INTO children (id, parent) VALUES (1, 999);\nINSERT INTO children (id, parent) VALUES (2, 999);\n")},
				"0000000002_deferred.down.sql": {Data: []byte("DELETE FROM children;\n")},
			})
			c.Assert(err, qt.IsNil)

			runErr := mig.WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(runErr, qt.IsNotNil)
			result := migrator.NewRunResult(migrator.RunEvidence{Err: runErr})
			c.Assert(result.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statements: 2, Commit: true})
		})
	}
}

// TestRunResultKeepsTheStatementWhenItsFailureCannotBeRecorded fails a statement of
// a batch, and then the write that records it, which a trigger refuses. The
// message says both, and the statement is still what stopped the run.
func TestRunResultKeepsTheStatementWhenItsFailureCannotBeRecorded(t *testing.T) {
	c := qt.New(t)
	conn := runSQLiteConnection(c, "run-unrecorded.sqlite")
	files := fstest.MapFS{
		"0000000001_plain.up.sql":   {Data: []byte("CREATE TABLE plain (id INTEGER PRIMARY KEY);\n")},
		"0000000001_plain.down.sql": {Data: []byte("DROP TABLE plain;\n")},
	}
	mig, err := migrator.NewFSMigrator(conn, files)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	for _, operation := range []string{"INSERT", "UPDATE"} {
		_, err = conn.ExecContext(c.Context(), "CREATE TRIGGER refuse_"+operation+" BEFORE "+operation+
			" ON schema_migrations BEGIN SELECT RAISE(ABORT, 'no revision rows'); END")
		c.Assert(err, qt.IsNil)
	}
	files["0000000002_broken.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\nNOT SQL;\n")}
	files["0000000002_broken.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE second;\n")}
	mig, err = migrator.NewFSMigrator(conn, files)
	c.Assert(err, qt.IsNil)

	runErr := mig.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(c.Context())

	c.Assert(runErr, qt.ErrorMatches, "(?s).*additionally failed to record pending migration 2.*")
	result := migrator.NewRunResult(migrator.RunEvidence{Err: runErr})
	c.Assert(result.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 2, Statements: 2})
}

// TestRunResultHasNoFailureForARefusalBeforeAnyStatement keeps the failure
// object about statements the database saw. A statement validator refuses a
// statement by position too, before any statement runs, so the database never
// answered it and there is nothing for the object to report.
func TestRunResultHasNoFailureForARefusalBeforeAnyStatement(t *testing.T) {
	c := qt.New(t)
	conn := runSQLiteConnection(c, "run-refused.sqlite")
	refusal := errors.New("unsafe second statement")
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_refused.up.sql": {Data: []byte(
			"CREATE TABLE alpha (id INTEGER PRIMARY KEY);\n" +
				"ATTACH DATABASE 'other.db' AS aux;\n",
		)},
		"0000000001_refused.down.sql": {Data: []byte("DROP TABLE alpha;\n")},
	}, migrator.WithStatementValidator(&statementValidatorRecorder{
		reject: "ATTACH DATABASE 'other.db' AS aux",
		err:    refusal,
	}))
	c.Assert(err, qt.IsNil)
	runErr := mig.MigrateUp(c.Context())
	c.Assert(runErr, qt.ErrorIs, refusal)
	after, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)

	result := migrator.NewRunResult(migrator.RunEvidence{
		Plan:  &migrator.MigrationPlan{Versions: []int64{1}},
		After: after,
		Err:   runErr,
	})

	c.Assert(result.Error, qt.IsNotNil)
	c.Assert(result.Failure, qt.IsNil)
}

// TestRunResultDocumentCarriesItsContractVersion pins what a consumer reads
// before it reads anything else.
func TestRunResultDocumentCarriesItsContractVersion(t *testing.T) {
	c := qt.New(t)
	conn := runSQLiteConnection(c, "run-document.sqlite")
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":   {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql": {Data: []byte("DROP TABLE first;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	after, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)

	encoded, err := json.Marshal(migrator.NewRunResult(migrator.RunEvidence{
		Plan:  &migrator.MigrationPlan{Versions: []int64{1}},
		After: after,
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"contract_version":2`)
	c.Assert(string(encoded), qt.Contains, `"outcome":"applied"`)
	c.Assert(string(encoded), qt.Contains, `"direction":"up"`)
	var decoded migrator.RunResult
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.Status, qt.IsNotNil)
	c.Assert(decoded.Status.ContractVersion, qt.Equals, migrator.StatusContractVersion)
}

func runSQLiteConnection(c *qt.C, name string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// A lost log entry says nothing about the migration. The outcome reads the
// revision table, so a caller that retried on this document would rerun nothing
// and bring nothing back.
func TestNewRunResult_ALostLogEntryKeepsTheOutcome(t *testing.T) {
	c := qt.New(t)
	report := &migrator.MigrationLogReport{Written: 2, Lost: []migrator.MigrationLogLoss{{
		Operation:   "up",
		Version:     2,
		VersionKey:  "000002",
		State:       migrator.MigrationLogStarted,
		SQLState:    "22001",
		ErrorNumber: 1406,
		Message:     "Data too long for column 'actor' at row 1",
	}}}

	result := migrator.NewRunResult(migrator.RunEvidence{
		Plan:         &migrator.MigrationPlan{Versions: []int64{1, 2}},
		After:        &migrator.MigrationStatus{AppliedMigrations: []int64{1, 2}},
		MigrationLog: report,
	})

	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.Error, qt.IsNil)
	c.Assert(result.MigrationLog, qt.DeepEquals, report)
}

// The field names are the contract a caller reads, so they are pinned as text.
func TestRunResult_NamesTheLostEntriesInTheDocument(t *testing.T) {
	c := qt.New(t)

	document, err := json.Marshal(migrator.NewRunResult(migrator.RunEvidence{
		After: &migrator.MigrationStatus{},
		MigrationLog: &migrator.MigrationLogReport{Written: 1, Lost: []migrator.MigrationLogLoss{{
			Operation:   "up",
			Version:     2,
			VersionKey:  "000002",
			State:       migrator.MigrationLogApplied,
			SQLState:    "22001",
			ErrorNumber: 1406,
			Message:     "too long",
		}}},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(string(document), qt.Contains, `"migration_log":{"written":1,"lost":[{"operation":"up","version":2,`+
		`"version_key":"000002","state":"applied","sqlstate":"22001","error_number":1406,"message":"too long"}]}`)
}

// Zero entries written is present in the document, which is what separates a run
// that had nothing to record from a run that keeps no log.
func TestRunResult_NamesARunWithNothingToRecord(t *testing.T) {
	c := qt.New(t)

	withLog, err := json.Marshal(migrator.NewRunResult(migrator.RunEvidence{
		After:        &migrator.MigrationStatus{},
		MigrationLog: &migrator.MigrationLogReport{},
	}))
	c.Assert(err, qt.IsNil)
	withoutLog, err := json.Marshal(migrator.NewRunResult(migrator.RunEvidence{
		After: &migrator.MigrationStatus{},
	}))
	c.Assert(err, qt.IsNil)

	c.Assert(string(withLog), qt.Contains, `"migration_log":{"written":0}`)
	c.Assert(string(withoutLog), qt.Not(qt.Contains), "migration_log")
}

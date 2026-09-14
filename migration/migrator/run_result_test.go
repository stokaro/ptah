package migrator_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

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
			c.Assert(result.Error, qt.Not(qt.Equals), "")
			c.Assert(after.DirtyRevision, qt.IsNotNil)
			c.Assert(after.DirtyRevision.Applied, qt.Equals, test.applied)
			c.Assert(after.DirtyRevision.Total, qt.Equals, test.statement)
		})
	}
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
	c.Assert(string(encoded), qt.Contains, `"contract_version":1`)
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

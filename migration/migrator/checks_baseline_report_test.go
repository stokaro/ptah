package migrator_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// newBaselineCheckMigrator builds a migrator whose single migration carries
// upSQL, logging through a handler the test can read back.
func newBaselineCheckMigrator(t *testing.T, upSQL string, messages *[]string) *migrator.Migrator {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "baseline.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	migration := migrator.CreateMigrationFromSQL(1, "create_accounts", upSQL,
		"DROP TABLE accounts;\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithLogger(slog.New(countingHandler{messages: messages}))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	return m
}

// TestBaseline_SaysWhichChecksItDidNotEvaluate answers what baseline does with
// an assertion.
//
// Baseline records a migration as applied without running its body, so there
// was no statement for a precondition to guard and no moment at which a
// postcondition was true. Skipping is the only available answer, and an
// operator who wrote the assertion has to be told it was never asked
// (stokaro/ptah#3405).
func TestBaseline_SaysWhichChecksItDidNotEvaluate(t *testing.T) {
	c := qt.New(t)
	var messages []string
	up := `-- +ptah check name="rows_arrived" phase=after assert="SELECT count(*) > 0 FROM accounts"` + "\n" +
		"CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n"
	m := newBaselineCheckMigrator(t, up, &messages)

	c.Assert(m.Baseline(context.Background(), 1), qt.IsNil)

	c.Assert(countMessages(messages, "without evaluating their checks"), qt.Equals, 1)
}

// TestBaseline_SaysNothingAboutAMigrationWithNoChecks is the control. A report
// that named every baselined migration would say nothing about assertions.
func TestBaseline_SaysNothingAboutAMigrationWithNoChecks(t *testing.T) {
	c := qt.New(t)
	var messages []string
	m := newBaselineCheckMigrator(t, "CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n", &messages)

	c.Assert(m.Baseline(context.Background(), 1), qt.IsNil)

	c.Assert(countMessages(messages, "without evaluating their checks"), qt.Equals, 0)
}

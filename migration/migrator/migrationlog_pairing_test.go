package migrator_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// logRow is one entry of the log, in the order the rows of a test are written.
type logRow struct {
	run     string
	seq     int
	version int
	state   string
	second  int
}

// logAttemptFixture is a SQLite migrator whose log holds exactly rows.
func logAttemptFixture(c *qt.C, rows []logRow) *migrator.Migrator {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "pairing.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n")))
	// A real run creates the table, and the rows below replace what it wrote.
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	_, err = conn.ExecContext(ctx, "DELETE FROM schema_migrations_log")
	c.Assert(err, qt.IsNil)
	base := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	for _, row := range rows {
		_, err := conn.ExecContext(ctx,
			`INSERT INTO schema_migrations_log (run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at)
VALUES (?, ?, 'up', ?, ?, 'release-bot', 'provided', 'sum', ?)`,
			row.run, row.seq, row.version, row.state, base.Add(time.Duration(row.second)*time.Second))
		c.Assert(err, qt.IsNil)
	}
	return m
}

// describeAttempt spells one attempt for a row to compare: the version of each
// entry it holds, the seconds they were written at, and what the two questions
// about it answer.
func describeAttempt(attempt migrator.MigrationLogAttempt) string {
	return fmt.Sprintf("start=%d/%s@%d outcome=%d/%s@%d lost=%t undetermined=%t",
		attempt.Start.Version, attempt.Start.State, attempt.Start.At.Second(),
		attempt.Outcome.Version, attempt.Outcome.State, attempt.Outcome.At.Second(),
		attempt.StartLost(), attempt.Undetermined())
}

// A write to the log fails on its own and stops nothing, so a start can be lost
// while its outcome is kept. The outcome is then the only record the attempt has,
// and it stands where it was written among the others.
func TestMigrationLog_PairsEntriesIntoAttempts(t *testing.T) {
	tests := []struct {
		name string
		rows []logRow
		want []string
	}{
		{
			name: "a start and its outcome",
			rows: []logRow{
				{run: "a", seq: 1, version: 1, state: "started", second: 1},
				{run: "a", seq: 2, version: 1, state: "applied", second: 2},
			},
			want: []string{"start=1/started@1 outcome=1/applied@2 lost=false undetermined=false"},
		},
		{
			name: "a start with no outcome",
			rows: []logRow{{run: "a", seq: 1, version: 1, state: "started", second: 1}},
			want: []string{"start=1/started@1 outcome=0/@0 lost=false undetermined=true"},
		},
		{
			name: "an outcome and nothing else",
			rows: []logRow{{run: "a", seq: 1, version: 1, state: "applied", second: 1}},
			want: []string{"start=0/@0 outcome=1/applied@1 lost=true undetermined=false"},
		},
		{
			name: "an outcome whose start was lost stands between two whole attempts",
			rows: []logRow{
				{run: "a", seq: 1, version: 1, state: "started", second: 1},
				{run: "a", seq: 2, version: 1, state: "applied", second: 2},
				{run: "a", seq: 3, version: 2, state: "applied", second: 3},
				{run: "a", seq: 4, version: 3, state: "started", second: 4},
				{run: "a", seq: 5, version: 3, state: "failed", second: 5},
			},
			want: []string{
				"start=3/started@4 outcome=3/failed@5 lost=false undetermined=false",
				"start=0/@0 outcome=2/applied@3 lost=true undetermined=false",
				"start=1/started@1 outcome=1/applied@2 lost=false undetermined=false",
			},
		},
		{
			name: "a second outcome of one version does not hide behind the first",
			rows: []logRow{
				{run: "a", seq: 1, version: 1, state: "started", second: 1},
				{run: "a", seq: 2, version: 1, state: "failed", second: 2},
				{run: "a", seq: 3, version: 1, state: "applied", second: 3},
			},
			want: []string{
				"start=0/@0 outcome=1/applied@3 lost=true undetermined=false",
				"start=1/started@1 outcome=1/failed@2 lost=false undetermined=false",
			},
		},
		{
			name: "one outcome settles the start before it and no earlier one",
			rows: []logRow{
				{run: "a", seq: 1, version: 1, state: "started", second: 1},
				{run: "a", seq: 2, version: 1, state: "started", second: 2},
				{run: "a", seq: 3, version: 1, state: "applied", second: 3},
			},
			want: []string{
				"start=1/started@2 outcome=1/applied@3 lost=false undetermined=false",
				"start=1/started@1 outcome=0/@0 lost=false undetermined=true",
			},
		},
		{
			name: "attempts of two runs keep the order they started in",
			rows: []logRow{
				{run: "a", seq: 1, version: 1, state: "started", second: 1},
				{run: "b", seq: 1, version: 1, state: "started", second: 2},
				{run: "b", seq: 2, version: 1, state: "failed", second: 3},
				{run: "a", seq: 2, version: 1, state: "applied", second: 4},
			},
			want: []string{
				"start=1/started@2 outcome=1/failed@3 lost=false undetermined=false",
				"start=1/started@1 outcome=1/applied@4 lost=false undetermined=false",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m := logAttemptFixture(c, test.rows)

			attempts, err := m.MigrationLog(context.Background(), 0)

			c.Assert(err, qt.IsNil)
			got := make([]string, 0, len(attempts))
			for _, attempt := range attempts {
				got = append(got, describeAttempt(attempt))
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The limit counts attempts, and an attempt with no start is one.
func TestMigrationLog_LimitCountsAnAttemptWhoseStartWasLost(t *testing.T) {
	c := qt.New(t)
	m := logAttemptFixture(c, []logRow{
		{run: "a", seq: 1, version: 1, state: "started", second: 1},
		{run: "a", seq: 2, version: 1, state: "applied", second: 2},
		{run: "a", seq: 3, version: 2, state: "applied", second: 3},
	})

	attempts, err := m.MigrationLog(context.Background(), 1)

	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Outcome.Version, qt.Equals, int64(2))
	c.Assert(attempts[0].StartLost(), qt.IsTrue)
}

//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
	"ptah.run/migration/migrator"
)

// repeatableClaimingEngines report the rows an UPDATE matched, which is what
// lets the attempt's write decide between two runners that read one record.
var repeatableClaimingEngines = []repeatableEngine{
	repeatablePostgres, repeatableCockroachDB, repeatableYugabyteDB, repeatableMySQL, repeatableMariaDB,
	repeatableSQLServer, repeatableOracle,
}

// interveningRunner stands for a second runner against the same database.
// The run under test reads the record when it plans, and claims the row when
// it starts a repeatable migration. The migrator opens the
// ptah.migrate.repeatable span between the two, so the statements given for
// that span change the record the way the other runner's claim does: after the
// read and before the claim.
type interveningRunner struct {
	migrator.NoopObserver
	r          *repeatableEngineTarget
	statements map[string][]string
	failures   *[]error
}

// StartSpan runs the statements given for name, then starts a no-op span.
func (o interveningRunner) StartSpan(
	ctx context.Context,
	name string,
	attrs ...migrator.ObservationAttribute,
) (context.Context, migrator.ObservationSpan) {
	for _, statement := range o.statements[name] {
		_, err := o.r.conn.ExecContext(ctx, statement)
		*o.failures = append(*o.failures, err)
	}
	return o.NoopObserver.StartSpan(ctx, name, attrs...)
}

// upWithInterveningRunner runs every pending migration of the directory while
// another runner executes statements against the record between this run's
// read and its claim of a repeatable migration. It returns what each of the
// other runner's statements returned, and the run's error.
func (r *repeatableEngineTarget) upWithInterveningRunner(
	c *qt.C,
	files map[string]string,
	statements ...string,
) ([]error, error) {
	c.Helper()
	var failures []error
	observer := interveningRunner{
		r:          r,
		statements: map[string][]string{"ptah.migrate.repeatable": statements},
		failures:   &failures,
	}
	err := r.migrator(c, repeatableEngineMigrations(files)).WithObserver(observer).
		MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{})
	return failures, err
}

// widgetIDsKey is the condition that selects the widget_ids row of the record.
func (r *repeatableEngineTarget) widgetIDsKey() string {
	return sqlident.Ident(r.engine.dialect, "identity") + " = 'widget_ids'"
}

func TestRepeatableClaimOnEnginesLive_FailurePath(t *testing.T) {
	// What the other runner wrote to a row this run read as applied. Taking
	// the row leaves it running; a claim that ran and failed again leaves the
	// state and the digest as this run read them, and only the run differs.
	changes := []struct {
		name string
		set  string
	}{
		{name: "another runner holds its row running", set: "state = 'running', run_id = 'another-run'"},
		{name: "another runner claimed its row and left its state and digest as read", set: "run_id = 'another-run'"},
	}
	for _, engine := range repeatableClaimingEngines {
		for _, change := range changes {
			t.Run(engine.name+": a changed file is refused before its body runs when "+change.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRepeatableEngineTarget(c, t, engine)
				fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})
				c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
				applied := r.record(c, fsys, "widget_ids")

				failures, err := r.upWithInterveningRunner(c, map[string]string{"R__widget_ids.sql": engine.widerIDs()},
					"UPDATE "+r.recordTable()+" SET "+change.set+" WHERE "+r.widgetIDsKey())

				c.Assert(failures, qt.DeepEquals, []error{nil})
				c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableClaimLost)
				c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
				c.Assert(r.record(c, fsys, "widget_ids").AppliedChecksum, qt.Equals, applied.AppliedChecksum)
			})
		}

		t.Run(engine.name+": a new file whose row another runner inserted is refused before its body runs", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)

			failures, err := r.upWithInterveningRunner(c, map[string]string{"R__widget_ids.sql": engine.widgetIDs()},
				"INSERT INTO "+r.recordTable()+" ("+sqlident.Ident(engine.dialect, "identity")+
					", checksum, state, attempt_checksum, attempt_order, transactional, run_id, attempted_at) "+
					"VALUES ('widget_ids', '', 'running', 'h1:other', 1, 0, 'another-run', CURRENT_TIMESTAMP)")

			c.Assert(failures, qt.DeepEquals, []error{nil})
			c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableClaimLost)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 0)
		})
	}
}

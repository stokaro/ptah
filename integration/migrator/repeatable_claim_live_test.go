//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
	"ptah.run/migration/migrator"
)

// repeatableClaimingEngines report the rows an UPDATE matched, which is what
// lets the attempt's write decide between two runners that read one record.
// Every engine of the table does.
var repeatableClaimingEngines = repeatableEngines

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

// upWithInterveningRunner runs every pending migration of fsys while another
// runner executes statements against the record between this run's read and
// its claim of a repeatable migration. It returns what each of the other
// runner's statements returned, and the run's error.
func (r *repeatableEngineTarget) upWithInterveningRunner(
	c *qt.C,
	fsys fstest.MapFS,
	statements ...string,
) ([]error, error) {
	c.Helper()
	var failures []error
	observer := interveningRunner{
		r:          r,
		statements: map[string][]string{"ptah.migrate.repeatable": statements},
		failures:   &failures,
	}
	err := r.migrator(c, fsys).WithObserver(observer).
		MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{})
	return failures, err
}

// widgetIDsKey is the condition that selects the widget_ids row of the record.
func (r *repeatableEngineTarget) widgetIDsKey() string {
	return sqlident.Ident(r.engine.dialect, "identity") + " = 'widget_ids'"
}

// withGadgets adds a second versioned migration to fsys. The run that applies
// it logs the entry under its own run ID before any repeatable migration
// starts, which is how a statement given to the intervening runner names the
// run under test.
func withGadgets(fsys fstest.MapFS) fstest.MapFS {
	fsys["0000000002_gadgets.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n")}
	fsys["0000000002_gadgets.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE gadgets;\n")}
	return fsys
}

// thisRunID is a scalar subquery for the ID of the run that applied the
// gadgets migration: the run under test.
func (r *repeatableEngineTarget) thisRunID() string {
	return "(SELECT max(run_id) FROM " + sqlident.Qualified(r.engine.dialect, "", "schema_migrations_log") +
		" WHERE version = 2)"
}

func TestRepeatableClaimOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableClaimingEngines {
		// A migrator run again over the row its own attempt left, or a
		// claim whose response was lost after it committed, finds its own run
		// ID where it expected the one it read. On MySQL an UPDATE that
		// changes nothing reports no rows as well. Either way the read-back
		// finds this run, and the claim holds.
		t.Run(engine.name+": a changed file whose row names this run is claimed although the update matched nothing", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			writeRepeatable(fsys, "R__widget_ids.sql", engine.widerIDs())

			failures, err := r.upWithInterveningRunner(c, withGadgets(fsys),
				"UPDATE "+r.recordTable()+" SET run_id = "+r.thisRunID()+" WHERE "+r.widgetIDsKey())

			c.Assert(failures, qt.DeepEquals, []error{nil})
			c.Assert(err, qt.IsNil)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 2)
			c.Assert(r.record(c, fsys, "widget_ids").AppliedChecksum, qt.Equals, repeatableDigest(engine.widerIDs()))
		})

		t.Run(engine.name+": a new file whose insert this run's own row refuses is claimed", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(nil)
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			writeRepeatable(fsys, "R__widget_ids.sql", engine.widgetIDs())

			failures, err := r.upWithInterveningRunner(c, withGadgets(fsys),
				"INSERT INTO "+r.recordTable()+" ("+sqlident.Ident(engine.dialect, "identity")+
					", checksum, state, attempt_checksum, attempt_order, transactional, run_id, attempted_at) "+
					"SELECT 'widget_ids', '', 'running', 'h1:other', 1, 0, max(run_id), CURRENT_TIMESTAMP FROM "+
					sqlident.Qualified(engine.dialect, "", "schema_migrations_log")+" WHERE version = 2")

			c.Assert(failures, qt.DeepEquals, []error{nil})
			c.Assert(err, qt.IsNil)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
			c.Assert(r.record(c, fsys, "widget_ids").AppliedChecksum, qt.Equals, repeatableDigest(engine.widgetIDs()))
		})
	}
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
				writeRepeatable(fsys, "R__widget_ids.sql", engine.widerIDs())

				failures, err := r.upWithInterveningRunner(c, fsys,
					"UPDATE "+r.recordTable()+" SET "+change.set+" WHERE "+r.widgetIDsKey())

				c.Assert(failures, qt.DeepEquals, []error{nil})
				c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableClaimLost)
				c.Assert(err, qt.ErrorMatches, `(?s).*: run "another-run" holds the row`)
				c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
				c.Assert(r.record(c, fsys, "widget_ids").AppliedChecksum, qt.Equals, applied.AppliedChecksum)
			})
		}

		t.Run(engine.name+": a new file whose row another runner inserted is refused before its body runs", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)

			failures, err := r.upWithInterveningRunner(c,
				repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()}),
				"INSERT INTO "+r.recordTable()+" ("+sqlident.Ident(engine.dialect, "identity")+
					", checksum, state, attempt_checksum, attempt_order, transactional, run_id, attempted_at) "+
					"VALUES ('widget_ids', '', 'running', 'h1:other', 1, 0, 'another-run', CURRENT_TIMESTAMP)")

			c.Assert(failures, qt.DeepEquals, []error{nil})
			c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableClaimLost)
			c.Assert(err, qt.ErrorMatches, `(?s).*: run "another-run" holds the row: .*`)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 0)
		})
	}
}

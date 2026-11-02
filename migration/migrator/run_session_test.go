package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// An up run executes every migration body on one session, so the state a
// migration leaves on its session, such as the search_path it sets, holds for
// the migrations after it in the same run, as golang-migrate and the pinned
// community binary apply a directory (stokaro/ptah#5034). The session is
// closed when the run ends, so the next run starts on a new one.
//
// The connection here is an executable provider's, which reports the session
// that runs each statement: the run's sessions are the provider's to open and
// close, so the test sees every one. A database/sql pool hands its idle
// connections out again, which can carry a setting from one migration to the
// next without the run asking for it. The index preflight PostgreSQL runs
// before each migration replays the migration's settings on a session of its
// own, so the run's session holds the bodies' statements and nothing else.

// sessionRecorder is a provider session that records which session ran each
// statement and how often each session was closed. The connection's own
// session is number 0, and every session OpenSession opens takes the next
// number.
type sessionRecorder struct {
	*metadataSession
	id    int
	trail *sessionTrail
}

// sessionTrail is what the sessions of one provider share.
type sessionTrail struct {
	opened int
	// ran holds every statement in the order it ran.
	ran []ranStatement
	// closed counts the closes of each session.
	closed map[int]int
}

// ranStatement is one statement and the session that ran it.
type ranStatement struct {
	session   int
	statement string
}

func newSessionRecorder() *sessionRecorder {
	return &sessionRecorder{metadataSession: &metadataSession{}, trail: &sessionTrail{closed: make(map[int]int)}}
}

func (r *sessionRecorder) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return r, nil
}

func (r *sessionRecorder) OpenSession(context.Context) (dbschema.NativeSession, error) {
	r.trail.opened++
	return &sessionRecorder{metadataSession: r.metadataSession, id: r.trail.opened, trail: r.trail}, nil
}

func (r *sessionRecorder) Info() catalog.ServerInfo {
	info := r.metadataSession.Info()
	info.Dialect = "postgres"
	info.Schema = "public"
	info.IdentifierSemantics = identifier.ForDialect("postgres")
	return info
}

func (r *sessionRecorder) Writer() catalog.SchemaWriter { return r }

func (r *sessionRecorder) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	r.trail.ran = append(r.trail.ran, ranStatement{session: r.id, statement: statement})
	return nil
}

func (r *sessionRecorder) Close() error {
	r.trail.closed[r.id]++
	return nil
}

// lastSession returns the session that ran the latest statement.
func (t *sessionTrail) lastSession() int {
	return t.ran[len(t.ran)-1].session
}

// statementsOn returns the statements session ran, in order.
func (t *sessionTrail) statementsOn(session int) []string {
	var statements []string
	for _, ran := range t.ran {
		if ran.session == session {
			statements = append(statements, ran.statement)
		}
	}
	return statements
}

// newRunSessionMigrator returns a migrator over three migrations of one
// statement each, run outside a transaction, on a connection to recorder.
func newRunSessionMigrator(c *qt.C, recorder *sessionRecorder) *migrator.Migrator {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"recorder": {Target: "postgres", Connector: recorder},
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "recorder://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "init", "CREATE SCHEMA s1;", "DROP SCHEMA s1;"),
		migrator.CreateMigrationFromSQL(2, "set", "SET search_path = s1;", "RESET search_path;"),
		migrator.CreateMigrationFromSQL(3, "next", "CREATE TABLE t3 (id int);", "DROP TABLE t3;"),
	)).WithMigrationsTable("public", "_revisions").WithTransactionMode(migrator.MigrationTxModeNone).
		WithoutMigrationLock()
}

func TestMigrateUpRunSession_HappyPath(t *testing.T) {
	t.Run("every body runs on one session the run opens and closes", func(t *testing.T) {
		c := qt.New(t)
		recorder := newSessionRecorder()

		err := newRunSessionMigrator(c, recorder).MigrateUp(c.Context())

		c.Assert(err, qt.IsNil)
		run := recorder.trail.lastSession()
		c.Assert(run, qt.Not(qt.Equals), 0)
		c.Assert(recorder.trail.statementsOn(run), qt.DeepEquals,
			[]string{"CREATE SCHEMA s1", "SET search_path = s1", "CREATE TABLE t3 (id int)"})
		c.Assert(recorder.trail.closed[run], qt.Equals, 1)
	})

	t.Run("a later run starts on a session of its own", func(t *testing.T) {
		c := qt.New(t)
		recorder := newSessionRecorder()
		m := newRunSessionMigrator(c, recorder)
		c.Assert(m.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{Amount: 2}), qt.IsNil)
		first := recorder.trail.lastSession()

		err := m.MigrateUp(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(recorder.trail.statementsOn(first), qt.DeepEquals, []string{"CREATE SCHEMA s1", "SET search_path = s1"})
		c.Assert(recorder.trail.closed[first], qt.Equals, 1)
		later := recorder.trail.lastSession()
		c.Assert(later, qt.Not(qt.Equals), first)
		c.Assert(recorder.trail.statementsOn(later), qt.DeepEquals, []string{"CREATE TABLE t3 (id int)"})
	})
}

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/postgres"
)

// setting is a server that answers one setting, and that counts the questions.
type setting struct {
	migratorext.Queryer
	value string
	null  bool
	err   error
	asked int
}

type settingRow struct{ server *setting }

func (r settingRow) Scan(dest ...any) error {
	if r.server.err != nil {
		return r.server.err
	}
	target, ok := dest[0].(*sql.NullString)
	if !ok {
		return fmt.Errorf("unexpected scan target %T", dest[0])
	}
	*target = sql.NullString{String: r.server.value, Valid: !r.server.null}
	return nil
}

func (r settingRow) Err() error { return r.server.err }

func (s *setting) QueryRowContext(context.Context, string, ...any) dataext.Row {
	s.asked++
	return settingRow{server: s}
}

const (
	createTable = "CREATE TABLE k (id INT PRIMARY KEY)"
	insertRow   = "INSERT INTO seed (id) VALUES (1)"
)

// TestYugabyteDBRollbackSurvivors holds the answer on a server whose schema
// statements commit on their own: the schema statements stay and the data goes,
// and a resume can only start past a leading run of them.
func TestYugabyteDBRollbackSurvivors(t *testing.T) {
	tests := []struct {
		name      string
		ran       []string
		wantKept  int
		wantKnown bool
	}{
		{name: "nothing ran", wantKnown: true},
		{name: "data only", ran: []string{insertRow, "UPDATE seed SET id = 2", "DELETE FROM seed"}, wantKnown: true},
		{name: "a schema statement", ran: []string{createTable}, wantKept: 1, wantKnown: true},
		{name: "schema statements of every measured word", ran: []string{
			createTable, "ALTER TABLE k ADD COLUMN c INT", "DROP TABLE k", "TRUNCATE seed",
			"COMMENT ON TABLE seed IS 'x'", "GRANT SELECT ON seed TO r", "REVOKE SELECT ON seed FROM r",
		}, wantKept: 7, wantKnown: true},
		{name: "a schema statement, then data, which goes", ran: []string{createTable, insertRow}, wantKept: 1, wantKnown: true},
		{name: "a setting ahead of a schema statement counts with it", ran: []string{"SET search_path TO app", createTable}, wantKept: 2, wantKnown: true},
		{name: "a setting after the last schema statement is left to run again", ran: []string{createTable, "SET search_path TO app"}, wantKept: 1, wantKnown: true},
		{name: "a temporary table lasts as long as the session", ran: []string{"CREATE TEMP TABLE t (id INT)", "create temporary table u (id INT)"}, wantKnown: true},
		{name: "a comment ahead of the first word", ran: []string{"-- the table\n/* k */ " + createTable}, wantKept: 1, wantKnown: true},
		{name: "lower case", ran: []string{"create table k (id int primary key)"}, wantKept: 1, wantKnown: true},
		{name: "data ahead of a schema statement is no leading run", ran: []string{insertRow, createTable}},
		{name: "a read ahead of a schema statement is no leading run", ran: []string{"SELECT f()", createTable}},
		{name: "a schema statement behind data behind a schema statement", ran: []string{createTable, insertRow, createTable}},
		{name: "a DO block may have done either", ran: []string{"DO $$ BEGIN CREATE TABLE d (id INT); END $$"}},
		{name: "a CALL may have done either", ran: []string{"CALL p()"}},
		{name: "a body that commits", ran: []string{createTable, "COMMIT"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &setting{value: "off"}

			kept, known := postgres.YugabyteDB().RollbackSurvivors.Kept(c.Context(), server, migratorext.RollbackEvidence{Ran: test.ran})

			c.Assert(kept, qt.Equals, test.wantKept)
			c.Assert(known, qt.Equals, test.wantKnown)
		})
	}
}

// TestYugabyteDBRollbackSurvivors_ReadsTheServer holds the answer to the server's
// setting, which is what separates a statement that stayed from one the rollback
// took. Measured on 2026.1.2 with the setting on: nothing stays.
func TestYugabyteDBRollbackSurvivors_ReadsTheServer(t *testing.T) {
	tests := []struct {
		name      string
		server    setting
		wantKept  int
		wantKnown bool
	}{
		{name: "off, the default", server: setting{value: "off"}, wantKept: 1, wantKnown: true},
		{name: "a release with no such setting", server: setting{null: true}, wantKept: 1, wantKnown: true},
		{name: "on, so the rollback takes everything", server: setting{value: "on"}, wantKnown: true},
		{name: "the read failed", server: setting{err: errors.New("connection refused")}},
		{name: "an answer nobody measured", server: setting{value: "maybe"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kept, known := postgres.YugabyteDB().RollbackSurvivors.Kept(c.Context(), &test.server, migratorext.RollbackEvidence{Ran: []string{createTable}})

			c.Assert(kept, qt.Equals, test.wantKept)
			c.Assert(known, qt.Equals, test.wantKnown)
			c.Assert(test.server.asked, qt.Equals, 1)
		})
	}
}

// TestYugabyteDBRollbackSurvivors_AsksNothingOfADataBody is the control for the
// test above: a body with no statement that could stay needs no answer, so a
// failed read cannot change the report of one.
func TestYugabyteDBRollbackSurvivors_AsksNothingOfADataBody(t *testing.T) {
	c := qt.New(t)
	server := &setting{err: errors.New("connection refused")}

	kept, known := postgres.YugabyteDB().RollbackSurvivors.Kept(c.Context(), server, migratorext.RollbackEvidence{Ran: []string{insertRow}})

	c.Assert(kept, qt.Equals, 0)
	c.Assert(known, qt.IsTrue)
	c.Assert(server.asked, qt.Equals, 0)
}

// TestRollbackSurvivors_AFailureTheServerDidNotAnswer holds both engines to what a
// lost answer can mean: a data statement in flight dies with its transaction, and
// a schema statement in flight may have committed.
func TestRollbackSurvivors_AFailureTheServerDidNotAnswer(t *testing.T) {
	tests := []struct {
		name       string
		services   migratorext.Services
		ran        []string
		unanswered []string
		outside    bool
		wantKnown  bool
	}{
		{name: "yugabytedb, data in flight", services: postgres.YugabyteDB(), ran: []string{insertRow}, unanswered: []string{insertRow}, wantKnown: true},
		{name: "yugabytedb, a schema statement in flight", services: postgres.YugabyteDB(), unanswered: []string{createTable}},
		{name: "yugabytedb, data in flight after a schema statement", services: postgres.YugabyteDB(), ran: []string{createTable}, unanswered: []string{insertRow}},
		{name: "cockroachdb, data in flight", services: postgres.CockroachDB(), ran: []string{insertRow}, unanswered: []string{insertRow}, wantKnown: true},
		{name: "cockroachdb, a schema statement in flight", services: postgres.CockroachDB(), unanswered: []string{createTable}},
		{name: "cockroachdb, data in flight after a schema statement", services: postgres.CockroachDB(), ran: []string{createTable}, unanswered: []string{insertRow}, outside: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kept, known := test.services.RollbackSurvivors.Kept(c.Context(), &setting{value: "off"}, migratorext.RollbackEvidence{
				Ran: test.ran, Unanswered: test.unanswered, SessionKnown: true, OutsideTransaction: test.outside,
			})

			c.Assert(kept, qt.Equals, 0)
			c.Assert(known, qt.Equals, test.wantKnown)
		})
	}
}

// TestCockroachDBRollbackSurvivors holds the answer to what the server says of the
// session, whichever statements ran: outside a transaction everything that ran
// stays, and inside one the rollback takes it all.
func TestCockroachDBRollbackSurvivors(t *testing.T) {
	tests := []struct {
		name         string
		ran          []string
		sessionKnown bool
		outside      bool
		wantKept     int
		wantKnown    bool
	}{
		{name: "data in an open transaction", ran: []string{insertRow}, sessionKnown: true, wantKnown: true},
		{
			name: "a schema statement a server with the setting off kept in the transaction",
			ran:  []string{createTable}, sessionKnown: true, wantKnown: true,
		},
		{name: "the server ended the transaction", ran: []string{createTable}, sessionKnown: true, outside: true, wantKept: 1, wantKnown: true},
		{name: "data on both sides of the schema statement", ran: []string{insertRow, createTable, insertRow}, sessionKnown: true, outside: true, wantKept: 3, wantKnown: true},
		{name: "data ahead of a refused schema statement", ran: []string{insertRow}, sessionKnown: true, outside: true, wantKept: 1, wantKnown: true},
		{name: "the failed first statement", sessionKnown: true, outside: true, wantKnown: true},
		{name: "a body that rolls back itself", ran: []string{createTable, "ROLLBACK"}, sessionKnown: true, outside: true},
		{name: "a body that opens a transaction again", ran: []string{createTable, "COMMIT", "BEGIN"}, sessionKnown: true},
		{name: "no session to ask, with a schema statement", ran: []string{createTable}},
		{name: "no session to ask, with data alone", ran: []string{insertRow}, wantKnown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kept, known := postgres.CockroachDB().RollbackSurvivors.Kept(c.Context(), nil, migratorext.RollbackEvidence{
				Ran: test.ran, SessionKnown: test.sessionKnown, OutsideTransaction: test.outside,
			})

			c.Assert(kept, qt.Equals, test.wantKept)
			c.Assert(known, qt.Equals, test.wantKnown)
		})
	}
}

// TestPostgresRollbackTakesTheWholeBody is the control: PostgreSQL offers no such
// service, and the migrator reads that as a rollback that takes everything.
func TestPostgresRollbackTakesTheWholeBody(t *testing.T) {
	c := qt.New(t)

	c.Assert(postgres.Postgres().RollbackSurvivors, qt.IsNil)
}

package tsprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsprobe"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/dbschema/dbtest"
)

// server scripts the statements a probe sends: whether the extension is
// installed, which bodies the server refuses, and what it stores for the rest.
type server struct {
	installed bool
	refuse    string
	stored    string
	executed  []string
}

func (s *server) query(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "pg_extension"):
		return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{s.installed}}}, nil
	case strings.Contains(query, "timescaledb_information.continuous_aggregates"):
		return dbtest.QueryResult{Columns: []string{"view_definition"}, Rows: [][]driver.Value{{s.stored}}}, nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

func (s *server) exec(query string, _ []driver.NamedValue) (driver.Result, error) {
	s.executed = append(s.executed, query)
	if s.refuse != "" && strings.Contains(query, s.refuse) {
		return nil, errors.New(`syntax error at or near "FROM"`)
	}
	return driver.RowsAffected(0), nil
}

// session runs the body in a transaction it always rolls back, the contract
// a connection's probe session keeps. declined answers as a session that
// could not open an isolated transaction.
type session struct {
	db       *sql.DB
	declined bool
}

func (s session) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	if s.declined {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	return true, body(ctx, tx)
}

func request(c *qt.C, probe schemaext.ProbeSession, declared ...schemaext.Object) schemaext.NormalizationRequest {
	c.Helper()
	return schemaext.NormalizationRequest{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Session: probe,
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(declared...))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(
			tsschema.ObservedContinuousAggregateObject("public", "hourly", tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;"}),
			tsschema.ObservedContinuousAggregateObject("Metrics", "hourly", tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;"}),
		))},
	}
}

func normalized(c *qt.C, result schemaext.NormalizationResult, schema, name string) *tsschema.NormalizedBody {
	c.Helper()
	object, found, err := result.Desired.Objects.Get(tsschema.ContinuousAggregateRef(schema, name))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return object.Value.(*tsschema.DesiredContinuousAggregate).Normalized
}

// TestNormalizeObjects_AttachesTheServersSpelling pins the probe: a held
// aggregate is created under a probe name in its own schema -- quoted, so a
// mixed-case schema is not folded into another -- WITH NO DATA, with the
// option the declaration wrote, inside a savepoint, and its stored definition
// is attached.
// A declaration of an aggregate the server does not hold is not probed: its
// CREATE carries the declaration unchanged.
func TestNormalizeObjects_AttachesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	scripted := &server{installed: true, stored: "  SELECT 1 AS one;  "}
	db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
	held := tsschema.DesiredContinuousAggregateObject("Metrics", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1 AS one", MaterializedOnly: new(true)})
	created := tsschema.DesiredContinuousAggregateObject("", "daily", tsschema.DesiredContinuousAggregate{Body: "SELECT 2"})

	result, err := tsprobe.Service{}.NormalizeObjects(t.Context(), request(c, session{db: db.SQL}, held, created))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(normalized(c, result, "Metrics", "hourly"), qt.DeepEquals, &tsschema.NormalizedBody{Body: "SELECT 1 AS one;"})
	c.Assert(normalized(c, result, "", "daily"), qt.IsNil)
	c.Assert(scripted.executed, qt.DeepEquals, []string{
		"SAVEPOINT ptah_cagg_probe",
		"CREATE MATERIALIZED VIEW \"Metrics\".ptah_cagg_probe_0 WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS\nSELECT 1 AS one\nWITH NO DATA",
		"ROLLBACK TO SAVEPOINT ptah_cagg_probe",
	})
}

// TestNormalizeObjects_LeavesWhatItCouldNotAnswer pins the cases that attach
// nothing and fail nothing: a server without the extension, a session that
// could not isolate the probe, and a body the server refused -- which rolls
// back to the savepoint so a later probe in the same transaction still runs.
func TestNormalizeObjects_LeavesWhatItCouldNotAnswer(t *testing.T) {
	tests := []struct {
		name         string
		server       server
		declined     bool
		wantExecuted []string
	}{
		{name: "no extension", server: server{installed: false}},
		{name: "a declined session", server: server{installed: true}, declined: true},
		{name: "a refused body", server: server{installed: true, refuse: "SELECT broken"}, wantExecuted: []string{
			"SAVEPOINT ptah_cagg_probe",
			"CREATE MATERIALIZED VIEW ptah_cagg_probe_0 WITH (timescaledb.continuous) AS\nSELECT broken\nWITH NO DATA",
			"ROLLBACK TO SAVEPOINT ptah_cagg_probe",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := test.server
			db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
			held := tsschema.DesiredContinuousAggregateObject("", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT broken"})

			result, err := tsprobe.Service{}.NormalizeObjects(t.Context(), request(c, session{db: db.SQL, declined: test.declined}, held))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(normalized(c, result, "", "hourly"), qt.IsNil)
			c.Assert(scripted.executed, qt.DeepEquals, test.wantExecuted)
		})
	}
}

// TestNormalizeObjects_FailurePath pins the refusals that come before any
// statement: another target family and a request with no session.
func TestNormalizeObjects_FailurePath(t *testing.T) {
	held := tsschema.DesiredContinuousAggregateObject("", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"})
	tests := []struct {
		name    string
		target  string
		session schemaext.ProbeSession
		want    error
	}{
		{name: "another target", target: "mysql", session: session{}, want: ptaherr.ErrUnsupportedDialect},
		{name: "no session", target: "postgres", want: schemaext.ErrInvalidValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			probe := request(c, test.session, held)
			probe.Target = test.target

			result, err := tsprobe.Service{}.NormalizeObjects(t.Context(), probe)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}

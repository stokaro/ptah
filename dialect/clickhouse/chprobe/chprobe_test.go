package chprobe_test

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
	"ptah.run/dialect/clickhouse/chprobe"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dbschema/dbtest"
)

// server answers the formatting query: the filter it was handed, spelled as
// spelled says, or an error for a filter it refuses.
type server struct {
	spelled map[string]string
	queries []string
}

func (s *server) query(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	s.queries = append(s.queries, query)
	if !strings.Contains(query, "formatQuerySingleLine") || len(args) != 1 {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	filter, _ := args[0].Value.(string)
	formatted, found := s.spelled[filter]
	if !found {
		return dbtest.QueryResult{}, errors.New("Code: 62. DB::Exception: Syntax error")
	}
	return dbtest.QueryResult{Columns: []string{"formatted"}, Rows: [][]driver.Value{{"SELECT " + formatted}}}, nil
}

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

func semantics() identifier.Semantics {
	rules := identifier.ForDialect("clickhouse")
	rules.DefaultSchema = "app"
	return rules
}

func normalizationRequest(probe schemaext.ProbeSession, declared ...schemaext.Object) schemaext.NormalizationRequest {
	observed := must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("app", "orders", "tenant"),
		chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive}))
	return schemaext.NormalizationRequest{
		Target: "clickhouse", Identifiers: semantics(), Session: probe,
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(declared...))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(observed))},
	}
}

func declaredPolicy(table, name, filter string) schemaext.Object {
	return must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", table, name), chschema.DesiredRowPolicy{Filter: new(filter)}))
}

func normalizedFilter(c *qt.C, result schemaext.NormalizationResult, table, name string) *string {
	c.Helper()
	object, found, err := result.Desired.Objects.Get(chschema.RowPolicyRef("", table, name))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return object.Value.(*chschema.DesiredRowPolicy).NormalizedFilter
}

// A declaration whose policy the server holds is spelled by the server, the
// query naming the filter as a bound parameter inside the parentheses the
// renderer writes. One the server does not hold is created as declared and is
// not probed.
func TestNormalizeObjects_AttachesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	scripted := &server{spelled: map[string]string{"tenant=1": "(tenant = 1)"}}
	db := dbtest.Open(t, scripted.query)

	result, err := chprobe.Service{}.NormalizeObjects(t.Context(), normalizationRequest(session{db: db.SQL},
		declaredPolicy("orders", "tenant", "tenant=1"), declaredPolicy("customers", "tenant", "tenant=1")))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(normalizedFilter(c, result, "orders", "tenant"), qt.DeepEquals, new("(tenant = 1)"))
	c.Assert(normalizedFilter(c, result, "customers", "tenant"), qt.IsNil)
	c.Assert(scripted.queries, qt.DeepEquals, []string{"SELECT formatQuerySingleLine(concat('SELECT (', ?, ')'))"})
}

// A session that could not run the probe and a filter the server refused
// attach nothing and fail nothing: the comparison treats the filter as
// unanswered.
func TestNormalizeObjects_LeavesWhatItCouldNotAnswer(t *testing.T) {
	for _, test := range []struct {
		name     string
		spelled  map[string]string
		declined bool
	}{
		{"a declined session", map[string]string{"tenant=1": "(tenant = 1)"}, true},
		{"a refused filter", make(map[string]string), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := &server{spelled: test.spelled}
			db := dbtest.Open(t, scripted.query)

			result, err := chprobe.Service{}.NormalizeObjects(t.Context(), normalizationRequest(session{db: db.SQL, declined: test.declined},
				declaredPolicy("orders", "tenant", "tenant=1")))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(normalizedFilter(c, result, "orders", "tenant"), qt.IsNil)
		})
	}
}

// The probe runs on ClickHouse only, and needs a session to run in.
func TestNormalizeObjects_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		target  string
		session schemaext.ProbeSession
		want    error
	}{
		{"another target", "postgres", session{}, ptaherr.ErrUnsupportedDialect},
		{"no session", "clickhouse", nil, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			probe := normalizationRequest(test.session, declaredPolicy("orders", "tenant", "tenant=1"))
			probe.Target = test.target

			result, err := chprobe.Service{}.NormalizeObjects(t.Context(), probe)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}

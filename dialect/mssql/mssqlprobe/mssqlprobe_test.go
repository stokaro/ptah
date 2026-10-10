package mssqlprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlprobe"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/dbschema/dbtest"
)

// server scripts the probe: the CREATE SECURITY POLICY it is given succeeds
// unless refuse is set, and the predicates it then reports are stored.
type server struct {
	stored []driver.Value
	refuse bool
	execs  []string
}

func (s *server) exec(query string, _ []driver.NamedValue) (driver.Result, error) {
	s.execs = append(s.execs, query)
	if s.refuse && strings.HasPrefix(query, "CREATE SECURITY POLICY") {
		return nil, errors.New("mssql: Cannot schema bind security policy")
	}
	return driver.RowsAffected(0), nil
}

func (s *server) query(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "sys.security_predicates") {
		return dbtest.QueryResult{}, errors.New("unexpected query: " + query)
	}
	return dbtest.QueryResult{Columns: []string{"table_schema", "table_name", "predicate_definition", "type", "operation"},
		Rows: [][]driver.Value{s.stored}}, nil
}

// session runs the probe in a transaction of db, or declines one.
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

var (
	orders   = mssqlschema.ObjectName{Schema: "app", Name: "orders"}
	function = mssqlschema.ObjectName{Schema: "rls", Name: "fn"}
	ref      = mssqlschema.SecurityPolicyRef("rls", "tenancy")
)

// filter is the filter predicate on orders with argument.
func filter(argument string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: function, Arguments: []string{argument}, Table: orders}
}

// request declares tenancy with argument against a database holding it with
// the catalog's spelling of a cast.
func request(c *qt.C, s session, argument string) schemaext.NormalizationRequest {
	c.Helper()
	declared := must.Must(mssqlschema.DesiredSecurityPolicyObject(ref, mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{filter(argument)}}))
	declared.Targets = []string{"sqlserver"}
	held := must.Must(mssqlschema.ObservedSecurityPolicyObject(ref, mssqlschema.ObservedSecurityPolicy{
		Predicates: []mssqlschema.Predicate{filter("CONVERT([int],[tenant])+(0)")}, Enabled: true, SchemaBinding: true}))
	return schemaext.NormalizationRequest{Target: "sqlserver", Identifiers: identifier.ForDialect("sqlserver"), Session: s,
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(declared))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(held))}}
}

// declared returns the one declaration a normalization returns.
func declared(c *qt.C, result schemaext.NormalizationResult) *mssqlschema.DesiredSecurityPolicy {
	c.Helper()
	object, found, err := result.Desired.Objects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(object.Targets, qt.DeepEquals, []string{"sqlserver"})
	return object.Value.(*mssqlschema.DesiredSecurityPolicy)
}

// A cast the catalog stores rewritten is put through the server, turned off
// under a probe name and rolled back to a savepoint, and the declaration
// comes back carrying the server's spelling, which agrees with the policy
// held.
func TestNormalizeObjects_AttachesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	script := &server{stored: []driver.Value{"app", "orders", "([rls].[fn](CONVERT([int],[tenant])+(0)))", "FILTER", ""}}
	db := dbtest.OpenWithExec(t, script.query, script.exec)

	result, err := mssqlprobe.Service{}.NormalizeObjects(t.Context(), request(c, session{db: db.SQL}, "CAST(tenant AS int) + 0"))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	value := declared(c, result)
	c.Assert(value.Predicates, qt.DeepEquals, []mssqlschema.Predicate{filter("CAST(tenant AS int) + 0")})
	c.Assert(value.Normalized, qt.DeepEquals, []mssqlschema.Predicate{filter("CONVERT([int],[tenant])+(0)")})
	c.Assert(script.execs, qt.HasLen, 3)
	c.Assert(script.execs[0], qt.Equals, "SAVE TRANSACTION ptah_policy_probe")
	c.Assert(script.execs[1], qt.Matches, `(?s)CREATE SECURITY POLICY \[rls\]\.\[ptah_probe_[0-9a-f]{16}\]\n    ADD FILTER PREDICATE \[rls\]\.\[fn\]\(CAST\(tenant AS int\) \+ 0\) ON \[app\]\.\[orders\]\n    WITH \(STATE = OFF, SCHEMABINDING = ON\);`)
	c.Assert(script.execs[2], qt.Equals, "ROLLBACK TRANSACTION ptah_policy_probe")
}

// What the probe cannot answer leaves the declaration as it was: a
// declaration that agrees offline is not probed, a statement the server
// refuses and a session that declines a transaction answer nothing.
func TestNormalizeObjects_LeavesTheUnansweredAlone(t *testing.T) {
	tests := []struct {
		name      string
		argument  string
		refuse    bool
		declined  bool
		wantExecs int
	}{
		{name: "an argument that agrees offline", argument: "CONVERT([int],[tenant])+(0)"},
		{name: "a statement the server refuses", argument: "CAST(tenant AS int) + 0", refuse: true, wantExecs: 3},
		{name: "a declined session", argument: "CAST(tenant AS int) + 0", declined: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			script := &server{refuse: test.refuse, stored: []driver.Value{"app", "orders", "([rls].[fn]([tenant]))", "FILTER", ""}}
			db := dbtest.OpenWithExec(t, script.query, script.exec)

			result, err := mssqlprobe.Service{}.NormalizeObjects(t.Context(), request(c, session{db: db.SQL, declined: test.declined}, test.argument))

			c.Assert(err, qt.IsNil)
			c.Assert(declared(c, result).Normalized, qt.IsNil)
			c.Assert(script.execs, qt.HasLen, test.wantExecs)
		})
	}
}

func TestNormalizeObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, (&server{}).query, (&server{}).exec)
	declaredRequest := request(c, session{db: db.SQL}, "CAST(tenant AS int) + 0")
	declaredRequest.Target = "postgres"

	result, err := mssqlprobe.Service{}.NormalizeObjects(t.Context(), declaredRequest)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
}

package pgpolicyprovider_test

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

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// server scripts what a probe sends: which statement the server refuses and
// what pg_policy reports for the probe policy.
type server struct {
	refuse   string
	public   bool
	roles    string
	using    driver.Value
	executed []string
}

func (s *server) query(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "FROM pg_policy") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	return dbtest.QueryResult{Columns: []string{"public", "roles", "using", "with_check"},
		Rows: [][]driver.Value{{s.public, s.roles, s.using, nil}}}, nil
}

func (s *server) exec(query string, _ []driver.NamedValue) (driver.Result, error) {
	s.executed = append(s.executed, query)
	if s.refuse != "" && strings.Contains(query, s.refuse) {
		return nil, errors.New("permission denied for table orders")
	}
	return driver.RowsAffected(0), nil
}

// session runs the body in a transaction it always rolls back, the contract a
// connection's probe session keeps. declined answers as a session that could
// not open an isolated transaction.
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

// normalization asks for the declared policies of orders, which the server
// holds as tenant and owners.
func normalization(c *qt.C, probe schemaext.ProbeSession, declared ...schemaext.Object) schemaext.NormalizationRequest {
	c.Helper()
	return schemaext.NormalizationRequest{
		Target: "postgres", Identifiers: postgres, Session: probe,
		Desired: schemaext.ObjectState{Objects: objects(c, declared...), Coverage: complete(c, schemaext.Desired)},
		Current: schemaext.ObjectState{Objects: objects(c,
			observedPolicy(c, "orders", "tenant", held("true")),
			observedPolicy(c, "orders", "owners", held("true")),
		), Coverage: complete(c, schemaext.Observed)},
	}
}

func normalized(c *qt.C, result schemaext.NormalizationResult, name string) *pgpolicy.NormalizedPolicy {
	c.Helper()
	object, found := must.Must2(result.Desired.Objects.Get(pgpolicy.PolicyRef("app", "orders", name)))
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgpolicy.DesiredPolicy).Normalized
}

const searchPath = `SELECT set_config('search_path', CASE
	WHEN current_setting('search_path') = '' THEN 'pg_temp'
	ELSE current_setting('search_path') || ', pg_temp' END, true)`

// TestNormalizeObjects_AttachesTheServersSpelling pins the probe: a held
// policy whose clause or role keyword the server rewrites is created, with the
// statement a plan would run, on a temporary copy of its table named after it,
// with pg_temp last in the search path, inside a savepoint, and the stored
// roles and clauses are attached. A held policy the server has nothing to
// rewrite in, and a declaration of a policy the server does not hold, are not
// probed.
func TestNormalizeObjects_AttachesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	scripted := &server{roles: `["app","reader"]`, using: "((owner)::text = 'x'::text)"}
	db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
	rewritten := desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{
		Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}, reader}, Using: new("owner = 'x'")})
	plain := desiredPolicy(c, "orders", "owners", pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{reader}})
	created := desiredPolicy(c, "orders", "fresh", pgpolicy.DesiredPolicy{Using: new("true")})

	result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(c, session{db: db.SQL}, rewritten, plain, created))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(normalized(c, result, "tenant"), qt.DeepEquals,
		&pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{app, reader}, Using: new("((owner)::text = 'x'::text)")})
	c.Assert(normalized(c, result, "owners"), qt.IsNil)
	c.Assert(normalized(c, result, "fresh"), qt.IsNil)
	c.Assert(scripted.executed, qt.DeepEquals, []string{
		"SAVEPOINT ptah_policy_probe",
		searchPath,
		`CREATE TEMPORARY TABLE pg_temp."orders" (LIKE "app"."orders")`,
		"CREATE POLICY ptah_policy_probe ON pg_temp.\"orders\" TO CURRENT_USER, \"reader\"\n    USING (owner = 'x')\n",
		"ROLLBACK TO SAVEPOINT ptah_policy_probe",
	})
}

// TestNormalizeObjects_AttachesPUBLIC pins the role list a policy for every
// role reads back as.
func TestNormalizeObjects_AttachesPUBLIC(t *testing.T) {
	c := qt.New(t)
	scripted := &server{public: true, roles: `[]`, using: "true"}
	db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
	declared := desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{Using: new("true")})

	result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(c, session{db: db.SQL}, declared))

	c.Assert(err, qt.IsNil)
	c.Assert(normalized(c, result, "tenant"), qt.DeepEquals, &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{public}, Using: new("true")})
}

// TestNormalizeObjects_LeavesWhatItCouldNotAnswer pins the cases that attach
// nothing and fail nothing: a session that could not isolate the probe, a
// statement the server refused, which rolls back to the savepoint so a later
// probe in the same transaction still runs, and an answer the model cannot
// hold.
func TestNormalizeObjects_LeavesWhatItCouldNotAnswer(t *testing.T) {
	tests := []struct {
		name         string
		server       server
		declined     bool
		wantExecuted []string
	}{
		{name: "a declined session", server: server{roles: `["app"]`, using: "true"}, declined: true},
		{name: "a refused copy of the table", server: server{refuse: "CREATE TEMPORARY TABLE"}, wantExecuted: []string{
			"SAVEPOINT ptah_policy_probe", searchPath, `CREATE TEMPORARY TABLE pg_temp."orders" (LIKE "app"."orders")`,
			"ROLLBACK TO SAVEPOINT ptah_policy_probe",
		}},
		{name: "an answer without the declared clause", server: server{roles: `["app"]`, using: nil}, wantExecuted: []string{
			"SAVEPOINT ptah_policy_probe", searchPath, `CREATE TEMPORARY TABLE pg_temp."orders" (LIKE "app"."orders")`,
			"CREATE POLICY ptah_policy_probe ON pg_temp.\"orders\"\n    USING (owner = 'x')\n", "ROLLBACK TO SAVEPOINT ptah_policy_probe",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := test.server
			db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
			declared := desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{Using: new("owner = 'x'")})

			result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(c, session{db: db.SQL, declined: test.declined}, declared))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(normalized(c, result, "tenant"), qt.IsNil)
			c.Assert(scripted.executed, qt.DeepEquals, test.wantExecuted)
		})
	}
}

// TestNormalizeObjects_FailurePath pins the refusals that come before any
// statement: a request with no session, and a target outside the PostgreSQL
// family. The runtime sends the owner no target it was not registered for, so
// both are asked of the service directly.
func TestNormalizeObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		session schemaext.ProbeSession
		want    error
	}{
		{name: "no session", target: "postgres", want: schemaext.ErrInvalidValue},
		{name: "a target outside the family, asked directly", target: "mysql", session: session{}, want: ptaherr.ErrUnsupportedDialect},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := normalization(c, test.session, desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{Using: new("true")}))
			request.Target = test.target

			result, err := policyprobe.Service{}.NormalizeObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}

package pgtypeprovider_test

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
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typeprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// server scripts what a probe sends: which statement the server refuses and
// what pg_type reports for the probe domain.
type server struct {
	refuse      string
	defaultText string
	check       string
	executed    []string
}

func (s *server) query(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "FROM pg_type") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	return dbtest.QueryResult{Columns: []string{"default", "check"}, Rows: [][]driver.Value{{s.defaultText, s.check}}}, nil
}

func (s *server) exec(query string, _ []driver.NamedValue) (driver.Result, error) {
	s.executed = append(s.executed, query)
	if s.refuse != "" && strings.Contains(query, s.refuse) {
		return nil, errors.New(`type "missing" does not exist`)
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

// domainNamed is the declaration of the domain name, which names no schema.
func domainNamed(name string, domain *pgtype.DesiredDomain) schemaext.Object {
	return must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "", name), domain))
}

// normalization asks for declared domains on a server that holds the
// domains mail and code in public.
func normalization(probe schemaext.ProbeSession, declarations ...schemaext.Object) schemaext.NormalizationRequest {
	request := comparison(declarations, []schemaext.Object{
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "public", "mail"), &pgtype.ObservedDomain{BaseType: "text"})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "public", "code"), &pgtype.ObservedDomain{BaseType: "integer"})),
	})
	return schemaext.NormalizationRequest{Target: request.Target, Identifiers: request.Identifiers, Capabilities: request.Capabilities,
		Desired: request.Desired, Current: request.Current, Session: probe}
}

func normalizedDomain(c *qt.C, result schemaext.NormalizationResult, name string) *pgtype.NormalizedDomain {
	c.Helper()
	object, found := must.Must2(result.Desired.Objects.Get(pgtype.Ref(pgtype.DomainKind, "", name)))
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgtype.DesiredDomain).Normalized
}

// TestNormalizeTypes_AttachesTheServersSpelling pins the probe: a held domain
// that states a CHECK or a default is created in pg_temp over its base type,
// inside a savepoint, with a literal default quoted, and the stored
// expressions are attached. A held domain that states neither, and a
// declaration of a domain the server does not hold, are not probed.
func TestNormalizeTypes_AttachesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	scripted := &server{defaultText: "'it''s'::text", check: "(VALUE <> ''::text)"}
	db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
	mail := domainNamed("mail", &pgtype.DesiredDomain{BaseType: "text", Default: "it's", Check: "VALUE <> ''"})
	code := domainNamed("code", &pgtype.DesiredDomain{BaseType: "integer", NotNull: true})
	fresh := domainNamed("fresh", &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''"})

	result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(session{db: db.SQL}, mail, code, fresh))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(normalizedDomain(c, result, "mail"), qt.DeepEquals, &pgtype.NormalizedDomain{Check: "(VALUE <> ''::text)", Default: "'it''s'::text"})
	c.Assert(normalizedDomain(c, result, "code"), qt.IsNil)
	c.Assert(normalizedDomain(c, result, "fresh"), qt.IsNil)
	c.Assert(scripted.executed, qt.DeepEquals, []string{
		"SAVEPOINT ptah_domain_probe",
		"CREATE DOMAIN pg_temp.ptah_domain_probe_0 AS text DEFAULT 'it''s' CHECK (VALUE <> '')",
		"ROLLBACK TO SAVEPOINT ptah_domain_probe",
	})
}

// TestNormalizeTypes_AsksForAnExpressionAsWritten pins that a default
// expression is asked as written, and wins over a literal beside it, which is
// what CREATE DOMAIN writes.
func TestNormalizeTypes_AsksForAnExpressionAsWritten(t *testing.T) {
	c := qt.New(t)
	scripted := &server{defaultText: "lower('X'::text)"}
	db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
	mail := domainNamed("mail", &pgtype.DesiredDomain{BaseType: "text", Default: "x", DefaultExpr: "lower('X')"})

	result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(session{db: db.SQL}, mail))

	c.Assert(err, qt.IsNil)
	c.Assert(normalizedDomain(c, result, "mail"), qt.DeepEquals, &pgtype.NormalizedDomain{Default: "lower('X'::text)"})
	c.Assert(scripted.executed[1], qt.Equals, "CREATE DOMAIN pg_temp.ptah_domain_probe_0 AS text DEFAULT lower('X')")
}

// TestNormalizeTypes_LeavesWhatItCouldNotAnswer pins the cases that attach
// nothing and fail nothing: a session that could not isolate the probe, a
// declaration the server refused, which rolls back to the savepoint, and an
// answer the model cannot hold.
func TestNormalizeTypes_LeavesWhatItCouldNotAnswer(t *testing.T) {
	tests := []struct {
		name         string
		server       server
		declined     bool
		wantExecuted []string
	}{
		{name: "a declined session", server: server{check: "true"}, declined: true},
		{name: "a refused declaration", server: server{refuse: "CREATE DOMAIN"}, wantExecuted: []string{
			"SAVEPOINT ptah_domain_probe", "CREATE DOMAIN pg_temp.ptah_domain_probe_0 AS missing CHECK (VALUE > 0)",
			"ROLLBACK TO SAVEPOINT ptah_domain_probe",
		}},
		{name: "an answer the model cannot hold", server: server{check: "VALUE > 0\x00"}, wantExecuted: []string{
			"SAVEPOINT ptah_domain_probe", "CREATE DOMAIN pg_temp.ptah_domain_probe_0 AS missing CHECK (VALUE > 0)",
			"ROLLBACK TO SAVEPOINT ptah_domain_probe",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := test.server
			db := dbtest.OpenWithExec(t, scripted.query, scripted.exec)
			mail := domainNamed("mail", &pgtype.DesiredDomain{BaseType: "missing", Check: "VALUE > 0"})

			result, err := newRuntime(c).NormalizeObjects(t.Context(), normalization(session{db: db.SQL, declined: test.declined}, mail))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(normalizedDomain(c, result, "mail"), qt.IsNil)
			c.Assert(scripted.executed, qt.DeepEquals, test.wantExecuted)
		})
	}
}

// TestNormalizeTypes_FailurePath pins the refusals that come before any
// statement. The runtime sends the owner no target it was not registered for
// and no model it does not normalize, so each is asked of the service
// directly.
func TestNormalizeTypes_FailurePath(t *testing.T) {
	declared := domainNamed("mail", &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''"})
	outside := normalization(session{}, declared)
	outside.Target = "mysql"
	composite := normalization(session{}, must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "mail"),
		&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}})))
	tests := []struct {
		name    string
		request schemaext.NormalizationRequest
		is      error
	}{
		{name: "no session", request: normalization(nil, declared), is: schemaext.ErrInvalidValue},
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect},
		{name: "a composite type", request: composite, is: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := typeprobe.Service{}.NormalizeObjects(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

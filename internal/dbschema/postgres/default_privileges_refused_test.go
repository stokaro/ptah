package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// sqlStateError is a driver error carrying a SQLSTATE, as pgconn.PgError does.
type sqlStateError struct {
	state   string
	message string
}

func (e sqlStateError) Error() string    { return "ERROR: " + e.message + " (SQLSTATE " + e.state + ")" }
func (e sqlStateError) SQLState() string { return e.state }

// pgDefaultACLRefusal is what CockroachDB v26.2.7 answered every read of
// pg_default_acl with once a default named the role r-dash.
var pgDefaultACLRefusal = sqlStateError{state: "22P02", message: `missing "=" sign: "r-dash=U*/"`}

// refusingDefaultACL answers a full ReadSchemaContext and fails every
// statement that names pg_default_acl with failure, recording each statement.
// A read that still asked the relation after the refusal fails with it, so the
// read succeeding is itself the proof that nothing asked again.
func refusingDefaultACL(failure error, asked *[]string) dbtest.QueryHandler {
	base := catalogAnswers(intact, asked)
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		result, err := base(query, args)
		if strings.Contains(query, "pg_default_acl") {
			return dbtest.QueryResult{}, failure
		}
		return result, err
	}
}

// defaultACLStatements keeps the statements that name pg_default_acl.
func defaultACLStatements(asked []string) []string {
	var named []string
	for _, query := range asked {
		if strings.Contains(query, "pg_default_acl") {
			named = append(named, query)
		}
	}
	return named
}

// TestReadSchemaContext_RecordsARefusedDefaultACLHappyPath reads a server that
// refuses pg_default_acl, as CockroachDB v26.2.7 does once a default privilege
// names a role that needs quoting (stokaro/ptah#3816).
//
// The read succeeds, carries no default privilege, and records the kind as not
// inspected, which is what keeps a comparison from reading the silence as
// absence. The relation is asked once: the role scoping and both default
// privilege reads leave it alone after the refusal, or they would fail with it.
func TestReadSchemaContext_RecordsARefusedDefaultACLHappyPath(t *testing.T) {
	c := qt.New(t)
	var asked []string
	db := dbtest.Open(c, refusingDefaultACL(pgDefaultACLRefusal, &asked))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.CockroachDB26())

	schema, err := reader.ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.DefaultPrivileges, qt.IsNil)
	c.Assert(schema.UndescribedDefaultPrivileges, qt.IsNil)
	c.Assert(schema.NotDescribed, qt.DeepEquals, coverage.Set{}.With(coverage.Refused(coverage.DefaultPrivilege)))
	c.Assert(defaultACLStatements(asked), qt.DeepEquals, []string{"SELECT 1 FROM pg_default_acl"})
}

// TestReadSchemaContext_UnreadableDefaultACLFailurePath fails the read on any
// other error from pg_default_acl. Recording it as a refusal would turn a
// broken connection into a description that says nothing about default
// privileges.
func TestReadSchemaContext_UnreadableDefaultACLFailurePath(t *testing.T) {
	c := qt.New(t)
	var asked []string
	broken := sqlStateError{state: "08006", message: "connection failure"}
	db := dbtest.Open(c, refusingDefaultACL(broken, &asked))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.CockroachDB26())

	schema, err := reader.ReadSchemaContext(c.Context())

	c.Assert(err, qt.ErrorIs, broken)
	c.Assert(err, qt.ErrorMatches, `.*failed to read default privileges: failed to read pg_default_acl: ERROR: connection failure \(SQLSTATE 08006\)`)
	c.Assert(schema, qt.IsNil)
}

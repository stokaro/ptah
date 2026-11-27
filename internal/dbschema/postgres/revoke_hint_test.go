package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestWriterHintsAtDependentGrants(t *testing.T) {
	for _, statement := range []string{
		"REVOKE GRANT OPTION FOR SELECT ON t FROM reader",
		"/* permission */ revoke grant option for SELECT (id) ON t FROM reader; -- end",
		"REVOKE GRANT OPTION FOR EXECUTE ON FUNCTION f(integer) FROM reader;",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			refusal := &pgconn.PgError{Code: "2BP01", Message: "dependent privileges exist",
				Detail: "DETAIL-SENTINEL", Hint: "HINT-SENTINEL"}

			err := passwordWriter(&passwordRunner{refusal: refusal}).ExecuteSQL(c.Context(), statement)

			c.Assert(err, qt.ErrorIs, error(refusal))
			c.Assert(err, qt.ErrorMatches, `(?s).*\nhint: revoke the dependent grants first, then retry the grant-option revoke`)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*SENTINEL.*`)
		})
	}
}

func TestWriterDoesNotSuggestGrantChangesForOtherErrors(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		code      string
	}{
		{name: "another code", statement: "REVOKE GRANT OPTION FOR SELECT ON t FROM reader", code: "42501"},
		{name: "a dropped object", statement: "DROP TABLE t", code: "2BP01"},
		{name: "a full revoke", statement: "REVOKE SELECT ON t FROM reader", code: "2BP01"},
		{name: "a quoted prefix", statement: `"REVOKE" GRANT OPTION FOR SELECT ON t FROM reader`, code: "2BP01"},
		{name: "multiple statements", statement: "REVOKE GRANT OPTION FOR SELECT ON t FROM reader; DROP TABLE t", code: "2BP01"},
		{name: "only a comment", statement: "/* REVOKE GRANT OPTION FOR SELECT ON t FROM reader */ DROP TABLE t", code: "2BP01"},
		{name: "only a literal", statement: "SELECT 'REVOKE GRANT OPTION FOR SELECT ON t FROM reader'", code: "2BP01"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := &pgconn.PgError{Code: test.code, Message: "refused", Detail: "DETAIL-SENTINEL", Hint: "HINT-SENTINEL"}

			err := passwordWriter(&passwordRunner{refusal: refusal}).ExecuteSQL(c.Context(), test.statement)

			c.Assert(err, qt.ErrorIs, error(refusal))
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*hint:.*`)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*SENTINEL.*`)
		})
	}
}

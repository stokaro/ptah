package schemamodel

// White-box testing required: postgresQuoting is the lexer mode the routine
// case fold reads quotes with, and it is unexported because nothing outside
// the fold has a use for it. The fold's black-box tests show its effect on
// text; only this test can say the mode is the one internal/dialectlexer gives
// PostgreSQL, which keeps the two from drifting apart.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
)

func TestPostgresQuoting_IsPostgreSQLsLexerMode(t *testing.T) {
	c := qt.New(t)

	c.Assert(postgresQuoting, qt.Equals, dialectlexer.Options(platform.Postgres))
}

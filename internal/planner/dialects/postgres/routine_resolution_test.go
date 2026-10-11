package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
)

// The PostgreSQL family resolves a LANGUAGE sql body when the routine is
// created, in any letter case of the language, and resolves a PL/pgSQL body,
// or one stating no language, at its first call.
func TestResolvesBodyAtCreate(t *testing.T) {
	for _, test := range []struct {
		language string
		want     bool
	}{
		{language: "sql", want: true},
		{language: " SQL ", want: true},
		{language: "plpgsql", want: false},
		{language: "", want: false},
	} {
		t.Run(test.language, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgres.ResolvesBodyAtCreate(schemamodel.Function{Language: test.language, Body: "SELECT 1"}), qt.Equals, test.want)
		})
	}
}

package generator

// White-box testing required: a plan whose reverse holds no statement is reached
// through the down renderer, and no exported entry point writes a migration for a
// diff that plans nothing in either direction.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/engine/builtin"
	"ptah.run/migration/migrationfile"
)

// TestRenderedDown_AnEmptyReverseStatesItHasNoRollback holds the generator to the
// rule a hand-written file meets. It decided nothing has to be undone, so the file
// says so in the form `migrations down` reads, and a rollback through it and the
// round trip a generate's shadow verification runs are not refused.
func TestRenderedDown_AnEmptyReverseStatesItHasNoRollback(t *testing.T) {
	c := qt.New(t)

	down, err := downMigrationSQL(c.Context(), must.Must(builtin.New()), &difftypes.SchemaDiff{}, nil, nil, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "-- No rollback operations needed\n")
	declared, err := migrationfile.DeclaresEmptyRollback(down, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.IsTrue)
}

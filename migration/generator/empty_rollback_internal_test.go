package generator

// White-box testing required: a plan whose reverse holds no statement is reached
// through the down renderer, and no exported entry point writes a migration for a
// diff that plans nothing in either direction, or for one whose rollback is notes
// alone: a built-in owner always reverses with a statement, and only an
// executable provider's reversal runs nothing and says why.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
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

// TestRenderedDown_NotesAloneStateTheyHaveNoRollback holds a reverse plan that
// runs nothing and carries notes to the same rule. The reverse here holds one
// note, about an object the target skips, and no statement; a provider's
// rollback of a secret rotation has the same shape, with a note saying the
// rollback keeps the rotated value. The generator decided nothing can be run,
// so the file says so in the form `migrations down` reads, and keeps the note
// that says what the rollback leaves.
func TestRenderedDown_NotesAloneStateTheyHaveNoRollback(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{SkippedFeatures: []schemaext.SkippedObject{{
		Subject: objectidentity.ID{Kind: "example.org/extension", Name: objectidentity.Part{Source: "pg_trgm", Normalized: "pg_trgm"}},
		Label:   "PostgreSQL extension", Target: "mysql"}}}

	down, err := downMigrationSQL(c.Context(), must.Must(builtin.New()), diff, nil, nil, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "-- MYSQL: PostgreSQL extension pg_trgm is not supported by this target; skipped.")
	c.Assert(down, qt.Not(qt.Contains), "No rollback operations needed")
	declared, err := migrationfile.DeclaresEmptyRollback(down, "mysql")
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.IsTrue)
}

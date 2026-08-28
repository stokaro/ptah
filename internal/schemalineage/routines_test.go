package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemalineage"
)

// TestDeriveRoutines_ResolvesWhatItCanAndNamesWhatItCannot pins both halves.
//
// "Nothing depends on this column" and "I could not tell" decide different
// things, and a confident wrong answer about a drop is worse than no answer, so
// an unresolved routine is named rather than silent (stokaro/ptah#2394).
func TestDeriveRoutines_ResolvesWhatItCanAndNamesWhatItCannot(t *testing.T) {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "U", Name: "id", Type: "BIGINT"},
			{StructName: "U", Name: "email", Type: "TEXT"},
		},
		// Declared out of alphabetical order on purpose: a sorted result has to
		// be the sort's doing rather than the fixture's.
		Functions: []schemamodel.Function{
			{Name: "run_it", Kind: "procedure", Language: "sql", Body: "SELECT id FROM users"},
			{Name: "empty_body", Language: "sql"},
			{Name: "counts", Language: "plpgsql", Body: "BEGIN RETURN 1; END;"},
			{Name: "active_emails", Language: "sql", Body: "SELECT email FROM users"},
		},
	}

	c := qt.New(t)

	result := schemalineage.DeriveRoutines(db)

	c.Assert(result.Edges, qt.HasLen, 2)
	c.Assert(result.Edges[0].ToRoutine, qt.Equals, "active_emails")
	c.Assert(result.Edges[0].FromTable, qt.Equals, "users")
	c.Assert(result.Edges[0].FromColumn, qt.Equals, "email")
	c.Assert(result.Edges[0].Kind, qt.Equals, "function")

	c.Assert(result.Edges[1].ToRoutine, qt.Equals, "run_it")
	c.Assert(result.Edges[1].Kind, qt.Equals, "procedure")

	c.Assert(undecidedRoutines(result), qt.DeepEquals, []string{"counts", "empty_body"})
}

// TestDeriveRoutines_AProceduralBodyIsUndecidedRatherThanEmpty is the control
// the boundary needs.
//
// A procedural body needs the references the parser does not yet yield; #1270
// records that as the prerequisite the rest waits on, and names matching
// identifiers lexically as the approach #1280/#1281 measured as wrong. Silence
// here would read as "this routine reads nothing".
func TestDeriveRoutines_AProceduralBodyIsUndecidedRatherThanEmpty(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "f", Language: "plpgsql", Body: "BEGIN SELECT email FROM users; END;"},
		},
	})

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Routine, qt.Equals, "f")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "plpgsql")
}

// TestDeriveRoutines_NoRoutinesIsNotAnUndecided is the other control: a schema
// with nothing to resolve must produce nothing, or every empty run would look
// like an unresolved one.
func TestDeriveRoutines_NoRoutinesIsNotAnUndecided(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
	})

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// undecidedRoutines lists the routines a result could not resolve.
func undecidedRoutines(result schemalineage.RoutineResult) []string {
	names := make([]string, 0, len(result.Undecided))
	for _, undecided := range result.Undecided {
		names = append(names, undecided.Routine)
	}
	return names
}

// TestDeriveRoutines_AnUnresolvableSQLBodyCarriesTheReadersReason pins the
// passthrough.
//
// The body is plain SQL, so the language guard lets it through, and the reader
// that resolves a select list over one source cannot resolve a join. Swallowing
// its answer here would report the routine as reading nothing at all, which is
// the confident wrong answer this package exists to avoid.
func TestDeriveRoutines_AnUnresolvableSQLBodyCarriesTheReadersReason(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "U", Name: "email", Type: "TEXT"}},
		Functions: []schemamodel.Function{
			{Name: "joined", Language: "sql", Body: "SELECT email FROM users JOIN orders ON orders.user_id = users.id"},
		},
	})

	c.Assert(result.Edges, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Routine, qt.Equals, "joined")
	c.Assert(result.Undecided[0].Reason, qt.Not(qt.Equals), "")
	c.Assert(result.Undecided[0].Reason, qt.Not(qt.Contains), "plain SQL")
}

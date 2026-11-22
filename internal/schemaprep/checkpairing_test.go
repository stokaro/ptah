package schemaprep_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// heldPairing is a database holding the CHECKs of held per table, which pairs
// a declared CHECK with a held one of the same text. It records the
// expressions each call asks about.
type heldPairing struct {
	held  map[string][]schemaprep.HeldCheck
	asked *[][]string
}

func (p heldPairing) Held(table schemamodel.Table) []schemaprep.HeldCheck {
	return p.held[table.Name]
}

func (p heldPairing) Match(_ context.Context, _ schemamodel.Table, expressions []string, candidates []schemaprep.HeldCheck) ([][]bool, error) {
	*p.asked = append(*p.asked, expressions)
	alike := make([][]bool, len(expressions))
	for i, expression := range expressions {
		alike[i] = make([]bool, len(candidates))
		for j, held := range candidates {
			alike[i][j] = held.Expression == expression
		}
	}
	return alike, nil
}

// failingPairing is a database whose pairing answers for one declaration
// fewer than it was asked about, and fails with err where err is set.
type failingPairing struct{ err error }

func (failingPairing) Held(schemamodel.Table) []schemaprep.HeldCheck {
	return []schemaprep.HeldCheck{{Name: "t_check", Expression: "v < 10"}}
}

func (p failingPairing) Match(_ context.Context, _ schemamodel.Table, expressions []string, _ []schemaprep.HeldCheck) ([][]bool, error) {
	return make([][]bool, len(expressions)-1), p.err
}

// An unnamed CHECK keeps the name the database holds it under, whatever its
// place among the declarations, so reordering the checks list or naming a
// neighbor renames nothing; only the CHECKs the database does not hold are
// numbered, past every name taken. The pairing is asked once for each table
// that holds a CHECK no declaration names, about all of its unnamed CHECKs.
func TestAssignConstraintNamesAgainst_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		given []schemamodel.Constraint
		held  map[string][]schemaprep.HeldCheck
		want  []string
		asked [][]string
	}{
		{
			name:  "two unnamed CHECKs swapped keep their names",
			given: []schemamodel.Constraint{check("t", "", "v != 5"), check("t", "", "v < 10")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check", Expression: "v < 10"}, {Name: "t_check1", Expression: "v != 5"}}},
			want:  []string{"t_check1", "t_check"},
			asked: [][]string{{"v != 5", "v < 10"}},
		},
		{
			name:  "a named neighbor that takes the first name moves only the CHECK it displaces",
			given: []schemamodel.Constraint{check("t", "t_check", "v != 7"), check("t", "", "v < 10"), check("t", "", "v != 5")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check", Expression: "v < 10"}, {Name: "t_check1", Expression: "v != 5"}}},
			want:  []string{"t_check", "t_check2", "t_check1"},
			asked: [][]string{{"v < 10", "v != 5"}},
		},
		{
			name:  "a held CHECK with another condition is numbered over",
			given: []schemamodel.Constraint{check("t", "", "v < 10")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check", Expression: "v < 9"}}},
			want:  []string{"t_check"},
			asked: [][]string{{"v < 10"}},
		},
		{
			name:  "a held CHECK pairs with one declaration",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("t", "", "v < 10")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check1", Expression: "v < 10"}}},
			want:  []string{"t_check1", "t_check"},
			asked: [][]string{{"v < 10", "v < 10"}},
		},
		{
			name:  "two held CHECKs of one condition pair in order",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("t", "", "v < 10")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check1", Expression: "v < 10"}, {Name: "t_check2", Expression: "v < 10"}}},
			want:  []string{"t_check1", "t_check2"},
			asked: [][]string{{"v < 10", "v < 10"}},
		},
		{
			name:  "a held name a declaration names is not paired",
			given: []schemamodel.Constraint{check("t", "t_check", "v > 100"), check("t", "", "v < 10")},
			held:  map[string][]schemaprep.HeldCheck{"t": {{Name: "t_check", Expression: "v < 10"}}},
			want:  []string{"t_check", "t_check1"},
			asked: nil,
		},
		{
			name:  "a CHECK held on another table is not paired",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("u", "", "id > 0")},
			held:  map[string][]schemaprep.HeldCheck{"u": {{Name: "u_check3", Expression: "v < 10"}, {Name: "u_check7", Expression: "id > 0"}}},
			want:  []string{"t_check", "u_check7"},
			asked: [][]string{{"id > 0"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := checkNamingDatabase(test.given...)
			var asked [][]string

			assigned, err := schemaprep.AssignConstraintNamesAgainst(t.Context(), database, platform.ClickHouse, heldPairing{held: test.held, asked: &asked})

			c.Assert(err, qt.IsNil)
			names := make([]string, len(assigned.Constraints))
			for i, constraint := range assigned.Constraints {
				names[i] = constraint.Name
			}
			c.Assert(names, qt.DeepEquals, test.want)
			c.Assert(database.Constraints, qt.DeepEquals, test.given)
			c.Assert(asked, qt.DeepEquals, test.asked)
		})
	}
}

// A pairing that fails fails the naming, and no half-named schema is
// returned.
func TestAssignConstraintNamesAgainst_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("refused")

	assigned, err := schemaprep.AssignConstraintNamesAgainst(t.Context(), checkNamingDatabase(check("t", "", "v < 10")), platform.ClickHouse, failingPairing{err: refused})

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(assigned, qt.IsNil)
}

// A pairing that answers for fewer declarations than it was asked about fails
// the naming rather than leave a declaration unpaired.
func TestAssignConstraintNamesAgainst_AShortAnswerFails(t *testing.T) {
	c := qt.New(t)
	database := checkNamingDatabase(check("t", "", "v < 10"), check("t", "", "v > 0"))

	assigned, err := schemaprep.AssignConstraintNamesAgainst(t.Context(), database, platform.ClickHouse, failingPairing{})

	c.Assert(err, qt.ErrorMatches, `name the CHECKs of t: the pairing answered 1 of 2 declarations`)
	c.Assert(assigned, qt.IsNil)
}

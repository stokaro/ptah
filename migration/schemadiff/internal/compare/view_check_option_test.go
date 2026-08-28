package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestViewDefinitions_TheCheckOptionIsCompared covers the rule this comparison
// shares with the conversion and the removal carry.
//
// The catalog reports a word -- NONE, LOCAL, CASCADED, or a dialect equivalent
// -- and the model holds a bool, so the two have to be read by one rule or they
// answer differently the first time either learns something. That rule is
// sqlutil.CheckOptionRequestsCheck (stokaro/ptah#2315).
//
// Nothing measured this comparison: replacing it with a constant `false` left
// the whole suite green, which is why the rows below include both directions
// and a control that must report nothing.
func TestViewDefinitions_TheCheckOptionIsCompared(t *testing.T) {
	tests := []struct {
		name        string
		declared    bool
		reported    string
		wantChange  string
		wantChanged bool
		why         string
	}{
		{
			name:        "a declared check option the server does not have",
			declared:    true,
			reported:    "NONE",
			wantChange:  "false -> true",
			wantChanged: true,
			why:         "the view asks for WITH CHECK OPTION and does not have it",
		},
		{
			name:        "a check option the server has and the schema drops",
			declared:    false,
			reported:    "CASCADED",
			wantChange:  "true -> false",
			wantChanged: true,
			why:         "the clause is on the server and the declaration stopped asking for it",
		},
		{
			name:        "an unknown word the server reports is a check option",
			declared:    false,
			reported:    "CASCADE",
			wantChange:  "true -> false",
			wantChanged: true,
			why:         "a dialect equivalent is a clause the view has; reading it as none would hide the change",
		},
		{
			name:        "agreement reports nothing",
			declared:    true,
			reported:    "LOCAL",
			wantChanged: false,
			why:         "the control: a row that cannot fail proves the rows above are not always-true",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			viewDiff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: "SELECT 1", WithCheck: test.declared},
				catalog.View{Name: "v", Body: "SELECT 1", CheckOption: test.reported},
				"postgres",
			)

			c.Assert(viewDiff.Changes["with_check"], qt.Equals, test.wantChange,
				qt.Commentf("%s", test.why))
		})
	}
}

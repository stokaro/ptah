package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestViewDefinitions_TheCheckOptionIsCompared covers the rule this comparison
// shares with the conversion and the removal carry.
//
// The catalog reports a word -- NONE, LOCAL, CASCADED, or a dialect equivalent
// -- and the model holds two bools, so the two have to be read by one rule or
// they answer differently the first time either learns something. That rule is
// sqlutil.CheckOptionRequestsCheck with sqlutil.CheckOptionIsLocal
// (stokaro/ptah#2315).
//
// The level is compared too. A LOCAL view checks a row against its own
// condition alone, so a declared CASCADED check option is a change from a held
// LOCAL one, and the other way round (stokaro/ptah#5647).
func TestViewDefinitions_TheCheckOptionIsCompared(t *testing.T) {
	tests := []struct {
		name       string
		withCheck  bool
		local      bool
		reported   string
		wantChange string
		why        string
	}{
		{
			name:       "a declared check option the server does not have",
			withCheck:  true,
			reported:   "NONE",
			wantChange: "NONE -> CASCADED",
			why:        "the view asks for WITH CHECK OPTION and does not have it",
		},
		{
			name:       "a check option the server has and the schema drops",
			reported:   "CASCADED",
			wantChange: "CASCADED -> NONE",
			why:        "the clause is on the server and the declaration stopped asking for it",
		},
		{
			name:       "an unknown word the server reports is a check option",
			reported:   "CASCADE",
			wantChange: "CASCADED -> NONE",
			why:        "a dialect equivalent is a clause the view has; reading it as none would hide the change",
		},
		{
			name:       "a held LOCAL check option the schema declares CASCADED",
			withCheck:  true,
			reported:   "LOCAL",
			wantChange: "LOCAL -> CASCADED",
			why:        "the declared view checks the conditions of the views it reads, and the held one does not",
		},
		{
			name:       "a held CASCADED check option the schema declares LOCAL",
			withCheck:  true,
			local:      true,
			reported:   "CASCADED",
			wantChange: "CASCADED -> LOCAL",
			why:        "the held view checks more than the declared one",
		},
		{
			name:       "a declared LOCAL check option the server does not have",
			withCheck:  true,
			local:      true,
			reported:   "NONE",
			wantChange: "NONE -> LOCAL",
			why:        "the view asks for WITH LOCAL CHECK OPTION and does not have it",
		},
		{
			name:     "agreement at the CASCADED level reports nothing",
			reported: "CASCADED", withCheck: true,
			why: "the control: a row that cannot fail proves the rows above are not always-true",
		},
		{
			name:     "agreement at the LOCAL level reports nothing, in any case",
			reported: "local", withCheck: true, local: true,
			why: "the control for the level rows",
		},
		{
			name:     "LOCAL without a check option is no check option",
			reported: "NONE", local: true,
			why: "LOCAL narrows a check option and declares none of its own",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			viewDiff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: "SELECT 1", WithCheck: test.withCheck, LocalCheck: test.local},
				catalog.View{Name: "v", Body: "SELECT 1", CheckOption: test.reported},
				"postgres",
			)

			c.Assert(viewDiff.Changes["with_check"], qt.Equals, test.wantChange,
				qt.Commentf("%s", test.why))
		})
	}
}

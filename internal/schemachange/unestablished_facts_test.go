package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// TestAnUnestablishedFactIsNotAMeasuredAbsence separates the two things a
// missing capability can mean.
//
// A preset for a dialect Ptah knows fills every key, so a false there was
// decided and the change is blocked BY THE TARGET. A dialect Ptah does not know
// gets the empty set, where nothing is answered -- and read through the same
// question that is indistinguishable from a target that answered no. An
// operator pointing Ptah at an unrecognized server was told it lacked the
// feature, fifty capabilities over, when the truth was that no profile for it
// exists (stokaro/ptah#1348).
//
// The diagnostics differ because they ask for different things: one says fix
// the schema or the target, the other says name a target Ptah knows.
func TestAnUnestablishedFactIsNotAMeasuredAbsence(t *testing.T) {
	tests := []struct {
		name           string
		profile        schemastate.Profile
		wantStatus     schemachange.Status
		wantDiagnostic string
	}{
		{
			// ClickHouse parses no FOREIGN KEY clause, and the preset says so.
			name:           "the target answered no",
			profile:        clickhouseProfile(),
			wantStatus:     schemachange.Blocked,
			wantDiagnostic: "the target does not have",
		},
		{
			name:           "nothing answered at all",
			profile:        unknownTargetProfile(),
			wantStatus:     schemachange.Undecidable,
			wantDiagnostic: "nothing established whether the target has",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesForProfile(c,
				parentChildDescription("CASCADE"), parentChildCatalog("NO ACTION"), test.profile)

			foreignKeys := changesOfKind(changes, "constraint")
			c.Assert(foreignKeys, qt.Not(qt.HasLen), 0)
			c.Assert(foreignKeys[0].Status, qt.Equals, test.wantStatus)
			c.Assert(foreignKeys[0].Diagnostic, qt.Contains, test.wantDiagnostic)
		})
	}
}

// unknownTargetProfile is a target Ptah has no preset for: every capability is
// unanswered rather than answered no.
func unknownTargetProfile() schemastate.Profile {
	return schemastate.Profile{
		Dialect:      "unknowndb",
		Semantics:    identifier.ForDialect("unknowndb"),
		Capabilities: capability.ForDialect("unknowndb"),
	}
}

// changesOfKind is every change planned for one object kind.
func changesOfKind(changes []schemachange.Change, kind string) []schemachange.Change {
	matched := make([]schemachange.Change, 0, len(changes))
	for _, change := range changes {
		if string(change.ID.Kind) == kind {
			matched = append(matched, change)
		}
	}
	return matched
}

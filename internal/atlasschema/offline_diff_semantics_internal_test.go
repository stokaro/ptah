package atlasschema

// White-box testing required: offlineDiffSemantics chooses between the rules
// a --to database was read with and an answer a server gave for the names of
// both sides. Only a connected SQL Server produces both, so the precedence
// cannot be reached offline through the exported diff; the live half is
// TestSchemaDiffFromAFileToASQLServerDatabaseE2E.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/atlassource"
)

// TestOfflineDiffSemantics_PrefersTheAnswerForBothSides reads a --to state as
// a SQL Server read records it, with rules that cover none of its names, and
// an answer for both sides. The comparison refuses the first as an invalid
// snapshot, so preferring it failed every `schema diff --from <file> --to
// <database>` on SQL Server (stokaro/ptah#6749).
func TestOfflineDiffSemantics_PrefersTheAnswerForBothSides(t *testing.T) {
	read := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS")
	answered := read.WithResolvedNames([]identifier.ResolvedName{
		{Name: "ORDERS", Key: "ORDERS"}, {Name: "Orders", Key: "ORDERS"}, {Name: "dbo", Key: "dbo"},
	})
	tests := []struct {
		name     string
		to       atlassource.State
		resolved *identifier.Semantics
		want     *identifier.Semantics
	}{
		{name: "an answer beside a --to read", to: atlassource.State{IdentifierSemantics: &read}, resolved: &answered, want: &answered},
		{name: "a --to read alone", to: atlassource.State{IdentifierSemantics: &read}, want: &read},
		{name: "an answer alone", resolved: &answered, want: &answered},
		{name: "neither"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := offlineDiffSemantics(test.to, test.resolved)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

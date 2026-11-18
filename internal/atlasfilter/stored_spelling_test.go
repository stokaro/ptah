package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// TestExclude_MatchesTheStoredSpellingOnBothSides excludes tables from an
// Oracle read and from the schema converted from it. The read reports the
// table created quoted as orders and its folded namesake as ORDERS; the
// converted schema spells the first `"orders"`. A selector matches the stored
// spelling on both, so orders leaves the same table on each side and ORDERS
// the other one. Matched against the spelling, orders would leave the read's
// table and keep the converted one, and a diff of the database against itself
// would plan the excluded table.
func TestExclude_MatchesTheStoredSpellingOnBothSides(t *testing.T) {
	tests := []struct {
		exclude string
		// read and converted are the tables each side keeps, as each spells
		// them.
		read      []string
		converted []string
	}{
		{exclude: "orders", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
		{exclude: "ORDERS", read: []string{"KEEP_ME", "orders"}, converted: []string{"KEEP_ME", `"orders"`}},
		{exclude: "APP.orders", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
		{exclude: "o*", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: "APP"}
			read := &catalog.Database{StoredNames: true, Tables: []catalog.Table{
				{Name: "KEEP_ME", Schema: "APP"}, {Name: "orders", Schema: "APP"}, {Name: "ORDERS", Schema: "APP"},
			}}
			converted := &schemamodel.Database{Tables: []schemamodel.Table{
				{StructName: "keep_me", Name: "KEEP_ME", Schema: "APP"},
				{StructName: "orders", Name: `"orders"`, Schema: "APP"},
				{StructName: "ORDERS", Name: "ORDERS", Schema: "APP"},
			}}

			scopedRead, _, readErr := atlasfilter.ScopeDatabaseReport(read, scope)
			scopedConverted, _, convertedErr := atlasfilter.ExcludeGeneratedScopeReport(converted, scope)

			c.Assert(readErr, qt.IsNil)
			c.Assert(convertedErr, qt.IsNil)
			readNames := make([]string, 0, len(scopedRead.Tables))
			for _, table := range scopedRead.Tables {
				readNames = append(readNames, table.Name)
			}
			convertedNames := make([]string, 0, len(scopedConverted.Tables))
			for _, table := range scopedConverted.Tables {
				convertedNames = append(convertedNames, table.Name)
			}
			c.Assert(readNames, qt.ContentEquals, test.read)
			c.Assert(convertedNames, qt.ContentEquals, test.converted)
		})
	}
}

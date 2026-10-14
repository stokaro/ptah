package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
)

// TestFromDescriptor_HappyPath pins that every registered descriptor states
// complete offline rules, and that ForDialect answers with exactly those.
func TestFromDescriptor_HappyPath(t *testing.T) {
	for _, described := range platform.Dialects().Dialects() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			semantics, err := identifier.FromDescriptor(described)

			c.Assert(err, qt.IsNil)
			c.Assert(semantics.DefaultSchema, qt.Equals, described.DefaultSchema)
			c.Assert(string(semantics.IndexNamespace), qt.Equals, described.Identifiers.IndexNamespace)
			c.Assert(string(semantics.IndexNames), qt.Equals, described.Identifiers.IndexNames)
			c.Assert(string(semantics.TableNames), qt.Equals, described.Identifiers.TableNames)
			c.Assert(string(semantics.ColumnNames), qt.Equals, described.Identifiers.ColumnNames)
			c.Assert(identifier.ForDialect(described.Name), qt.DeepEquals, semantics)
		})
	}
}

func TestFromDescriptor_FailurePath(t *testing.T) {
	complete := descriptor.Identifiers{IndexNamespace: "table", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"}
	withNamespace := complete
	withNamespace.IndexNamespace = "database"
	withComparison := complete
	withComparison.TableNames = "case_insensitive"
	withoutColumns := complete
	withoutColumns.ColumnNames = ""
	catalogResolved := descriptor.Identifiers{IndexNamespace: "table", IndexNames: "catalog_resolved", TableNames: "catalog_resolved", ColumnNames: "catalog_resolved"}
	asciiResolved := complete
	asciiResolved.IndexNames = "ascii_folded_non_ascii_resolved"
	tests := []struct {
		name        string
		identifiers descriptor.Identifiers
	}{
		{name: "an undeclared namespace", identifiers: withNamespace},
		{name: "an undeclared comparison", identifiers: withComparison},
		{name: "a missing comparison", identifiers: withoutColumns},
		{name: "a comparison only a live catalog answers", identifiers: catalogResolved},
		{name: "a comparison only a live server answers", identifiers: asciiResolved},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics, err := identifier.FromDescriptor(descriptor.Dialect{Name: "acme", Identifiers: test.identifiers})

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, `invalid dialect descriptor: dialect "acme" states identifier rules .*, which are not complete offline rules`)
			c.Assert(semantics.IsZero(), qt.IsTrue)
		})
	}
}

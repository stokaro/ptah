package providerhost

// White-box testing required: the selector adapter validates identities before
// a provider's values enter the shared model; starting a provider crosses a
// process boundary and belongs to the integration tests.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/objectidentity"
)

func scopeDescription() *protocol.Description {
	return &protocol.Description{ProviderId: "example.org/queue", Engines: []*protocol.Engine{{
		Id: "example", DefaultSchema: "main",
		Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
			ColumnNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, IndexNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
			IndexNamespace: protocol.IdentifierRules_SCHEMA},
		ScopeSelections: []*protocol.ScopeSelection{
			{Kind: "example.org/queue", SelectorType: "queue", Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC"},
			{Kind: "example.org/pool", SelectorType: "pool", Naming: protocol.ScopeSelection_DATABASE_WIDE},
		},
	}}}
}

func scopePart(name string) objectidentity.Part {
	return objectidentity.Part{Source: name, Normalized: name}
}

func TestScopeSelectionsGrantUsesTheEnginesDefaultSchema(t *testing.T) {
	c := qt.New(t)
	selection, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)

	qualified, found := selection.extension.Grants.GrantObject("topic", "MAIN.Jobs")
	c.Assert(found, qt.IsTrue)
	unqualified, found := selection.extension.Grants.GrantObject(" TOPIC ", "jobs")
	c.Assert(found, qt.IsTrue)
	c.Assert(qualified.Key(), qt.Equals, unqualified.Key())
	c.Assert(qualified.Schema.Normalized, qt.Equals, "main")
	c.Assert(qualified.Name.Normalized, qt.Equals, "jobs")
	c.Assert(selection.fit(qualified), qt.IsNil)
}

func TestScopeSelectionsRefuseIdentitiesWhoseQualifiersWouldBeLost(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "database qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Database: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "catalog qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Catalog: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "parent qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Parent: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "missing default schema", ref: objectidentity.ID{Kind: "example.org/queue", Name: scopePart("jobs")}},
		{name: "schema on a database object", ref: objectidentity.ID{Kind: "example.org/pool", Schema: scopePart("main"), Name: scopePart("fast")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selection, err := newScopeSelections(scopeDescription())
			c.Assert(err, qt.IsNil)
			c.Assert(selection.fit(test.ref), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestScopeSelectionsKeepDistinctObjectNames(t *testing.T) {
	c := qt.New(t)
	selection, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/queue", Schema: scopePart("main"), Name: scopePart("jobs")}), qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/pool", Name: scopePart("fast.pool")}), qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/unselected", Database: scopePart("other"), Name: scopePart("jobs")}), qt.IsNil)
}

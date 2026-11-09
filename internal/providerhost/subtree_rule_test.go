package providerhost_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/provider-sdk/provider"
)

// The host's coverage lookup states the subtree rule in core/schemaext, which
// does not import the provider SDK, and a provider decides it with
// provider.SchemaWithin. Both answer the same for every pair, so a read the
// provider claims and a comparison the host bounds agree on what a subtree
// region covers.
func TestSubtreeRuleMatchesTheProviderSDK(t *testing.T) {
	schemas := []string{"", "a", "a/b", "a/b/c", "ab", "b", "a/bc", "hidden", "hidden/inner"}
	for _, region := range schemas {
		for _, subject := range schemas {
			t.Run(region+" covers "+subject, func(t *testing.T) {
				c := qt.New(t)
				scope := schemaext.NamespaceScope{Schema: &objectidentity.Part{Source: region, Normalized: region}, SchemaSubtree: true, SchemaSeparator: "/"}
				domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside"},
					schemaext.NamespaceKnowledge{Scope: scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
				c.Assert(err, qt.IsNil)
				id := objectidentity.ID{Kind: "example.org/fixture/secret", Schema: objectidentity.Part{Source: subject, Normalized: subject},
					Name: objectidentity.Part{Source: "pw", Normalized: "pw"}}

				covered := domain.Lookup(id).State == schemaext.Complete

				c.Assert(covered, qt.Equals, provider.SchemaWithin(region, subject, "/"))
			})
		}
	}
}

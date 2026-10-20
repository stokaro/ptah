package catalog_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestProviderIdentityRequiresEveryBindingComponent(t *testing.T) {
	c := qt.New(t)
	valid := catalog.ProviderIdentity{ID: "example.provider", Version: "test", SemanticIdentity: "tables-1", ProtocolRevision: 2, ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64)}
	c.Assert(valid.Validate(), qt.IsNil)
	c.Assert(valid.IsZero(), qt.IsFalse)
	c.Assert((catalog.ProviderIdentity{}).IsZero(), qt.IsTrue)
	cases := []struct {
		name   string
		change func(*catalog.ProviderIdentity)
	}{
		{"ID", func(value *catalog.ProviderIdentity) { value.ID = "" }},
		{"version", func(value *catalog.ProviderIdentity) { value.Version = "" }},
		{"semantics", func(value *catalog.ProviderIdentity) { value.SemanticIdentity = "" }},
		{"whitespace", func(value *catalog.ProviderIdentity) { value.ID += " " }},
		{"protocol", func(value *catalog.ProviderIdentity) { value.ProtocolRevision = 0 }},
		{"artifact", func(value *catalog.ProviderIdentity) { value.ArtifactSHA256 = "" }},
		{"contract", func(value *catalog.ProviderIdentity) { value.ContractSHA256 = "" }},
		{"uppercase", func(value *catalog.ProviderIdentity) { value.ArtifactSHA256 = strings.Repeat("A", 64) }},
		{"invalid hex", func(value *catalog.ProviderIdentity) { value.ContractSHA256 = strings.Repeat("z", 64) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := valid
			test.change(&changed)
			c.Assert(changed.Validate(), qt.IsNotNil)
		})
	}
}

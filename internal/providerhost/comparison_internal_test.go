package providerhost

// White-box testing required: inspect transport projections that public comparison results
// intentionally do not expose, including omitted versus explicit scope parts.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func TestComparisonContextRetainsEstablishedFalseAndIdentifierFacts(t *testing.T) {
	c := qt.New(t)
	target := yamlIdentityTarget()
	service := modelComparer{target: target, declaration: &protocol.ModelComparison{Id: "compare"}}
	rules := identifier.Semantics{DefaultSchema: "main", CatalogCollation: "source collation", ResolvedNames: []identifier.ResolvedName{{Name: "Name", Key: "name"}}, TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonASCIIInsensitive, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceSchema}
	flag := capability.All()[0]
	request, err := service.request("fixture", rules, capability.Of(map[capability.Capability]bool{flag: false}), []schemaext.Kind{"example.org/fixture/value"})
	c.Assert(err, qt.IsNil)
	c.Assert(request.Capabilities, qt.DeepEquals, map[string]bool{string(flag): false})
	c.Assert(request.Identifiers.GetDefaultSchema(), qt.Equals, "main")
	c.Assert(request.Identifiers.GetCatalogCollation(), qt.Equals, "source collation")
	c.Assert(request.Identifiers.GetResolvedNames()[0].GetKey(), qt.Equals, "name")
	request.Identifiers.ResolvedNames[0].Key = "changed"
	c.Assert(rules.ResolvedNames[0].Key, qt.Equals, "name")
	rules.TableNames = identifier.ComparisonCatalogResolved
	_, err = service.request("fixture", rules, capability.Capabilities{}, nil)
	c.Assert(err, qt.IsNotNil)
}

func TestComparisonNamespaceRoundTripPreservesIndependentSources(t *testing.T) {
	c := qt.New(t)
	part := objectidentity.Part{Source: "Main", Normalized: "main", Quoted: true, Defaulted: true}
	scope := schemaext.NamespaceScope{Targets: []string{"fixture"}, SubjectKinds: []objectidentity.Kind{objectidentity.KindColumn}, Schema: &part, Parent: &part, Database: new(objectidentity.Part), PathExact: true, PathPrefix: objectidentity.NewParts(objectidentity.Part{Source: "a/b", Normalized: "a/b"}, objectidentity.Part{Source: "c", Normalized: "c"})}
	wire := comparisonNamespace(scope)
	c.Assert(wire.GetSchema(), qt.IsNotNil)
	c.Assert(wire.GetParent(), qt.IsNotNil)
	c.Assert(wire.GetCatalog(), qt.IsNil)
	c.Assert(wire.GetDatabase(), qt.IsNotNil)
	restored := boundNamespace(wire)
	c.Assert(restored, qt.DeepEquals, scope)
	wire.Schema.Source = "changed"
	c.Assert(scope.Schema.Source, qt.Equals, "Main")
	c.Assert(restored.Schema.Source, qt.Equals, "Main")
	source := &protocol.ComparisonKnowledgeSource{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	unknown := &protocol.ComparisonKnowledgeSource{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "Partial capture"}}
	domain, err := boundKnowledgeDomain([]*protocol.ComparisonKnowledgeSource{source, unknown})
	c.Assert(err, qt.IsNil)
	c.Assert(domain.Sources(), qt.HasLen, 2)
	subject := objectidentity.ID{Kind: objectidentity.KindColumn, Name: part, Parent: part}
	c.Assert(domain.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(boundIdentity(comparisonIdentity(subject)), qt.DeepEquals, subject)
}

func TestComparisonOwnershipChangesFrozenContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ModelComparison)
	}{
		{"service", func(d *protocol.ModelComparison) { d.Id = "different" }},
		{"placement", func(d *protocol.ModelComparison) { d.Placement = protocol.ModelComparison_FACETS }},
		{"models", func(d *protocol.ModelComparison) { d.Kinds[0] = "example.org/other/model" }},
		{"changes", func(d *protocol.ModelComparison) { d.ChangeKinds[0] = "example.org/other/change" }},
		{"owners", func(d *protocol.ModelComparison) { d.OwnerKinds = []string{"column"} }},
		{"actions", func(d *protocol.ModelComparison) { d.Actions = []string{"rotate"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := yamlIdentityTarget()
			target.engine.Comparisons = []*protocol.ModelComparison{{Id: "compare", Placement: protocol.ModelComparison_OBJECTS, Kinds: []string{"example.org/fixture/value"}, ChangeKinds: []string{"example.org/fixture/change"}}}
			contract := &modelContract{engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
			before, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			definition, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			test.mutate(target.engine.Comparisons[0])
			after, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			changed, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
			c.Assert(string(changed), qt.Not(qt.Equals), string(definition))
		})
	}
}

func TestComparisonContractIgnoresDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	first := &protocol.ModelComparison{Id: "objects", Kinds: []string{"b", "a"}, ChangeKinds: []string{"d", "c"}, Actions: []string{"z", "y"}}
	second := &protocol.ModelComparison{Id: "facets", OwnerKinds: []string{"table", "column"}}
	before := comparisonContracts([]*protocol.ModelComparison{first, second})
	reordered := proto.CloneOf(first)
	reordered.Kinds, reordered.ChangeKinds, reordered.Actions = []string{"a", "b"}, []string{"c", "d"}, []string{"y", "z"}
	c.Assert(comparisonContracts([]*protocol.ModelComparison{second, reordered}), qt.DeepEquals, before)
}

func TestResolvedRequirementsUseNormalizedIdentity(t *testing.T) {
	c := qt.New(t)
	contract := &modelContract{key: modelKey{kind: "example.org/fixture/value", representation: schemaext.Desired}, signature: "fixture"}
	dependency := &protocol.ComparisonIdentity{Kind: "extension", Name: &protocol.ComparisonPart{Source: "Example", Normalized: "example"}, Path: []*protocol.ComparisonPart{{Source: "a/b", Normalized: "a/b"}, {Source: "c", Normalized: "c"}}}
	value := &protocol.NormalizedValue{Value: &protocol.Envelope{Owner: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "records"}, Payload: []byte("{}")}, NormalizedOwner: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "records"}, CanonicalFormat: "fixture", CanonicalProjection: []byte("{}"), RequiredObjects: []*protocol.ComparisonIdentity{dependency}}
	original, err := contract.resolved(modelWire{Format: 1, Engine: "fixture"}, value)
	c.Assert(err, qt.IsNil)
	value.RequiredObjects[0].Name.Source = "example"
	value.RequiredObjects[0].Name.Quoted = true
	value.RequiredObjects[0].Name.Defaulted = true
	alias, err := contract.resolved(modelWire{Format: 1, Engine: "fixture"}, value)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Equal(alias), qt.IsTrue)
	originalRequired, err := original.Requires()
	c.Assert(err, qt.IsNil)
	aliasRequired, err := alias.Requires()
	c.Assert(err, qt.IsNil)
	c.Assert(originalRequired[0].Name.Source, qt.Equals, "Example")
	c.Assert(aliasRequired[0].Name.Source, qt.Equals, "example")
	value.RequiredObjects[0].Path = []*protocol.ComparisonPart{{Source: "a", Normalized: "a"}, {Source: "b/c", Normalized: "b/c"}}
	different, err := contract.resolved(modelWire{Format: 1, Engine: "fixture"}, value)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Equal(different), qt.IsFalse)
	value.RequiredObjects = nil
	absent, err := contract.resolved(modelWire{Format: 1, Engine: "fixture"}, value)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Equal(absent), qt.IsFalse)
}

func TestResolvedRequirementsRefusePendingDatabaseContext(t *testing.T) {
	c := qt.New(t)
	_, value := planningFixture(c)
	value.normalized.RequiresDatabaseContext = true
	requirements, err := value.Requires()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(requirements, qt.IsNil)
	value.normalized.RequiresDatabaseContext = false
	requirements, err = value.Requires()
	c.Assert(err, qt.IsNil)
	c.Assert(requirements, qt.HasLen, 0)
}

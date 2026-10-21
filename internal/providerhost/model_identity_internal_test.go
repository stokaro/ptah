package providerhost

// White-box testing required: the negotiated contract digest is assembled by
// private target state. A pure fixture isolates changes in that digest from
// database sessions and process artifact changes that also invalidate plans.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/platform/capability"
)

func modelIdentityTarget() *target {
	description := &protocol.Description{ProviderId: "example.org/fixture", Version: "1", SemanticIdentity: "semantics", Revision: provider.Revision}
	declared := &protocol.Engine{Id: "fixture", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}, ValueModels: []*protocol.ValueModel{{Kind: "example.org/fixture/value", Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: "definition", CanonicalFormat: "format", SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL}}}}
	return &target{host: &Host{description: description, artifact: "artifact"}, engine: declared}
}

func TestContractIdentityBindsEveryModelInterpretation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ValueModel)
	}{
		{"kind", func(m *protocol.ValueModel) { m.Kind = "example.org/other" }},
		{"representation", func(m *protocol.ValueModel) { m.Representation = protocol.ValueInput_OBSERVED }},
		{"version", func(m *protocol.ValueModel) { m.SchemaVersion++ }},
		{"encoding", func(m *protocol.ValueModel) { m.Encoding = "bytes" }},
		{"definition digest", func(m *protocol.ValueModel) { m.DefinitionSha256 = "other" }},
		{"definition", func(m *protocol.ValueModel) { m.DefinitionJson = []byte(`{"type":"string"}`) }},
		{"projection", func(m *protocol.ValueModel) { m.CanonicalFormat = "other" }},
		{"source formats", func(m *protocol.ValueModel) {
			m.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := modelIdentityTarget()
			before, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			test.mutate(target.engine.ValueModels[0])
			after, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
		})
	}
}

func TestContractIdentityDoesNotDependOnModelOrFormatOrder(t *testing.T) {
	c := qt.New(t)
	target := modelIdentityTarget()
	second := &protocol.ValueModel{Kind: "example.org/second", Representation: protocol.ValueInput_OBSERVED}
	target.engine.ValueModels = append(target.engine.ValueModels, second)
	before, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	target.engine.ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_HCL, protocol.SourceDocument_YAML}
	target.engine.ValueModels[0], target.engine.ValueModels[1] = target.engine.ValueModels[1], target.engine.ValueModels[0]
	after, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	c.Assert(after.ContractSHA256, qt.Equals, before.ContractSHA256)
}

func yamlIdentityTarget() *target {
	selected := modelIdentityTarget()
	selected.engine.YamlSourceKeys = []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_TABLE, Key: "retention", Selector: "retention", Models: []string{"example.org/fixture/value"}}}
	selected.host.targets = map[string]*target{"fixture": selected}
	selected.host.description.Engines = []*protocol.Engine{selected.engine}
	return selected
}

func TestContractAndModelDefinitionsBindYAMLGrammar(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.YAMLSourceKey)
	}{
		{"key", func(key *protocol.YAMLSourceKey) { key.Key = "different" }},
		{"scope", func(key *protocol.YAMLSourceKey) { key.Scope = protocol.YAMLSourceKey_DOCUMENT }},
		{"selector", func(key *protocol.YAMLSourceKey) { key.Selector = "different" }},
		{"models", func(key *protocol.YAMLSourceKey) { key.Models = []string{"example.org/fixture/other"} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := yamlIdentityTarget()
			contract := &modelContract{engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
			before, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			definition, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			test.change(target.engine.YamlSourceKeys[0])
			after, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			changedDefinition, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
			c.Assert(string(changedDefinition), qt.Not(qt.Equals), string(definition))
		})
	}
}

func TestYAMLGrammarIdentityIgnoresDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	target := yamlIdentityTarget()
	target.engine.YamlSourceKeys[0].Models = []string{"example.org/fixture/value", "example.org/fixture/other"}
	target.engine.YamlSourceKeys = append(target.engine.YamlSourceKeys, &protocol.YAMLSourceKey{
		Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "retention", Selector: "retention", Models: []string{"example.org/fixture/value"},
	})
	contract := &modelContract{engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
	before, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	definition, err := target.host.modelDefinition(contract)
	c.Assert(err, qt.IsNil)
	target.engine.YamlSourceKeys[0].Models = []string{"example.org/fixture/other", "example.org/fixture/value"}
	target.engine.YamlSourceKeys[0], target.engine.YamlSourceKeys[1] = target.engine.YamlSourceKeys[1], target.engine.YamlSourceKeys[0]
	after, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	reorderedDefinition, err := target.host.modelDefinition(contract)
	c.Assert(err, qt.IsNil)
	c.Assert(after.ContractSHA256, qt.Equals, before.ContractSHA256)
	c.Assert(string(reorderedDefinition), qt.Equals, string(definition))
}

func TestModelDisplayNameChangesFrozenDefinitions(t *testing.T) {
	c := qt.New(t)
	target := yamlIdentityTarget()
	contract := &modelContract{engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
	before, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	definition, err := target.host.modelDefinition(contract)
	c.Assert(err, qt.IsNil)
	target.engine.ValueModels[0].DisplayName = "retention values"
	after, err := target.identity(nil)
	c.Assert(err, qt.IsNil)
	changed, err := target.host.modelDefinition(contract)
	c.Assert(err, qt.IsNil)
	c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
	c.Assert(string(changed), qt.Not(qt.Equals), string(definition))
}

func TestModelsRefuseEngineDependentOmissionLabels(t *testing.T) {
	for _, label := range []string{"", "different values"} {
		t.Run(label, func(t *testing.T) {
			c := qt.New(t)
			target := yamlIdentityTarget()
			target.engine.ValueModels[0].DisplayName = "retention values"
			other := proto.CloneOf(target.engine)
			other.Id = "other"
			other.ValueModels[0].DisplayName = label
			target.host.description.Engines = append(target.host.description.Engines, other)
			c.Assert(target.host.initializeModels(), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSessionCapabilitiesPreserveEstablishedFalseAndRejectUnknownNames(t *testing.T) {
	c := qt.New(t)
	target := modelIdentityTarget()
	remote := &protocol.Session{Identifiers: proto.CloneOf(target.engine.Identifiers), Capabilities: map[string]bool{"transactional_ddl": false}}
	info, err := target.sessionInfo(remote)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Capabilities.Established(capability.TransactionalDDL), qt.IsTrue)
	c.Assert(info.Capabilities.Has(capability.TransactionalDDL), qt.IsFalse)
	c.Assert(info.Capabilities.Established(capability.ForeignKeys), qt.IsFalse)
	remote.Capabilities["transactional_ddl"] = true
	c.Assert(info.Capabilities.Has(capability.TransactionalDDL), qt.IsFalse)
	remote.Capabilities["unknown_capability"] = false
	_, err = target.sessionInfo(remote)
	c.Assert(err, qt.ErrorMatches, ".*provider session capabilities.*")
}

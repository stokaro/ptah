package providerhost

// White-box testing required: the negotiated contract digest is assembled by
// private target state. A pure fixture isolates changes in that digest from
// database sessions and process artifact changes that also invalidate plans.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func modelIdentityTarget() *target {
	description := &protocol.Description{ProviderId: "example.org/fixture", Version: "1", SemanticIdentity: "semantics", Revision: 5}
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

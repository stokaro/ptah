package provider_test

import (
	"crypto/sha256"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// ExampleValidateDescription_yamlKeys declares a table key explicitly. The
// model definition describes a value; it does not imply a frontend key name.
func ExampleValidateDescription_yamlKeys() {
	definition := []byte(`{"type":"object"}`)
	digest := sha256.Sum256(definition)
	model := &protocol.ValueModel{Kind: "example.org/engine/retention", Representation: protocol.ValueInput_DESIRED,
		SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: fmt.Sprintf("%x", digest),
		CanonicalFormat: "example.org/engine/retention-v1", SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}}
	key := &protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_TABLE, Key: "retention", Selector: "retention", Models: []string{model.Kind}}
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/engine", Version: "0.1", SemanticIdentity: "retention-v1",
		Features: []string{provider.NormalizeValuesFeature, provider.DecodeSourceFeature},
		Engines: []*protocol.Engine{{Id: "example", ValueModels: []*protocol.ValueModel{model}, YamlSourceKeys: []*protocol.YAMLSourceKey{key},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	if err := provider.ValidateDescription(description, nil); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s.%s declares %s\n", key.Scope, key.Key, key.Models[0])
	// Output: TABLE.retention declares example.org/engine/retention
}

package backend

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// This fixture definition includes both source representations. The digest
// binds their declared grammar and defaults; it is not a value fingerprint.
const retentionDefinition = `{"encoding":"json","kind":"example.org/fixture/retention","maximum_seconds":18446744073709551615,` +
	`"representations":{"desired":{"default_seconds":60},"observed":{"explicit_value_required":true}},` +
	`"source":{"formats":["yaml","hcl","go_annotations"],"selector":"retention","on":"binding ID; conflicts refused","unknown_fields":"refused"},` +
	`"schema_version":1,"units":["minutes","seconds"],"value":"one nonnegative integer when present; unknown or repeated fields refused"}`

func valueModels() []*protocol.ValueModel {
	definition, err := modeljson.Canonical([]byte(retentionDefinition))
	if err != nil {
		panic(err)
	} // Static owner definition; invalid JSON is a fixture programming error.
	digest := sha256.Sum256(definition)
	var models []*protocol.ValueModel
	for _, representation := range []protocol.ValueInput_Representation{protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED} {
		models = append(models, &protocol.ValueModel{Kind: "example.org/fixture/retention", Representation: representation,
			SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]),
			CanonicalFormat: "example.org/fixture/retention/seconds-json-v1"})
	}
	models[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS}
	return models
}

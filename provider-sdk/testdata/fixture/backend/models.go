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
		models = append(models, &protocol.ValueModel{DisplayName: "retention values", Kind: "example.org/fixture/retention", Representation: representation,
			SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]),
			CanonicalFormat: "example.org/fixture/retention/seconds-json-v1"})
	}
	models[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS}
	return models
}

func yamlSourceKeys() []*protocol.YAMLSourceKey {
	return []*protocol.YAMLSourceKey{
		{Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "retention", Selector: "retention", Models: []string{"example.org/fixture/retention"}},
		{Scope: protocol.YAMLSourceKey_TABLE, Key: "retention", Selector: "retention", Models: []string{"example.org/fixture/retention"}},
	}
}

// goSourceDirectives claims one Go annotation directive. The host lexes
// `//ptah:fixture:retention seconds=60` and sends the attributes as a mapping.
func goSourceDirectives() []*protocol.GoSourceDirective {
	return []*protocol.GoSourceDirective{
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention", Selector: "retention", Attributes: []string{"seconds", "minutes"}, Models: []string{"example.org/fixture/retention"}},
	}
}

// ScopedGoSourceDirectives claims the Go syntax the host reads beyond table
// scope. Column and index directives are written beside the field or index
// directive they describe, and the attributes of the frontend's own table,
// field and index directives belong to the provider. Every claim reads the same
// retention values, so the fixture shows where a value attaches and nothing
// else.
func ScopedGoSourceDirectives() []*protocol.GoSourceDirective {
	attributes := []string{"seconds", "minutes"}
	models := []string{"example.org/fixture/retention"}
	return []*protocol.GoSourceDirective{
		{Scope: protocol.GoSourceDirective_COLUMN, Directive: "ptah:fixture:column-retention", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:fixture:index-retention", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:table-retention", Selector: "retention", Attributes: []string{"seconds", "minutes", "table"}, Models: models},
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:schema:table", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_COLUMN, Directive: "ptah:schema:field", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "retention", Attributes: attributes, Models: models},
	}
}

// PlatformSourceClaims returns the claims of a provider that keeps a
// retention setting in the platform group of the common syntax, as
// platform.fixture.seconds on a Go directive and as the fixture group of the
// platform key in YAML, and that declares a setting of one column on the
// directive of its table. The value is the provider's, so the host must not
// keep a copy in the common model.
func PlatformSourceClaims() (yaml []*protocol.YAMLSourceKey, directives []*protocol.GoSourceDirective) {
	attributes := []string{"platform.fixture.seconds", "platform.fixture.minutes"}
	models := []string{"example.org/fixture/retention"}
	yaml = []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_TABLE, Key: "platform.fixture", Selector: "retention", Models: models}}
	directives = []*protocol.GoSourceDirective{
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:schema:table", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_COLUMN, Directive: "ptah:schema:field", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "retention", Attributes: attributes, Models: models},
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:placed", Selector: "retention", Attributes: []string{"seconds", "column", "of"}, Models: models},
	}
	return yaml, directives
}

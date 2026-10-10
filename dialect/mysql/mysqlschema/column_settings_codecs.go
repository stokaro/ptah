package mysqlschema

import (
	_ "embed" // Embed the definition that identifies the column settings wire format.
	"encoding/json"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed column-settings-codecs.json
var columnSettingsDefinition []byte

// ColumnSettingsWireDefinition returns an independent description of the
// column settings wire model. The definition covers both representations.
func ColumnSettingsWireDefinition() json.RawMessage { return slices.Clone(columnSettingsDefinition) }

var columnSettingsShape = schemaext.ObjectShape{
	Name: "MySQL column settings", Allowed: []string{"charset", "on_update"}, NonEmpty: []string{"charset", "on_update"},
}

// ColumnSettingsCodecs returns the version-one desired and observed column
// settings codecs, in that order. Each call returns independent definitions.
// A decoder accepts only the spelling the encoder writes: a key in another
// letter case, a null, an empty string and an unknown key are refused, each as
// a [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue]. The
// codecs validate representation invariants; they claim no server support.
func ColumnSettingsCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredColumnSettings]{
			Prototype: &DesiredColumnSettings{}, Representation: schemaext.Desired, Version: 1, Definition: ColumnSettingsWireDefinition(),
			Shape: decodeColumnSettingsShape, Validate: ValidateDesiredColumnSettings,
		}.Codec(),
		schemaext.ModelCodec[*ObservedColumnSettings]{
			Prototype: &ObservedColumnSettings{}, Representation: schemaext.Observed, Version: 1, Definition: ColumnSettingsWireDefinition(),
			Shape: decodeColumnSettingsShape, Validate: ValidateObservedColumnSettings,
		}.Codec(),
	}
}

func decodeColumnSettingsShape(data json.RawMessage) error {
	_, err := schemaext.DecodeObject(data, columnSettingsShape)
	return err
}

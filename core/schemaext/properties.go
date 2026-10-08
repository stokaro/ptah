package schemaext

import (
	"context"
	"maps"
)

// PropertyFormat identifies a source fragment grammar independently of the
// feature encoded in it. Frontends own formats; providers own their property
// names and semantics within a selected target and format.
type PropertyFormat string

// TablePlatformProperties is the string-valued platform property group on a
// table declaration. Go annotations and YAML share this grammar after parsing
// their respective quoting and container syntax.
const TablePlatformProperties PropertyFormat = "ptah.run/source/table-platform-properties"

// PropertyDefinition declares the keys one feature owns in a source format.
// Registration grants no meaning to missing keys, no source coverage, and no
// permission to consume a key owned by another feature.
type PropertyDefinition struct {
	Kind Kind
	Keys []string
}

// PropertyFragment is one feature's source properties. Map membership preserves
// explicit empty values. Kind identifies the selected decoder; fragments are
// not model envelopes and carry no catalog knowledge or model fingerprint.
type PropertyFragment struct {
	Kind       Kind
	Properties map[string]string
}

// Clone returns an independent fragment, including its property map.
func (f PropertyFragment) Clone() PropertyFragment {
	f.Properties = maps.Clone(f.Properties)
	return f
}

// PropertyDecodeRequest asks an owner to decode an ordered batch into desired
// model values. The caller groups only keys claimed by that owner's definition.
// Absence of a fragment does not authorize creation of feature intent.
type PropertyDecodeRequest struct {
	Target    string
	Format    PropertyFormat
	Fragments []PropertyFragment
}

// PropertyEncodeRequest asks an owner to export desired values to the selected
// property format. A value the format cannot preserve must be refused, never
// reduced to an incomplete property map.
type PropertyEncodeRequest struct {
	Target string
	Format PropertyFormat
	Values []Value
}

// PropertyService encodes and decodes feature-owned source fragments. Services
// preserve batch order and kinds, leave inputs untouched, and return no partial
// result on any error or cancellation. They perform no catalog inspection.
// A process adapter obeys the same request and response contract.
type PropertyService interface {
	DecodeProperties(context.Context, PropertyDecodeRequest) ([]Value, error)
	EncodeProperties(context.Context, PropertyEncodeRequest) ([]PropertyFragment, error)
}

// PropertyRuntime supplies source property ownership and semantic services from
// an explicit provider selection. Definitions are independent copies. Unknown
// targets, formats, or kinds are unavailable, never a built-in fallback.
type PropertyRuntime interface {
	PropertyService
	ModelRuntime
	PropertyDefinitions(string, PropertyFormat) ([]PropertyDefinition, error)
	PropertyFormats(string) ([]PropertyFormat, error)
}

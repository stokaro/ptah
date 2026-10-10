package schemaext

import "fmt"

// OwnedCoverage records one source's claim about a single kind: it enrolls the
// model definition that owner's codecs give kind in representation, with
// knowledge as the kind-wide claim and subjects as per-object overrides.
//
// The definition comes from codecs alone, never from a runtime registry, so a
// model a runtime adds later cannot widen a source description already
// captured. Codecs may define other kinds and representations; only the
// matching one is enrolled. Codecs that define no model of kind in
// representation are refused with [ErrInvalidValue], codecs [NewRegistry]
// refuses with its error, and the claim is validated as [NewCoverage]
// validates it, which refuses a representation other than [Desired] or
// [Observed].
func OwnedCoverage(owner string, codecs []Codec, kind Kind, representation Representation, knowledge Knowledge, subjects []SubjectCoverage) (Coverage, error) {
	owned := make([]OwnedCodec, 0, len(codecs))
	for _, codec := range codecs {
		owned = append(owned, OwnedCodec{Owner: owner, Codec: codec})
	}
	registry, err := NewRegistry(owned...)
	if err != nil {
		return Coverage{}, err
	}
	for _, definition := range registry.Definitions() {
		if definition.Kind == kind && definition.Representation == representation {
			return NewCoverage(representation, []KindCoverage{{Model: definition, Knowledge: knowledge}}, subjects)
		}
	}
	return Coverage{}, fmt.Errorf("%w: the codecs define no %s model of kind %q", ErrInvalidValue, representation, kind)
}

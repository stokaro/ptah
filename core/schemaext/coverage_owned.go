package schemaext

import (
	"fmt"
	"sync"
)

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
	registry, err := ownedRegistry(owner, codecs)
	if err != nil {
		return Coverage{}, err
	}
	return registryCoverage(registry, kind, representation, knowledge, subjects)
}

// OwnedCoverageSource returns a function that answers as [OwnedCoverage] does
// for owner and the codecs that codecs returns. The registry is built on the
// first call and reused by every later one: building it validates and hashes
// every codec definition, and a source asks for coverage on every read and
// every comparison. codecs is called once. A registry refusal is returned by
// every call.
func OwnedCoverageSource(owner string, codecs func() []Codec) func(kind Kind, representation Representation, knowledge Knowledge, subjects []SubjectCoverage) (Coverage, error) {
	registry := sync.OnceValues(func() (Registry, error) { return ownedRegistry(owner, codecs()) })
	return func(kind Kind, representation Representation, knowledge Knowledge, subjects []SubjectCoverage) (Coverage, error) {
		built, err := registry()
		if err != nil {
			return Coverage{}, err
		}
		return registryCoverage(built, kind, representation, knowledge, subjects)
	}
}

func ownedRegistry(owner string, codecs []Codec) (Registry, error) {
	owned := make([]OwnedCodec, 0, len(codecs))
	for _, codec := range codecs {
		owned = append(owned, OwnedCodec{Owner: owner, Codec: codec})
	}
	return NewRegistry(owned...)
}

func registryCoverage(registry Registry, kind Kind, representation Representation, knowledge Knowledge, subjects []SubjectCoverage) (Coverage, error) {
	for _, definition := range registry.Definitions() {
		if definition.Kind == kind && definition.Representation == representation {
			return NewCoverage(representation, []KindCoverage{{Model: definition, Knowledge: knowledge}}, subjects)
		}
	}
	return Coverage{}, fmt.Errorf("%w: the codecs define no %s model of kind %q", ErrInvalidValue, representation, kind)
}

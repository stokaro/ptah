package schemaext

import (
	"maps"
	"slices"
)

// Equal compares captured facet representations through their local value
// contracts. It does not resolve target defaults or call a semantic service.
func (f Facets) Equal(other Facets) bool {
	if len(f.values) != len(other.values) {
		return false
	}
	for kind, value := range f.values {
		compared, found := other.values[kind]
		if !found || !value.Equal(compared) {
			return false
		}
	}
	return true
}

// Equal compares captured object identities, source spellings, and local value
// representations. Target-aware migration comparison uses a selected service.
// Like [Facets.Equal], it ignores target bindings: a binding selects where a
// declaration applies, and projection removes it before values are compared.
func (o Objects) Equal(other Objects) bool {
	if len(o.values) != len(other.values) {
		return false
	}
	for key, object := range o.values {
		compared, found := other.values[key]
		if !found || object.Ref != compared.Ref || !object.Value.Equal(compared.Value) {
			return false
		}
	}
	return true
}

// Equal compares recorded model definitions, directions, and knowledge claims.
// Unenrolled and explicitly uninspected kinds remain distinct captured sources.
func (c Coverage) Equal(other Coverage) bool {
	return c.representation == other.representation && maps.Equal(c.kinds, other.kinds) &&
		maps.EqualFunc(c.subjects, other.subjects, func(a, b SubjectCoverage) bool {
			return a.Kind == b.Kind && a.Subject == b.Subject && a.Knowledge == b.Knowledge && slices.Equal(a.Targets, b.Targets)
		})
}

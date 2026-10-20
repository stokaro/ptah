package schemaext

import (
	"fmt"
	"slices"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
)

// NamespaceScope selects structured names on the listed targets. A nil
// qualifier matches every value; a present empty qualifier matches only an
// empty component. PathPrefix matches whole components, never string prefixes.
// Targets follow TargetSelection, and empty Targets applies to every target.
type NamespaceScope struct {
	Targets    []string             `json:"targets,omitempty"`
	Database   *objectidentity.Part `json:"database,omitempty"`
	Catalog    *objectidentity.Part `json:"catalog,omitempty"`
	Schema     *objectidentity.Part `json:"schema,omitempty"`
	PathPrefix objectidentity.Parts `json:"path_prefix,omitzero"`
}

// NamespaceKnowledge records knowledge inside one explicitly bounded namespace.
// It cannot assert absence or requested defaults for an entire namespace.
type NamespaceKnowledge struct {
	Scope     NamespaceScope `json:"scope"`
	Knowledge Knowledge      `json:"knowledge"`
}

func (s NamespaceScope) clone() NamespaceScope {
	s.Targets = slices.Clone(s.Targets)
	if s.Database != nil {
		s.Database = new(*s.Database)
	}
	if s.Catalog != nil {
		s.Catalog = new(*s.Catalog)
	}
	if s.Schema != nil {
		s.Schema = new(*s.Schema)
	}
	return s
}

func (s NamespaceScope) normalized() (NamespaceScope, error) {
	s = s.clone()
	targets, err := normalizeTargetScope(s.Targets)
	if err != nil {
		return NamespaceScope{}, err
	}
	s.Targets = targets
	if len(s.Targets) == 0 && s.Database == nil && s.Catalog == nil && s.Schema == nil && s.PathPrefix.IsZero() {
		return NamespaceScope{}, fmt.Errorf("%w: namespace scope has no boundary", ErrInvalidValue)
	}
	for _, part := range []*objectidentity.Part{s.Database, s.Catalog, s.Schema} {
		if part != nil && (!utf8.ValidString(part.Source) || !utf8.ValidString(part.Normalized) || (part.Source == "") != (part.Normalized == "")) {
			return NamespaceScope{}, fmt.Errorf("%w: namespace qualifier lacks its source or normalized form", ErrInvalidValue)
		}
	}
	for _, part := range s.PathPrefix.Values() {
		if part.Source == "" || part.Normalized == "" || !utf8.ValidString(part.Source) || !utf8.ValidString(part.Normalized) {
			return NamespaceScope{}, fmt.Errorf("%w: namespace path has an empty component", ErrInvalidValue)
		}
	}
	return s, nil
}

func (s NamespaceScope) matches(subject objectidentity.ID) bool {
	if !matchesQualifier(s.Database, subject.Database) || !matchesQualifier(s.Catalog, subject.Catalog) || !matchesQualifier(s.Schema, subject.Schema) {
		return false
	}
	prefix, path := s.PathPrefix.Values(), subject.Namespace.Values()
	if len(prefix) > len(path) {
		return false
	}
	for i, part := range prefix {
		if part.Normalized != path[i].Normalized {
			return false
		}
	}
	return true
}

func matchesQualifier(scope *objectidentity.Part, subject objectidentity.Part) bool {
	return scope == nil || scope.Normalized == subject.Normalized
}

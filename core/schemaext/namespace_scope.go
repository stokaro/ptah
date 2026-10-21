package schemaext

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
)

// NamespaceScope selects structured names on the listed targets. A nil
// qualifier matches every value; a present empty qualifier matches only an
// empty component. PathPrefix matches whole components, never string prefixes.
// Targets follow TargetSelection, and empty Targets applies to every target.
type NamespaceScope struct {
	// PathExact requires PathPrefix to name the entire namespace path.
	PathExact bool `json:"path_exact,omitempty"`
	// SubjectKinds limits subject types; an empty list accepts any kind.
	SubjectKinds []objectidentity.Kind `json:"subject_kinds,omitempty"`
	// Parent limits the owning object; nil accepts any parent.
	Parent     *objectidentity.Part `json:"parent,omitempty"`
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
	s.SubjectKinds = slices.Clone(s.SubjectKinds)
	if s.Parent != nil {
		s.Parent = new(*s.Parent)
	}
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

	if err := s.normalizeSubjectKinds(); err != nil {
		return NamespaceScope{}, err
	}
	s.Targets = targets
	if s.unbounded() {
		return NamespaceScope{}, fmt.Errorf("%w: namespace scope has no boundary", ErrInvalidValue)
	}
	for _, part := range []*objectidentity.Part{s.Database, s.Catalog, s.Schema, s.Parent} {
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
	if (len(s.SubjectKinds) > 0 && !slices.Contains(s.SubjectKinds, subject.Kind)) || !matchesQualifier(s.Parent, subject.Parent) ||
		!matchesQualifier(s.Database, subject.Database) || !matchesQualifier(s.Catalog, subject.Catalog) || !matchesQualifier(s.Schema, subject.Schema) {
		return false
	}
	prefix, path := s.PathPrefix.Values(), subject.Namespace.Values()
	if len(prefix) > len(path) || (s.PathExact && len(prefix) != len(path)) {
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

func (s NamespaceScope) normalizeSubjectKinds() error {
	slices.Sort(s.SubjectKinds)
	for i, kind := range s.SubjectKinds {
		if strings.TrimSpace(string(kind)) == "" || !utf8.ValidString(string(kind)) || (i > 0 && s.SubjectKinds[i-1] == kind) {
			return fmt.Errorf("%w: invalid or repeated namespace subject kind", ErrInvalidValue)
		}
	}
	return nil
}

func (s NamespaceScope) unbounded() bool {
	return len(s.Targets) == 0 && s.Database == nil && s.Catalog == nil && s.Schema == nil && s.PathPrefix.IsZero() && !s.PathExact && s.Parent == nil && len(s.SubjectKinds) == 0
}

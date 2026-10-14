package descriptor

import (
	"fmt"
	"maps"
	"strings"
)

// Registry is an immutable set of validated descriptors, kept in the order
// they were added. It is safe for concurrent use. A nil *Registry is an empty
// registry: every lookup misses and every listing is empty.
type Registry struct {
	dialects   []Dialect
	bySpelling map[string]int
}

// NewRegistry validates dialects and returns them as a registry, in the given
// order. It refuses a malformed descriptor with [ErrInvalidDescriptor] and a
// spelling two different descriptors claim with [ErrConflict]. A descriptor
// given twice, equal both times, is kept once.
func NewRegistry(dialects ...Dialect) (*Registry, error) {
	var empty *Registry
	return empty.With(dialects...)
}

// With returns a new registry holding r's descriptors followed by dialects;
// r itself does not change. The checks are those of [NewRegistry], across the
// whole result: a descriptor equal to one r holds is kept once, and one that
// claims a spelling r gives another meaning is refused. So a spelling r
// answers keeps its answer in every registry With returns.
func (r *Registry) With(dialects ...Dialect) (*Registry, error) {
	next := &Registry{bySpelling: make(map[string]int)}
	if r != nil {
		next.dialects = make([]Dialect, len(r.dialects), len(r.dialects)+len(dialects))
		copy(next.dialects, r.dialects)
		maps.Copy(next.bySpelling, r.bySpelling)
	}
	for _, dialect := range dialects {
		if err := dialect.validate(); err != nil {
			return nil, err
		}
		if index, found := next.bySpelling[dialect.Name]; found && next.dialects[index].Name == dialect.Name {
			if next.dialects[index].Equal(dialect) {
				continue
			}
			return nil, fmt.Errorf("%w: dialect %q is already described differently", ErrConflict, dialect.Name)
		}
		for _, spelling := range dialect.Spellings() {
			if index, found := next.bySpelling[spelling]; found {
				return nil, fmt.Errorf("%w: dialect %q claims %q, which dialect %q already owns",
					ErrConflict, dialect.Name, spelling, next.dialects[index].Name)
			}
		}
		for _, spelling := range dialect.Spellings() {
			next.bySpelling[spelling] = len(next.dialects)
		}
		next.dialects = append(next.dialects, dialect.Clone())
	}
	if err := next.validateFamilies(); err != nil {
		return nil, err
	}
	return next, nil
}

// validateFamilies checks that every family a descriptor names is the name of
// a descriptor in the registry that heads it, by naming itself.
func (r *Registry) validateFamilies() error {
	for _, dialect := range r.dialects {
		if dialect.Family == "" {
			continue
		}
		index, found := r.bySpelling[dialect.Family]
		if !found || r.dialects[index].Name != dialect.Family {
			return fmt.Errorf("%w: dialect %q names family %q, which is no dialect's name", ErrInvalidDescriptor, dialect.Name, dialect.Family)
		}
		if head := r.dialects[index]; head.Family != head.Name {
			return fmt.Errorf("%w: dialect %q names family %q, whose own family is %q rather than itself",
				ErrInvalidDescriptor, dialect.Name, dialect.Family, head.Family)
		}
	}
	return nil
}

// Normalize folds a spelling onto the canonical name of the dialect that
// owns it, ignoring surrounding space and letter case, and returns "" for a
// spelling no descriptor claims. Check the empty answer: it is not a dialect.
func (r *Registry) Normalize(spelling string) string {
	index, found := r.index(spelling)
	if !found {
		return ""
	}
	return r.dialects[index].Name
}

// Lookup returns a copy of the descriptor that owns spelling, folded as
// [Registry.Normalize] folds it.
func (r *Registry) Lookup(spelling string) (Dialect, bool) {
	index, found := r.index(spelling)
	if !found {
		return Dialect{}, false
	}
	return r.dialects[index].Clone(), true
}

// Dialects returns a copy of every descriptor, in the order they were added.
func (r *Registry) Dialects() []Dialect {
	if r == nil {
		return nil
	}
	dialects := make([]Dialect, len(r.dialects))
	for i, dialect := range r.dialects {
		dialects[i] = dialect.Clone()
	}
	return dialects
}

// Spellings returns every name and alias, each dialect's name before its
// aliases, in the order the dialects were added. Each call returns a new
// slice.
func (r *Registry) Spellings() []string {
	if r == nil {
		return nil
	}
	spellings := make([]string, 0, len(r.bySpelling))
	for _, dialect := range r.dialects {
		spellings = append(spellings, dialect.Spellings()...)
	}
	return spellings
}

func (r *Registry) index(spelling string) (int, bool) {
	if r == nil {
		return 0, false
	}
	index, found := r.bySpelling[strings.ToLower(strings.TrimSpace(spelling))]
	return index, found
}

package schemaext

import (
	"fmt"
	"maps"

	"ptah.run/core/objectidentity"
)

// SelectSubjects retains the source representation and kind-wide knowledge,
// selecting explicit subject and parent-namespace records by identity. Use it
// with the same projection of schema objects: removing a subject override alone
// makes Lookup fall back to the kind-wide knowledge. A nil predicate retains
// every record. The source and result are immutable and share no writable state.
func (c Coverage) SelectSubjects(keep func(objectidentity.ID) bool) Coverage {
	if keep == nil {
		return c
	}
	result := Coverage{representation: c.representation, kinds: c.kinds, subjects: make(map[subjectKey]SubjectCoverage)}
	for key, record := range c.subjects {
		if keep(record.Subject) {
			result.subjects[key] = record
		}
	}
	return result
}

// SelectKinds captures only the named model kinds, including their subject and
// parent-namespace overrides. Unenrolled kinds remain unenrolled.
func (c Coverage) SelectKinds(kinds []Kind) Coverage {
	result := Coverage{representation: c.representation, kinds: make(map[Kind]KindCoverage), subjects: make(map[subjectKey]SubjectCoverage)}
	for _, kind := range kinds {
		if record, found := c.kinds[kind]; found {
			result.kinds[kind] = record
		}
	}
	for key, record := range c.subjects {
		if _, found := result.kinds[key.kind]; found {
			result.subjects[key] = record
		}
	}
	return result
}

// Combine joins disjoint model ownership after service dispatch. It refuses
// duplicate kinds. Unlike Merge, it combines partitions of one source, not
// independent sources whose knowledge must be intersected.
func (c Coverage) Combine(other Coverage) (Coverage, error) {
	direction := c.representation
	if direction == "" {
		direction = other.representation
	}
	if other.representation != "" && other.representation != direction {
		return Coverage{}, fmt.Errorf("%w: coverage directions differ", ErrInvalidValue)
	}
	result := Coverage{representation: direction, kinds: make(map[Kind]KindCoverage), subjects: make(map[subjectKey]SubjectCoverage)}
	maps.Copy(result.kinds, c.kinds)
	maps.Copy(result.subjects, c.subjects)
	for kind, record := range other.kinds {
		if _, found := result.kinds[kind]; found {
			return Coverage{}, fmt.Errorf("%w: combined coverage kind %q", ErrDuplicate, kind)
		}
		result.kinds[kind] = record
	}
	maps.Copy(result.subjects, other.subjects)
	return result, nil
}

// ForParent captures all model definitions and only this table's overrides.
// Kind-wide unknown claims survive even when the table has no named children.
// The result also carries facet claims whose subject is the table itself.
func (c Coverage) ForParent(parent objectidentity.ID) Coverage {
	result := Coverage{representation: c.representation, kinds: c.kinds, subjects: make(map[subjectKey]SubjectCoverage)}
	for key, record := range c.subjects {
		ref := record.Subject
		if ref.Key() == parent.Key() || (ref.Catalog.Normalized == parent.Catalog.Normalized &&
			ref.Schema.Normalized == parent.Schema.Normalized && ref.Parent.Normalized == parent.Name.Normalized && ref.Parent.Normalized != "") {
			result.subjects[key] = record
		}
	}
	return result
}

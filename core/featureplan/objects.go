package featureplan

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

// ObjectContext preserves named feature objects and their independent coverage
// through planning. ChangedSubjects is the selected runtime's ordered summary
// of accepted changes to those objects; it grants no additional write ownership.
type ObjectContext struct {
	Desired, Current schemaext.ObjectState
	ChangedSubjects  []objectidentity.ID
}

// Subjects returns the exact union assigned to kinds, in structured identity
// order. A desired spelling wins when both sides describe the same identity.
// Kind-level coverage describes knowledge, not an invented object identity.
func (c ObjectContext) Subjects(kinds []schemaext.Kind) []objectidentity.ID {
	return c.subjects(func(kind schemaext.Kind) bool { return slices.Contains(kinds, kind) })
}

// AllSubjects returns the complete structured union, including subject coverage,
// in the same order as Subjects. Diagnostic indexes refer to this union.
func (c ObjectContext) AllSubjects() []objectidentity.ID {
	return c.subjects(func(schemaext.Kind) bool { return true })
}

func (c ObjectContext) subjects(assigned func(schemaext.Kind) bool) []objectidentity.ID {
	union := make(map[objectidentity.Key]objectidentity.ID)
	for _, state := range []schemaext.ObjectState{c.Current, c.Desired} {
		for _, record := range state.Coverage.SubjectRecords() {
			if assigned(record.Kind) && record.Subject.Kind == objectidentity.Kind(record.Kind) {
				union[record.Subject.Key()] = record.Subject
			}
		}
		for _, ref := range state.Objects.Refs() {
			if assigned(schemaext.Kind(ref.Kind)) {
				union[ref.Key()] = ref
			}
		}
	}
	result := make([]objectidentity.ID, 0, len(union))
	for _, subject := range union {
		result = append(result, subject)
	}
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

// ObjectPlan accounts for one assigned native object, whether its declaration
// changes or its dependencies require lifecycle operations. An empty Steps
// list explicitly reports that no operation is needed and still needs Strategy.
type ObjectPlan struct {
	Subject  objectidentity.ID
	Kind     schemaext.Kind
	Strategy string
	Steps    []plangraph.StepID
}

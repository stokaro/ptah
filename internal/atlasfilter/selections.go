package atlasfilter

import (
	"context"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// vocabulary is what a selector may name in one run: the common types and
// fields this package knows, and the owners' selector types and the fields
// an exclusion may subtract from their objects.
type vocabulary struct {
	selections selectext.Set
}

// includeSelectable reports whether an --include selector may name a type: a
// common top-level type, or an owner's.
func (v vocabulary) includeSelectable() map[string]struct{} {
	selectable := maps.Clone(includeSelectableTypes)
	for _, selection := range v.selections.Selections() {
		selectable[selection.Type] = struct{}{}
	}
	return selectable
}

// fields returns the fields an exclusion may subtract from an object of a
// type: a common type's, or the fields an owner's selection names.
func (v vocabulary) fields(resourceType string) []string {
	if fields, common := excludeFieldSelectors[resourceType]; common {
		return slices.Sorted(maps.Keys(fields))
	}
	if selection, owned := v.selections.Selection(resourceType); owned {
		return slices.Clone(selection.Fields)
	}
	return nil
}

// namedKinds are the owners' kinds a scope selects by their own type and
// name. The tables one of them binds decide only whether a scope splits it;
// see [featureselect.Bindings.Select].
func (v vocabulary) namedKinds() []schemaext.Kind {
	selections := v.selections.Selections()
	kinds := make([]schemaext.Kind, 0, len(selections))
	for _, selection := range selections {
		kinds = append(kinds, selection.Kind)
	}
	return kinds
}

// ownerContext is the context the owners compute a scope in. The filters
// take no context yet; the owners compute in process.
func ownerContext() context.Context {
	return context.Background()
}

// filterOwnerFeatures drops the owners' objects that an exclusion selector
// names under their type, and those whose schema is excluded, on both sides
// of a comparison. An object whose name is a relation's excludes a table of
// its name with it. A database-wide name is matched alone, and no schema
// exclusion reaches it.
func (s *exclusionState) filterOwnerFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	for _, selection := range s.vocabulary.selections.Selections() {
		objects, coverage = selectNamedFeatures(objects, coverage, selection.Kind, func(schema, name string) bool {
			if selection.Naming == selectext.DatabaseWide {
				return !s.matches(selection.Type, name)
			}
			if s.matches(selection.Type, s.nameCandidates(schema, name)...) || s.schemaExcluded(schema) {
				if selection.Relation {
					s.excludeTable(schema, name)
				}
				return false
			}
			return true
		})
	}
	return objects, coverage
}

// clearOwnerFields subtracts the fields an exclusion's field selectors name
// from the owners' objects it kept, through one call to the owners.
func (s *exclusionState) clearOwnerFields(objects schemaext.Objects) (schemaext.Objects, error) {
	var clears []selectext.Clear
	for _, selection := range s.vocabulary.selections.Selections() {
		if len(selection.Fields) == 0 {
			continue
		}
		for _, ref := range objects.Refs() {
			if ref.Kind != objectidentity.Kind(selection.Kind) {
				continue
			}
			names := []string{ref.Name.Source}
			if selection.Naming == selectext.InSchema {
				names = s.nameCandidates(ref.Schema.Authored(), ref.Name.Source)
			}
			for _, field := range selection.Fields {
				if s.matchesField(selection.Type, field, names...) {
					clears = append(clears, selectext.Clear{Object: ref, Field: field})
				}
			}
		}
	}
	if len(clears) == 0 {
		return objects, nil
	}
	result, err := s.vocabulary.selections.SelectObjects(ownerContext(), selectext.Request{Kept: objects, Clears: clears})
	if err != nil {
		return schemaext.Objects{}, err
	}
	for _, object := range result.Cleared {
		if objects, err = objects.Replace(object); err != nil {
			return schemaext.Objects{}, err
		}
	}
	return objects, nil
}

// selectOwnerFeatures keeps the owners' objects that the scope's selectors
// name under their type, in the schema universe, and the objects the kept
// ones require, which the owners name in one call: a resource pool a kept
// classifier routes to. It keeps as well an owner's type a kept column
// names, as the common types are kept; referenced holds the kept columns'
// type names. A required or referenced object comes back only when the side
// holds it, with its knowledge.
func (s *scopeSelection) selectOwnerFeatures(
	objects schemaext.Objects, coverage schemaext.Coverage, referenced map[string]struct{},
) (schemaext.Objects, schemaext.Coverage, error) {
	originalObjects, originalCoverage := objects, coverage
	selections := s.vocabulary.selections.Selections()
	for _, selection := range selections {
		objects, coverage = selectNamedFeatures(objects, coverage, selection.Kind, func(schema, name string) bool {
			if selection.Naming == selectext.DatabaseWide {
				return s.selectedNames(typeList(selection.Type), name)
			}
			return s.selected(typeList(selection.Type), schema, name)
		})
	}
	if len(selections) == 0 {
		return objects, coverage, nil
	}
	result, err := s.vocabulary.selections.SelectObjects(ownerContext(), selectext.Request{Kept: objects})
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	required, err := referencedTypes(originalObjects, referenced)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	for _, ref := range result.Requires {
		required[ref.Key()] = true
	}
	if len(required) == 0 {
		return objects, coverage, nil
	}
	kept := make(map[objectidentity.Key]bool)
	for _, ref := range objects.Refs() {
		kept[ref.Key()] = true
	}
	for _, record := range coverage.SubjectRecords() {
		kept[record.Subject.Key()] = true
	}
	owned := make(map[objectidentity.Kind]bool, len(selections))
	for _, selection := range selections {
		owned[objectidentity.Kind(selection.Kind)] = true
	}
	keep := func(ref objectidentity.ID) bool {
		return !owned[ref.Kind] || kept[ref.Key()] || required[ref.Key()]
	}
	return originalObjects.Select(keep), originalCoverage.SelectSubjects(keep), nil
}

// selectNamedFeatures keeps the objects of kind keepNode keeps, with their
// knowledge, and every object of another kind. Table selection does not
// remove them. keepNode receives the schema the source wrote, so an unqualified object is
// resolved under the filter's own default schema rather than under the one
// its identity was built with.
func selectNamedFeatures(objects schemaext.Objects, coverage schemaext.Coverage, kind schemaext.Kind, keepNode func(schema, name string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(kind) || keepNode(ref.Schema.Authored(), ref.Name.Source)
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}

// referencedTypes names the objects that define a type a kept column names,
// read through [schemaext.TypeDefiner] so the filter imports no owner.
func referencedTypes(objects schemaext.Objects, referenced map[string]struct{}) (map[objectidentity.Key]bool, error) {
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	keys := make(map[objectidentity.Key]bool)
	for _, object := range all {
		definer, ok := object.Value.(schemaext.TypeDefiner)
		if !ok {
			continue
		}
		defined := definer.DefinedType(object.Ref)
		if typeNameReferenced(referenced, defined.ID.Schema.Authored(), defined.ID.Name.Source) {
			keys[object.Ref.Key()] = true
		}
	}
	return keys, nil
}

// definedTypeLabels name a common type kind the way a scope's refusal does.
var definedTypeLabels = map[objectidentity.Kind]string{
	objectidentity.KindDomain:    "domain",
	objectidentity.KindComposite: "composite type",
	objectidentity.KindRange:     "range type",
}

// reportDroppedOwnerTypes asks report about every owner's type original
// defines that final no longer holds, under the label of its common kind.
func reportDroppedOwnerTypes(original, final schemaext.Objects, report func(kind, schema, name string)) {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range final.Refs() {
		held[ref.Key()] = true
	}
	all, err := original.All()
	if err != nil {
		return
	}
	for _, object := range all {
		definer, ok := object.Value.(schemaext.TypeDefiner)
		if !ok || held[object.Ref.Key()] {
			continue
		}
		defined := definer.DefinedType(object.Ref)
		if label, known := definedTypeLabels[defined.ID.Kind]; known {
			report(label, defined.ID.Schema.Authored(), defined.ID.Name.Source)
		}
	}
}

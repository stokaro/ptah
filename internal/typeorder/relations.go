package typeorder

import (
	"cmp"
	"maps"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
)

// A PostgreSQL table, view or materialized view defines a row type, and a
// domain's base type, a composite type's attribute or a range type's subtype
// may name it: `CREATE TYPE wrap AS (r t, n int)`. The server records the
// dependency, so such a type is created after the relation and dropped before
// it. The functions below find these types among the steps of one plan, by
// the common steps that create and drop relations, so their owner can place
// them on the right side of those steps.

// RelationStep is a common step that creates or drops a relation, with the
// relation it names.
type RelationStep struct {
	Step     plangraph.StepID
	Relation objectidentity.ID
}

// Reads returns a read of each type of definitions that a spelling names,
// once each, in the order of the spellings. A spelling names a type as a
// column, a routine's signature or another type writes it, array dimensions
// and type modifiers included, and resolves as type references between
// definitions do. A type declared without a schema is also named by its
// default schema's spelling, `public.wrap` for `wrap`. A host gives a
// statement that uses a type these reads, so an owner can tell which common
// steps hold one of its types.
func Reads(spellings []string, definitions []schemaext.DefinedType) []plangraph.Effect {
	if len(spellings) == 0 || len(definitions) == 0 {
		return nil
	}
	types := make([]deporder.UserType, 0, len(definitions))
	byName := make(map[string]objectidentity.ID, len(definitions))
	for _, definition := range definitions {
		names := []string{Name(definition.ID)}
		if definition.ID.Schema.Defaulted && definition.ID.Schema.Source != "" {
			names = append(names, tableref.CanonicalExact(definition.ID.Schema.Source, definition.ID.Name.Source))
		}
		for _, name := range names {
			types = append(types, deporder.UserType{Name: name})
			byName[name] = definition.ID
		}
	}
	var reads []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, spelled := range spellings {
		name, ok := deporder.ResolveUserTypeReference(types, spelled)
		if !ok {
			continue
		}
		id := byName[name]
		if !seen[id.Key()] {
			seen[id.Key()] = true
			reads = append(reads, plangraph.Effect{Subject: id, Action: plangraph.Read})
		}
	}
	return reads
}

// CreatedAfterRelations returns each type of types whose definition names the
// row type of a relation a common step creates, directly or through the
// other types it names, with the steps that create those relations. The keys
// are type names as [deporder.NormalizeTypeReference] spells them, and a
// type's steps are sorted by step. Such a type cannot be created before those
// steps. Names resolve as semantics says, as [deporder.NamesRowType] resolves
// them.
func CreatedAfterRelations(types []deporder.UserType, steps []featureplan.CommonStep, semantics identifier.Semantics) map[string][]RelationStep {
	return reachedRelations(types, relationSteps(steps, plangraph.Create, objectidentity.KindTable, objectidentity.KindView, objectidentity.KindMatView), semantics)
}

// DroppedBeforeTables returns each type of removed whose definition, as types
// holds it now, names the row type of a table a common step drops, directly or
// through the other types it names, with the steps that drop those tables.
// The keys and the order are those of [CreatedAfterRelations].
//
// A type is left out while a common step that drops an object reads it, such
// as the DROP TABLE of a table whose column the type is, or a DROP FUNCTION
// whose signature names it: the host drops such an object after its tables,
// and the type has to outlive it. So is every type a type left out names,
// since that type is dropped first. A type that names a view's row type is
// not one either: the host removes views before the window a type dropped
// ahead of its tables joins. A type left out is dropped after the tables, and
// a table whose row type it names relies on CASCADE.
func DroppedBeforeTables(types []deporder.UserType, removed []string, steps []featureplan.CommonStep, semantics identifier.Semantics) map[string][]RelationStep {
	reached := reachedRelations(types, relationSteps(steps, plangraph.Drop, objectidentity.KindTable), semantics)
	isRemoved := make(map[string]bool, len(removed))
	for _, name := range removed {
		isRemoved[deporder.NormalizeTypeReference(name)] = true
	}
	held := make(map[string]bool)
	for name := range ReadBy(steps, func(step featureplan.CommonStep) bool {
		return slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool { return effect.Action == plangraph.Drop })
	}) {
		held[name] = true
	}
	references := normalizedReferences(types)
	for _, name := range slices.Sorted(maps.Keys(held)) {
		for _, ref := range reachableTypes(name, references) {
			held[ref] = true
		}
	}
	result := make(map[string][]RelationStep)
	for name, relations := range reached {
		if isRemoved[name] && !held[name] {
			result[name] = relations
		}
	}
	return result
}

// ReadBy returns, for each type a common step that keep accepts reads, the
// steps that read it, by the names [CreatedAfterRelations] keys types by.
func ReadBy(steps []featureplan.CommonStep, keep func(featureplan.CommonStep) bool) map[string][]featureplan.CommonStep {
	readers := make(map[string][]featureplan.CommonStep)
	for _, step := range steps {
		if !keep(step) {
			continue
		}
		for _, effect := range step.Effects {
			if effect.Action == plangraph.Read && isType(effect.Subject.Kind) {
				name := deporder.NormalizeTypeReference(Name(effect.Subject))
				readers[name] = append(readers[name], step)
			}
		}
	}
	return readers
}

// relationSteps lists the steps that act on a relation of one of kinds.
func relationSteps(steps []featureplan.CommonStep, action plangraph.Action, kinds ...objectidentity.Kind) []RelationStep {
	var result []RelationStep
	for _, step := range steps {
		for _, effect := range step.Effects {
			if effect.Action == action && slices.Contains(kinds, effect.Subject.Kind) {
				result = append(result, RelationStep{Step: step.ID, Relation: effect.Subject})
			}
		}
	}
	return result
}

// reachedRelations maps each type that names the row type of one of
// relations, directly or through the types it names, to those relations.
func reachedRelations(types []deporder.UserType, relations []RelationStep, semantics identifier.Semantics) map[string][]RelationStep {
	if len(relations) == 0 {
		return nil
	}
	direct := make(map[string][]RelationStep)
	for _, typ := range types {
		name := deporder.NormalizeTypeReference(typ.Name)
		for _, relation := range relations {
			if slices.ContainsFunc(typ.References, func(spelled string) bool {
				return deporder.NamesRowType(spelled, Name(relation.Relation), semantics)
			}) {
				direct[name] = append(direct[name], relation)
			}
		}
	}
	references := normalizedReferences(types)
	result := make(map[string][]RelationStep)
	for _, typ := range types {
		name := deporder.NormalizeTypeReference(typ.Name)
		reached := slices.Clone(direct[name])
		for _, ref := range reachableTypes(name, references) {
			reached = append(reached, direct[ref]...)
		}
		if len(reached) == 0 {
			continue
		}
		slices.SortFunc(reached, func(a, b RelationStep) int {
			return cmp.Or(cmp.Compare(a.Step.Owner, b.Step.Owner), cmp.Compare(a.Step.Name, b.Step.Name), schemaext.CompareRefs(a.Relation, b.Relation))
		})
		result[name] = slices.CompactFunc(reached, func(a, b RelationStep) bool {
			return a.Step == b.Step && a.Relation.Key() == b.Relation.Key()
		})
	}
	return result
}

// normalizedReferences is the types each type names, every name spelled as
// [deporder.NormalizeTypeReference] spells it.
func normalizedReferences(types []deporder.UserType) map[string][]string {
	result := make(map[string][]string)
	for name, refs := range deporder.UserTypeDependencies(types) {
		key := deporder.NormalizeTypeReference(name)
		for _, ref := range refs {
			result[key] = append(result[key], deporder.NormalizeTypeReference(ref))
		}
	}
	return result
}

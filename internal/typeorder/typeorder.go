// Package typeorder orders data type operations across planning owners.
// It reads neutral type definitions and step effects, never owner payloads.
package typeorder

import (
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
)

// Name spells a defined type without losing quoted identifier components.
func Name(id objectidentity.ID) string {
	return tableref.CanonicalExact(id.Schema.Authored(), id.Name.Source)
}

// Vocabulary joins the selected operations to all known definitions. The
// selected operands take precedence, including for callers that build a diff
// without a full schema. Types outside the selected operations still carry
// dependency paths between them.
func Vocabulary(selected []deporder.UserType, definitions []schemaext.DefinedType) []deporder.UserType {
	result := slices.Clone(selected)
	seen := make(map[string]bool, len(selected))
	for _, typ := range selected {
		seen[deporder.NormalizeTypeReference(typ.Name)] = true
	}
	for _, typ := range definitions {
		name := Name(typ.ID)
		key := deporder.NormalizeTypeReference(name)
		if !seen[key] {
			result = append(result, deporder.UserType{Name: name, References: slices.Clone(typ.References)})
			seen[key] = true
		}
	}
	return result
}

// Dependencies orders creation by desired definitions and removal by current
// definitions. Common and owned steps share the same identity vocabulary.
// References through a type without an operation retain their ordering path.
func Dependencies(current, desired []schemaext.DefinedType, steps []featureplan.CommonStep) []plangraph.Dependency {
	return slices.Concat(dependencies(desired, steps, plangraph.Create), dependencies(current, steps, plangraph.Drop))
}

func dependencies(definitions []schemaext.DefinedType, steps []featureplan.CommonStep, action plangraph.Action) []plangraph.Dependency {
	types := Vocabulary(nil, definitions)
	refs := deporder.UserTypeDependencies(types)
	byName := typeStepNames(definitions, steps, action)
	var result []plangraph.Dependency
	for _, typ := range types {
		for _, dependent := range byName[deporder.NormalizeTypeReference(typ.Name)] {
			for _, ref := range reachableTypes(typ.Name, refs) {
				for _, prerequisite := range byName[deporder.NormalizeTypeReference(ref)] {
					if prerequisite == dependent {
						continue
					}
					edge := plangraph.Dependency{Before: prerequisite, After: dependent}
					if action == plangraph.Drop {
						edge.Before, edge.After = edge.After, edge.Before
					}
					result = append(result, edge)
				}
			}
		}
	}
	return result
}

func typeStepNames(definitions []schemaext.DefinedType, steps []featureplan.CommonStep, action plangraph.Action) map[string][]plangraph.StepID {
	byID := make(map[objectidentity.Key][]plangraph.StepID)
	for _, step := range steps {
		for _, effect := range step.Effects {
			if effect.Action == action && isType(effect.Subject.Kind) {
				key := effect.Subject.Key()
				byID[key] = append(byID[key], step.ID)
			}
		}
	}
	byName := make(map[string][]plangraph.StepID)
	for _, definition := range definitions {
		byName[deporder.NormalizeTypeReference(Name(definition.ID))] = byID[definition.ID.Key()]
	}
	return byName
}

func reachableTypes(name string, references map[string][]string) []string {
	seen := map[string]bool{name: true}
	var result []string
	var visit func(string)
	visit = func(name string) {
		for _, ref := range references[name] {
			if seen[ref] {
				continue
			}
			seen[ref] = true
			result = append(result, ref)
			visit(ref)
		}
	}
	visit(name)
	return result
}

func isType(kind objectidentity.Kind) bool {
	return kind == objectidentity.KindDomain || kind == objectidentity.KindComposite || kind == objectidentity.KindRange
}

// RebuildNames closes replacement requirements over changed types. A changed
// type that still references a replaced type must also be replaced. Unchanged
// dependents remain in place so a non-CASCADE drop can refuse their loss.
func RebuildNames(definitions []deporder.UserType, changed, replacements []string) map[string]struct{} {
	rebuilt := make(map[string]struct{}, len(replacements))
	for _, name := range replacements {
		rebuilt[deporder.NormalizeTypeReference(name)] = struct{}{}
	}
	candidates := make(map[string]bool, len(changed))
	for _, name := range changed {
		candidates[deporder.NormalizeTypeReference(name)] = true
	}
	references := deporder.UserTypeDependencies(definitions)
	for grew := true; grew; {
		grew = false
		for name, refs := range references {
			key := deporder.NormalizeTypeReference(name)
			if _, found := rebuilt[key]; found || !candidates[key] {
				continue
			}
			for _, ref := range refs {
				if _, found := rebuilt[deporder.NormalizeTypeReference(ref)]; found {
					rebuilt[key], grew = struct{}{}, true
					break
				}
			}
		}
	}
	return rebuilt
}

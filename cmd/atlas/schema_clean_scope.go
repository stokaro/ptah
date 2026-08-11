package atlas

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/schemaclean"
)

// scopeAtlasSchemaCleanPlan restricts a cleanup plan to the objects selected by
// --include and --exclude. Every writer-owned top-level kind is offered to the
// shared Atlas matcher directly, so widening cleanup inventory cannot make
// full-mode selectors unusable. Foreign keys remain child resources and ride
// with their owning table.
func scopeAtlasSchemaCleanPlan(
	plan schemaclean.Plan,
	scope atlasfilter.Scope,
	dialect string,
) (schemaclean.Plan, error) {
	resources := make([]atlasfilter.Resource, 0, len(plan.Objects))
	objectIndexes := make([]int, 0, len(plan.Objects))
	for i, object := range plan.Objects {
		if object.Type == schemaclean.ObjectTypeForeignKey {
			continue
		}
		name := object.SelectorName
		if name == "" {
			name = object.Name
		}
		resources = append(resources, atlasfilter.Resource{
			Types:  atlasSchemaCleanResourceTypes(object.Type),
			Schema: object.Schema,
			Name:   name,
		})
		objectIndexes = append(objectIndexes, i)
	}
	resourceSelected, err := atlasfilter.ScopeResources(resources, scope)
	if err != nil {
		return schemaclean.Plan{}, fmt.Errorf("apply cleanup selectors: %w", err)
	}
	excludeSelection, err := atlasfilter.ScopeResources(resources, atlasfilter.Scope{
		Exclude:       scope.Exclude,
		DefaultSchema: scope.DefaultSchema,
	})
	if err != nil {
		return schemaclean.Plan{}, fmt.Errorf("apply cleanup exclusions: %w", err)
	}
	selectedByIndex := make(map[int]bool, len(objectIndexes))
	excludedByIndex := make(map[int]bool, len(objectIndexes))
	selectedTables := make(map[atlasSchemaCleanTableIdentity]bool)
	for i, objectIndex := range objectIndexes {
		selected := resourceSelected[i]
		selectedByIndex[objectIndex] = selected
		excludedByIndex[objectIndex] = !excludeSelection[i]
		object := plan.Objects[objectIndex]
		if object.Type == schemaclean.ObjectTypeTable {
			selectedTables[atlasSchemaCleanTableKey(object.Schema, object.Name, scope.DefaultSchema)] = selected
		}
	}
	for i, object := range plan.Objects {
		if object.Type != schemaclean.ObjectTypeSequence || !object.Implicit {
			continue
		}
		parent := atlasSchemaCleanTableKey(object.Schema, object.Table, scope.DefaultSchema)
		parentSelected := selectedTables[parent]
		if scope.Positive() && selectedByIndex[i] && !parentSelected {
			return schemaclean.Plan{}, fmt.Errorf(
				"owned sequence %q cannot be selected independently; select its owning table %q",
				atlasSchemaCleanObjectName(object.Schema, object.Name),
				atlasSchemaCleanObjectName(parent.schema, parent.name),
			)
		}
		if excludedByIndex[i] && parentSelected {
			return schemaclean.Plan{}, fmt.Errorf(
				"owned sequence %q cannot be excluded while its owning table %q is selected; exclude the table instead",
				atlasSchemaCleanObjectName(object.Schema, object.Name),
				atlasSchemaCleanObjectName(parent.schema, parent.name),
			)
		}
	}

	selected := make([]schemaclean.Object, 0, len(plan.Objects))
	for i, object := range plan.Objects {
		keep := selectedByIndex[i]
		if object.Type == schemaclean.ObjectTypeForeignKey {
			keep = selectedTables[atlasSchemaCleanTableKey(object.Schema, object.Table, scope.DefaultSchema)]
		}
		if object.Type == schemaclean.ObjectTypeSequence && object.Implicit {
			parentSelected := selectedTables[atlasSchemaCleanTableKey(object.Schema, object.Table, scope.DefaultSchema)]
			if scope.Positive() {
				keep = keep || parentSelected
			} else {
				keep = keep && parentSelected
			}
		}
		if keep {
			selected = append(selected, object)
		}
	}
	return schemaclean.PlanFromObjects(selected, dialect), nil
}

type atlasSchemaCleanTableIdentity struct {
	schema string
	name   string
}

func atlasSchemaCleanTableKey(schema, name, defaultSchema string) atlasSchemaCleanTableIdentity {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = strings.TrimSpace(defaultSchema)
	}
	return atlasSchemaCleanTableIdentity{schema: schema, name: name}
}

func atlasSchemaCleanObjectName(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

func atlasSchemaCleanResourceTypes(objectType string) []string {
	switch objectType {
	case schemaclean.ObjectTypeForeignTable:
		return []string{schemaclean.ObjectTypeTable, schemaclean.ObjectTypeForeignTable}
	case schemaclean.ObjectTypeProcedure:
		return []string{schemaclean.ObjectTypeFunction, schemaclean.ObjectTypeProcedure}
	case schemaclean.ObjectTypeAggregate:
		return []string{schemaclean.ObjectTypeFunction, schemaclean.ObjectTypeAggregate}
	case schemaclean.ObjectTypeComposite:
		return []string{"composite_type", schemaclean.ObjectTypeComposite}
	default:
		return []string{objectType}
	}
}

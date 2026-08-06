package atlas

import (
	"fmt"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/schemaclean"
)

// scopeAtlasSchemaCleanPlan restricts a cleanup plan to the objects selected by
// --include and --exclude.
//
// The selection is decided by the same engine `schema apply`, `schema diff` and
// `schema inspect` use — [atlasfilter] — rather than by a second glob matcher
// written for this verb. The input to the engine is a synthetic schema built
// from the objects the plan already enumerates, so the selection can only ever
// narrow what [schemaclean.Inspect] found: whatever the planner learns to
// enumerate flows through this projection without the projection having to
// learn about it separately.
//
// Two deliberate differences from the apply/diff projection, both consequences
// of this being a drop rather than a comparison:
//
//   - The synthetic schema carries identities only, not columns or foreign
//     keys, so atlasfilter's cross-scope dependency validation stays quiet. That
//     validation refuses a projection that keeps an object whose dependency it
//     dropped, which is right when the projection describes a desired state and
//     wrong when it describes a drop list: dropping "orders" while leaving
//     "users" alone is a perfectly good scoped clean, and refusing it would make
//     --include unusable on any schema with foreign keys.
//   - An object kind the mapping does not know is refused rather than silently
//     kept or silently dropped. Either silent answer would mis-scope a future
//     object kind while still printing a plausible plan; see stokaro/ptah#940,
//     which is widening exactly this enumeration.
func scopeAtlasSchemaCleanPlan(
	plan schemaclean.Plan,
	scope atlasfilter.Scope,
	dialect string,
) (schemaclean.Plan, error) {
	synthetic, err := atlasSchemaCleanSyntheticSchema(plan)
	if err != nil {
		return schemaclean.Plan{}, err
	}
	var projected *dbschematypes.DBSchema
	if scope.Positive() {
		projected, err = atlasfilter.ScopeDatabase(synthetic, scope)
		if err != nil {
			return schemaclean.Plan{}, fmt.Errorf("apply --include to the cleanup plan: %w", err)
		}
	} else {
		projected, err = atlasfilter.ExcludeDatabaseWithDefaultSchema(synthetic, scope.Exclude, scope.DefaultSchema)
		if err != nil {
			return schemaclean.Plan{}, fmt.Errorf("apply --exclude to the cleanup plan: %w", err)
		}
	}
	kept := atlasSchemaCleanKeptObjects(projected)
	selected := make([]schemaclean.Object, 0, len(plan.Objects))
	for _, object := range plan.Objects {
		keep, err := atlasSchemaCleanObjectKept(kept, object)
		if err != nil {
			return schemaclean.Plan{}, err
		}
		if keep {
			selected = append(selected, object)
		}
	}
	return schemaclean.PlanFromObjects(selected, dialect), nil
}

// atlasSchemaCleanObjectIdentity is the (type, schema, name) key the projection
// round-trips an enumerated object through.
type atlasSchemaCleanObjectIdentity struct {
	objectType string
	schema     string
	name       string
}

// atlasSchemaCleanSyntheticSchema renders the planned objects as the smallest
// introspected schema that carries their identities.
func atlasSchemaCleanSyntheticSchema(plan schemaclean.Plan) (*dbschematypes.DBSchema, error) {
	schema := &dbschematypes.DBSchema{}
	for _, object := range plan.Objects {
		switch object.Type {
		case schemaclean.ObjectTypeTable:
			schema.Tables = append(schema.Tables, dbschematypes.DBTable{
				Name:   object.Name,
				Schema: object.Schema,
				Type:   "BASE TABLE",
			})
		case schemaclean.ObjectTypeEnum:
			schema.Enums = append(schema.Enums, dbschematypes.DBEnum{Name: object.Name})
		case schemaclean.ObjectTypeSequence:
			schema.Sequences = append(schema.Sequences, dbschematypes.DBSequence{
				Name:   object.Name,
				Schema: object.Schema,
			})
		case schemaclean.ObjectTypeForeignKey:
			// A foreign key is a child resource: atlasfilter refuses to select
			// one on its own, so it rides along with the table that owns it.
		default:
			return nil, fmt.Errorf(
				"--include/--exclude cannot scope cleanup object kind %q; the selector engine has no mapping for it",
				object.Type)
		}
	}
	return schema, nil
}

func atlasSchemaCleanKeptObjects(schema *dbschematypes.DBSchema) map[atlasSchemaCleanObjectIdentity]struct{} {
	kept := make(map[atlasSchemaCleanObjectIdentity]struct{})
	if schema == nil {
		return kept
	}
	for _, table := range schema.Tables {
		kept[atlasSchemaCleanObjectIdentity{schemaclean.ObjectTypeTable, table.Schema, table.Name}] = struct{}{}
	}
	for _, enum := range schema.Enums {
		kept[atlasSchemaCleanObjectIdentity{schemaclean.ObjectTypeEnum, "", enum.Name}] = struct{}{}
	}
	for _, sequence := range schema.Sequences {
		kept[atlasSchemaCleanObjectIdentity{schemaclean.ObjectTypeSequence, sequence.Schema, sequence.Name}] = struct{}{}
	}
	return kept
}

func atlasSchemaCleanObjectKept(
	kept map[atlasSchemaCleanObjectIdentity]struct{},
	object schemaclean.Object,
) (bool, error) {
	switch object.Type {
	case schemaclean.ObjectTypeTable, schemaclean.ObjectTypeEnum, schemaclean.ObjectTypeSequence:
		_, ok := kept[atlasSchemaCleanObjectIdentity{object.Type, object.Schema, object.Name}]
		return ok, nil
	case schemaclean.ObjectTypeForeignKey:
		_, ok := kept[atlasSchemaCleanObjectIdentity{schemaclean.ObjectTypeTable, object.Schema, object.Table}]
		return ok, nil
	default:
		return false, fmt.Errorf(
			"--include/--exclude cannot scope cleanup object kind %q; the selector engine has no mapping for it",
			object.Type)
	}
}

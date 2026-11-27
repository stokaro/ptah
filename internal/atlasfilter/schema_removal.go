package atlasfilter

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

// protectSchemaContents records a resource a projection removed from each
// schema. A parent database is still a compared object, but dropping it would
// also remove every resource the projection promises to leave alone.
func protectSchemaContents(before, after *catalog.Database, defaultSchema string) {
	kept := schemaContents(after, defaultSchema)
	read := schemaContents(before, defaultSchema)
	for key, object := range read {
		if _, exists := kept[key]; !exists {
			parent := exactIdentities.TableParts(object.Schema.Source, object.Parent.Source).Key()
			_, readParent := read[parent]
			_, keptParent := kept[parent]
			if readParent && !keptParent {
				continue
			}
			blockSchemaRemoval(after, object.Schema.Source, object.String())
		}
	}
}

func blockSchemaRemoval(database *catalog.Database, schema, reason string) {
	if schema == "" {
		return
	}
	prior := database.SchemaRemovalBlockers[schema]
	if prior != "" && prior <= reason {
		return
	}
	if database.SchemaRemovalBlockers == nil {
		database.SchemaRemovalBlockers = make(map[string]string)
	}
	database.SchemaRemovalBlockers[schema] = reason
}

// A whole-kind selector lets a reader omit that kind's catalog query. An
// empty collection therefore cannot prove that dropping its database is safe.
func (s *exclusionState) protectUnreadSchemaContents(database *catalog.Database) {
	for _, pattern := range s.patterns {
		if len(pattern.wholeKinds()) == 0 {
			continue
		}
		for _, schema := range database.Schemas {
			blockSchemaRemoval(database, schema.Name, fmt.Sprintf("objects excluded by %q", pattern.raw))
		}
	}
}

// schemaContents names objects by structured identity, including table parts
// and owner-defined objects. Names holding dots stay distinct from names in
// another schema. Values carry the original spelling for the refusal.
func schemaContents(database *catalog.Database, defaultSchema string) map[objectidentity.Key]objectidentity.ID {
	objects := make(map[objectidentity.Key]objectidentity.ID)
	add := func(ref objectidentity.ID) {
		if ref.Schema.Empty() {
			ref.Schema = exactIdentities.TableParts(defaultSchema, ref.Name.Source).Schema
		}
		objects[ref.Key()] = ref
	}
	addTableContents(database, add)
	addSchemaObjects(database, add)
	for _, ref := range database.FeatureObjects.Refs() {
		add(ref)
	}
	for _, object := range database.UnmodeledObjects {
		add(exactIdentities.SchemaScopedParts(objectidentity.Kind(object.Kind), object.Schema, object.Name))
	}
	return objects
}

func addTableContents(database *catalog.Database, add func(objectidentity.ID)) {
	for _, table := range database.Tables {
		add(exactIdentities.TableParts(table.Schema, table.Name))
		for _, column := range table.Columns {
			add(exactIdentities.ColumnParts(table.Schema, table.Name, column.Name))
		}
	}
	for _, index := range database.Indexes {
		add(exactIdentities.IndexParts(index.Schema, index.TableName, index.Name))
	}
	for _, constraint := range database.Constraints {
		add(exactIdentities.ConstraintParts(constraint.Schema, constraint.TableName, constraint.Name))
	}
	for _, trigger := range database.Triggers {
		add(exactIdentities.Trigger(catalog.QualifyTableName(trigger.Schema, trigger.Table), trigger.Name))
	}
	for _, policy := range database.RLSPolicies {
		add(exactIdentities.Policy(policy.Table, policy.Name))
	}
	for _, grant := range database.Grants {
		add(exactIdentities.GrantParts(grant.Schema, grant.ObjectName, grant.Role, grant.Privilege))
	}
}

func addSchemaObjects(database *catalog.Database, add func(objectidentity.ID)) {
	for _, view := range database.Views {
		add(exactIdentities.SchemaScopedParts(objectidentity.KindView, view.Schema, view.Name))
	}
	for _, view := range database.MatViews {
		add(exactIdentities.SchemaScopedParts(objectidentity.KindMatView, view.Schema, view.Name))
	}
	for _, function := range database.Functions {
		add(exactIdentities.SchemaScopedParts(objectidentity.KindFunction, function.Schema, function.Name))
	}
	for _, sequence := range database.Sequences {
		add(exactIdentities.SchemaScopedParts(objectidentity.KindSequence, sequence.Schema, sequence.Name))
	}
	for _, enum := range database.Enums {
		add(exactIdentities.SchemaScopedParts(objectidentity.KindEnum, enum.Schema, enum.Name))
	}
}

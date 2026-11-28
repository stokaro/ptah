package atlasfilter

import (
	"cmp"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

// protectSchemaContents records, for each schema and each kept table, one
// object the projection from before to after took out of it, with cause as
// the reason it left. A parent database and a kept table are still compared,
// but dropping either would also remove what the projection promises to leave
// alone. A child whose table left with it is not recorded on its own: the
// table speaks for it. A kept table is protected only by a child tablesRead
// holds, the read as far as the table drop is concerned. patterns spells each
// container for the remedies.
func protectSchemaContents(before, tablesRead, after *catalog.Database, defaultSchema string, patterns containerPatterns, cause catalog.SelectionCause) {
	kept := schemaContents(after, defaultSchema)
	read := schemaContents(before, defaultSchema)
	childrenRead := read
	if tablesRead != before {
		childrenRead = schemaContents(tablesRead, defaultSchema)
	}
	tables := keptTables(after, defaultSchema)
	for key, object := range read {
		if _, exists := kept[key]; exists {
			continue
		}
		parent := exactIdentities.TableParts(object.Schema.Source, object.Parent.Source).Key()
		_, readParent := read[parent]
		_, keptParent := kept[parent]
		if readParent && !keptParent {
			continue
		}
		blocker := catalog.SelectionBlocker{Object: object.String(), Cause: cause}
		if table, child := tables[parent]; child && !object.Parent.Empty() {
			if _, read := childrenRead[key]; read {
				blockTableRemoval(after, table, patterns, blocker)
			}
		}
		blockSchemaRemoval(after, object.Schema.Source, blocker)
	}
}

// containerPatterns spells a database or a table as a selector names it on
// one run: from the realm, where the first part of a pattern is a schema, or
// from the connection's schema, where it is an object and a table in another
// schema is reached by its bare name. scope is the schema patterns count from,
// empty on a realm run, and defaultSchema owns a table the read left
// unqualified.
type containerPatterns struct {
	scope, defaultSchema string
}

func (p containerPatterns) table(schema, name string) string {
	schema = cmp.Or(schema, p.defaultSchema)
	if p.scope != "" || schema == "" {
		return name
	}
	return schema + "." + name
}

// keptTables maps the identity of each table database holds, keyed as
// [schemaContents] keys a parent, to the table.
func keptTables(database *catalog.Database, defaultSchema string) map[objectidentity.Key]catalog.Table {
	tables := make(map[objectidentity.Key]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		ref := exactIdentities.TableParts(table.Schema, table.Name)
		if ref.Schema.Empty() {
			ref.Schema = exactIdentities.TableParts(defaultSchema, ref.Name.Source).Schema
		}
		tables[ref.Key()] = table
	}
	return tables
}

func blockSchemaRemoval(database *catalog.Database, schema string, blocker catalog.SelectionBlocker) {
	if schema == "" {
		return
	}
	blocker.Pattern = schema
	database.SchemaRemovalBlockers = withBlocker(database.SchemaRemovalBlockers, schema, blocker)
}

// blockTableRemoval records blocker for table, keyed as
// [catalog.Database.TableRemovalBlockers] keys it: the read's own spelling,
// with no default schema put in.
func blockTableRemoval(database *catalog.Database, table catalog.Table, patterns containerPatterns, blocker catalog.SelectionBlocker) {
	blocker.Pattern = patterns.table(table.Schema, table.Name)
	database.TableRemovalBlockers = withBlocker(database.TableRemovalBlockers, catalog.QualifyTableName(table.Schema, table.Name), blocker)
}

// withBlocker records blocker for name unless one that sorts first is already
// there, so the blocker a refusal names does not depend on map order.
func withBlocker(blockers map[string]catalog.SelectionBlocker, name string, blocker catalog.SelectionBlocker) map[string]catalog.SelectionBlocker {
	if prior, exists := blockers[name]; exists && !blockerSortsBefore(blocker, prior) {
		return blockers
	}
	if blockers == nil {
		blockers = make(map[string]catalog.SelectionBlocker)
	}
	blockers[name] = blocker
	return blockers
}

// blockerSortsBefore orders blockers so a refusal names an object the read
// found ahead of a kind the read left out, which says less about what the
// drop would remove.
func blockerSortsBefore(a, b catalog.SelectionBlocker) bool {
	return cmp.Or(
		cmp.Compare(wholeKindCause(a.Cause), wholeKindCause(b.Cause)),
		cmp.Compare(a.Object, b.Object),
		cmp.Compare(a.Cause, b.Cause),
		cmp.Compare(a.Selector, b.Selector),
	) < 0
}

// wholeKindCause ranks a cause that names a kind the read left out after one
// that names an object.
func wholeKindCause(cause catalog.SelectionCause) int {
	if cause == catalog.SelectionKindExcluded || cause == catalog.SelectionKindNotIncluded {
		return 1
	}
	return 0
}

// A whole-kind selector lets a reader omit that kind's catalog query. An
// empty collection therefore cannot prove that dropping its database is safe.
//
// It blocks no table drop. A trigger or a constraint cannot outlive its
// table, and the run drops a table the desired schema leaves out; an account
// that lacks the privilege to read a kind takes this selector so it can plan
// at all. A child the read found and a pattern of another shape excludes
// still blocks its table, through [protectSchemaContents].
func (s *exclusionState) protectUnreadSchemaContents(database *catalog.Database) {
	for _, pattern := range s.patterns {
		kinds := pattern.wholeKinds()
		if len(kinds) == 0 {
			continue
		}
		blocker := catalog.SelectionBlocker{Object: string(kinds[0]), Cause: catalog.SelectionKindExcluded, Selector: pattern.raw}
		for _, schema := range database.Schemas {
			blockSchemaRemoval(database, schema.Name, blocker)
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

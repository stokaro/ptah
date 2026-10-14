// Package schemascope applies Atlas-style schema allow-lists to Ptah schema IRs.
package schemascope

import (
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/tableref"
)

// SplitNames expands repeated and comma-separated schema filter values.
func SplitNames(values []string) []string {
	seen := make(map[string]struct{})
	var schemas []string
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			schema := strings.TrimSpace(part)
			if schema == "" {
				continue
			}
			if _, ok := seen[schema]; ok {
				continue
			}
			seen[schema] = struct{}{}
			schemas = append(schemas, schema)
		}
	}
	return schemas
}

// FilterGenerated returns a shallow copy of db containing only objects in the
// selected schemas. Empty schema filters leave db unchanged. A context and a
// selected owner set are required; owner failures return no projection.
func FilterGenerated(ctx context.Context, selections selectext.Set, db *schemamodel.Database, schemas []string) (*schemamodel.Database, error) {
	return FilterGeneratedWithDefaultSchema(ctx, selections, db, schemas, "")
}

// FilterGeneratedWithDefaultSchema returns a shallow copy of db containing only
// objects in the selected schemas, treating unqualified objects as belonging to
// defaultSchema when it is set. Owner-described schema-wide state uses its
// explicit schema; an empty schema is global. Unknown coverage stays diagnostic.
func FilterGeneratedWithDefaultSchema(
	ctx context.Context, selections selectext.Set,
	db *schemamodel.Database,
	schemas []string,
	defaultSchema string,
) (*schemamodel.Database, error) {
	if err := validateFilter(ctx, selections); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, nil
	}
	allowed := schemaSet(schemas)
	if len(allowed) == 0 {
		return db, nil
	}

	filtered := *db
	keptStructs := make(map[string]struct{})
	keptTables := make(map[string]schemamodel.Table)
	filtered.Tables = keep(db.Tables, func(table schemamodel.Table) bool {
		if !schemaAllowed(allowed, effectiveSchema(table.Schema, defaultSchema)) {
			return false
		}
		keptStructs[table.StructName] = struct{}{}
		keptTables[table.QualifiedName()] = table
		return true
	})
	filtered.Schemas = keep(db.Schemas, func(schema schemamodel.Schema) bool {
		return schemaAllowed(allowed, effectiveSchema(schema.Name, defaultSchema))
	})
	filtered.Fields = keep(db.Fields, func(field schemamodel.Field) bool {
		if _, ok := keptStructs[field.StructName]; !ok {
			return false
		}
		return true
	})
	filtered.Fields = stripOutOfScopeFieldFKs(filtered.Fields, keptTables)
	filtered.Indexes = keep(db.Indexes, func(index schemamodel.Index) bool {
		return generatedStructOrTableAllowed(keptStructs, keptTables, index.StructName, index.TableName)
	})
	filtered.Constraints = keep(db.Constraints, func(constraint schemamodel.Constraint) bool {
		if !generatedStructOrTableAllowed(keptStructs, keptTables, constraint.StructName, constraint.Table) {
			return false
		}
		return foreignTableAllowed(keptTables, constraint.ForeignTable)
	})
	filtered.EmbeddedFields = keep(db.EmbeddedFields, func(field schemamodel.EmbeddedField) bool {
		_, ok := keptStructs[field.StructName]
		return ok
	})
	filtered.Functions = keep(db.Functions, func(function schemamodel.Function) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, function.StructName, function.Name, defaultSchema)
	})
	filtered.Views = keep(db.Views, func(view schemamodel.View) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, view.StructName, view.Name, defaultSchema)
	})
	filtered.MaterializedViews = keep(db.MaterializedViews, func(view schemamodel.MaterializedView) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, view.StructName, view.Name, defaultSchema)
	})
	filtered.Triggers = keep(db.Triggers, func(trigger schemamodel.Trigger) bool {
		return tableReferenceAllowed(keptTables, trigger.Table)
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy schemamodel.RLSPolicy) bool {
		return tableReferenceAllowed(keptTables, policy.Table)
	})
	filtered.RLSEnabledTables = keep(db.RLSEnabledTables, func(table schemamodel.RLSEnabledTable) bool {
		return tableReferenceAllowed(keptTables, table.Table)
	})
	// A table's feature children, such as its row-level security policies,
	// follow the table. An object with no parent stays: an extension is a
	// database-wide identity whose schema is installation placement, not
	// ownership, and a selected table may depend on one installed in another
	// schema.
	var err error
	filtered.FeatureObjects, filtered.FeatureCoverage, err = filterFeatureScopes(ctx, selections, db.FeatureObjects, db.FeatureCoverage, schemas, func(ref objectidentity.ID) bool {
		return ref.Parent.Empty() || tableReferenceAllowed(keptTables, childTable(ref))
	})
	if err != nil {
		return nil, err
	}
	filtered.Grants = keep(db.Grants, func(grant schemamodel.Grant) bool {
		return grantAllowed(allowed, keptTables, grant, defaultSchema)
	})
	filtered.RevokedGrants = keep(db.RevokedGrants, func(grant schemamodel.Grant) bool {
		return grantAllowed(allowed, keptTables, grant, defaultSchema)
	})
	filtered.Enums = keepReferencedGeneratedEnums(db.Enums, filtered.Fields)
	filtered.Dependencies = filterDependencies(db.Dependencies, keptTables)
	filtered.FunctionDependencies = filterNamedDependencies(db.FunctionDependencies, allowed, defaultSchema)
	filtered.SelfReferencingForeignKeys = filterSelfReferencingForeignKeys(db.SelfReferencingForeignKeys, keptTables)

	return &filtered, nil
}

// FilterDatabase returns a shallow copy of db containing only objects in the
// selected schemas. Empty schema filters leave db unchanged. A context and a
// selected owner set are required; owner failures return no projection.
func FilterDatabase(ctx context.Context, selections selectext.Set, db *catalog.Database, schemas []string) (*catalog.Database, error) {
	return FilterDatabaseWithDefaultSchema(ctx, selections, db, schemas, "")
}

// FilterDatabaseWithDefaultSchema returns a shallow copy of db containing only
// objects in the selected schemas, treating unqualified objects as belonging to
// defaultSchema when it is set. Owner-described schema-wide state uses its
// explicit schema; an empty schema is global. Unknown coverage stays diagnostic.
func FilterDatabaseWithDefaultSchema(
	ctx context.Context, selections selectext.Set,
	db *catalog.Database,
	schemas []string,
	defaultSchema string,
) (*catalog.Database, error) {
	if err := validateFilter(ctx, selections); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, nil
	}
	allowed := schemaSet(schemas)
	if len(allowed) == 0 {
		return db, nil
	}

	filtered := *db
	keptTables := make(map[string]struct{})
	filtered.Tables = keep(db.Tables, func(table catalog.Table) bool {
		if !schemaAllowed(allowed, effectiveSchema(table.Schema, defaultSchema)) {
			return false
		}
		keptTables[table.QualifiedName()] = struct{}{}
		return true
	})
	filtered.Indexes = keep(db.Indexes, func(index catalog.Index) bool {
		return schemaAllowed(allowed, effectiveSchema(index.Schema, defaultSchema))
	})
	filtered.Constraints = keep(db.Constraints, func(constraint catalog.Constraint) bool {
		if !schemaAllowed(allowed, effectiveSchema(constraint.Schema, defaultSchema)) {
			return false
		}
		return constraint.ForeignSchema == "" ||
			schemaAllowed(allowed, effectiveSchema(constraint.ForeignSchema, defaultSchema))
	})
	filtered.Views = keep(db.Views, func(view catalog.View) bool {
		return schemaAllowed(allowed, effectiveSchema(view.Schema, defaultSchema))
	})
	filtered.MatViews = keep(db.MatViews, func(view catalog.MaterializedView) bool {
		return schemaAllowed(allowed, effectiveSchema(view.Schema, defaultSchema))
	})
	filtered.Triggers = keep(db.Triggers, func(trigger catalog.Trigger) bool {
		return schemaAllowed(allowed, effectiveSchema(trigger.Schema, defaultSchema)) &&
			tableKeyAllowed(keptTables, trigger.QualifiedTable())
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy catalog.RLSPolicy) bool {
		return dbTableReferenceAllowed(keptTables, policy.Table)
	})
	// Every extension stays, for the database-wide identity reason the
	// declared projection gives. Filtering only one side invents additions or
	// removals when a selected object uses an extension placed elsewhere.
	var err error
	filtered.FeatureObjects, filtered.FeatureCoverage, err = filterFeatureScopes(ctx, selections, db.FeatureObjects, db.FeatureCoverage, schemas, func(ref objectidentity.ID) bool {
		return ref.Parent.Empty() || dbTableReferenceAllowed(keptTables, childTable(ref))
	})
	if err != nil {
		return nil, err
	}
	filtered.Grants = keep(db.Grants, func(grant catalog.Grant) bool {
		return dbGrantAllowed(allowed, keptTables, grant, defaultSchema)
	})
	filtered.Enums = keepReferencedDatabaseEnums(db.Enums, filtered.Tables)

	return &filtered, nil
}

func schemaSet(values []string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, schema := range SplitNames(values) {
		set[schema] = struct{}{}
	}
	return set
}

func schemaAllowed(allowed map[string]struct{}, schema string) bool {
	_, ok := allowed[strings.TrimSpace(schema)]
	return ok
}

func effectiveSchema(schema, defaultSchema string) string {
	schema = strings.TrimSpace(schema)
	if schema != "" {
		return schema
	}
	return strings.TrimSpace(defaultSchema)
}

func keep[T any](items []T, shouldKeep func(T) bool) []T {
	out := make([]T, 0, len(items))
	for _, item := range items {
		if shouldKeep(item) {
			out = append(out, item)
		}
	}
	return out
}

func generatedStructOrTableAllowed(
	keptStructs map[string]struct{},
	keptTables map[string]schemamodel.Table,
	structName,
	tableName string,
) bool {
	if strings.TrimSpace(tableName) != "" {
		return tableReferenceAllowed(keptTables, tableName)
	}
	if _, ok := keptStructs[structName]; ok {
		return true
	}
	return false
}

func generatedNamedObjectAllowed(
	allowed,
	keptStructs map[string]struct{},
	structName,
	name,
	defaultSchema string,
) bool {
	if _, ok := keptStructs[structName]; ok {
		return true
	}
	return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(name), defaultSchema))
}

// childTable names the table a feature object belongs to, as its source
// spelled it.
func childTable(ref objectidentity.ID) string {
	return schemamodel.QualifyTableName(ref.Schema.Authored(), ref.Parent.Source)
}

func tableReferenceAllowed(keptTables map[string]schemamodel.Table, tableName string) bool {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return false
	}
	if _, ok := keptTables[tableName]; ok {
		return true
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return false
	}
	for _, table := range keptTables {
		if table.Name == ref.Name {
			return true
		}
	}
	return false
}

func foreignTableAllowed(keptTables map[string]schemamodel.Table, foreignTable string) bool {
	foreignTable = strings.TrimSpace(foreignTable)
	if foreignTable == "" {
		return true
	}
	return tableReferenceAllowed(keptTables, foreignTable)
}

func stripOutOfScopeFieldFKs(fields []schemamodel.Field, keptTables map[string]schemamodel.Table) []schemamodel.Field {
	out := append([]schemamodel.Field(nil), fields...)
	for i := range out {
		refTable := foreignReferenceTable(out[i].Foreign)
		if refTable == "" || tableReferenceAllowed(keptTables, refTable) {
			continue
		}
		out[i].Foreign = ""
		out[i].ForeignKeyName = ""
		out[i].OnDelete = ""
		out[i].OnUpdate = ""
	}
	return out
}

func foreignReferenceTable(reference string) string {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return ""
	}
	table, _, _ := strings.Cut(reference, "(")
	return strings.TrimSpace(table)
}

func schemaFromQualifiedName(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok || !ref.Qualified {
		return ""
	}
	return ref.Schema
}

func grantAllowed(
	allowed map[string]struct{},
	keptTables map[string]schemamodel.Table,
	grant schemamodel.Grant,
	defaultSchema string,
) bool {
	// Database permissions have no owning schema, just like their principals.
	if grant.OnDatabase {
		return true
	}
	if grant.OnSchema != "" {
		return schemaAllowed(allowed, effectiveSchema(grant.OnSchema, defaultSchema))
	}
	if grant.OnTable != "" {
		return tableReferenceAllowed(keptTables, grant.OnTable)
	}
	if grant.OnRoutine != "" {
		// A routine is kept by its schema, and its grants ride it.
		return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(grant.OnRoutine), defaultSchema))
	}
	if grant.OnSequence != "" {
		// So is a sequence, standalone or owned by a column; see
		// [dbGrantAllowed] for the other side.
		return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(grant.OnSequence), defaultSchema))
	}
	if grant.OnObject != "" {
		// So is a scheme object such as a YDB topic, by the directory it is in.
		return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(grant.OnObject), defaultSchema))
	}
	return false
}

func dbGrantAllowed(
	allowed,
	keptTables map[string]struct{},
	grant catalog.Grant,
	defaultSchema string,
) bool {
	if strings.EqualFold(grant.ObjectType, "DATABASE") {
		return true
	}
	if strings.EqualFold(grant.ObjectType, "SCHEMA") {
		return schemaAllowed(allowed, effectiveSchema(grant.ObjectName, defaultSchema))
	}
	// A routine and a sequence are kept by their schema, as the generated
	// side keeps them. A sequence is not a table, so the table check below
	// dropped every grant on one from the current side, and a comparison with
	// a desired schema that grants on it planned the grant on every run
	// (stokaro/ptah#4085).
	if strings.EqualFold(grant.ObjectType, "FUNCTION") || strings.EqualFold(grant.ObjectType, "PROCEDURE") ||
		strings.EqualFold(grant.ObjectType, "SEQUENCE") {
		return schemaAllowed(allowed, effectiveSchema(grant.Schema, defaultSchema))
	}
	// A scheme object such as a YDB topic is kept by its directory too.
	if _, object := schemamodel.GrantObjectAttribute(grant.ObjectType); object {
		return schemaAllowed(allowed, effectiveSchema(grant.Schema, defaultSchema))
	}
	return schemaAllowed(allowed, effectiveSchema(grant.Schema, defaultSchema)) &&
		dbTableReferenceAllowed(keptTables, grant.QualifiedTarget())
}

func tableKeyAllowed(keptTables map[string]struct{}, tableName string) bool {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return false
	}
	_, ok := keptTables[tableName]
	return ok
}

func dbTableReferenceAllowed(keptTables map[string]struct{}, tableName string) bool {
	if tableKeyAllowed(keptTables, tableName) {
		return true
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return false
	}
	for keptTable := range keptTables {
		keptRef, valid := tableref.Parse(keptTable)
		if valid && keptRef.Name == ref.Name {
			return true
		}
	}
	return false
}

func keepReferencedGeneratedEnums(enums []schemamodel.Enum, fields []schemamodel.Field) []schemamodel.Enum {
	// Every field type is a candidate reference; the keep below intersects them
	// with the names the schema actually declares as enums, so a type that names
	// something else simply never matches.
	//
	// Admitting only types spelled "enum_*" makes a scoped run drop the CREATE
	// TYPE for an enum named e.g. "status_kind" while the column still names
	// it, and `ptah schema test --schema public` then dies with
	// `type "status_kind" does not exist` (SQLSTATE 42704) -- a hard failure,
	// not the silent under-generation the rest of stokaro/ptah#931 item 1
	// produces.
	referenced := make(map[string]struct{})
	for _, field := range fields {
		referenced[field.Type] = struct{}{}
	}
	return keep(enums, func(enum schemamodel.Enum) bool {
		_, ok := referenced[enum.Name]
		return ok
	})
}

func keepReferencedDatabaseEnums(enums []catalog.Enum, tables []catalog.Table) []catalog.Enum {
	referenced := make(map[string]struct{})
	for _, table := range tables {
		for _, column := range table.Columns {
			ref, ok := databaseEnumRef(column)
			if ok {
				referenced[ref] = struct{}{}
			}
		}
	}
	return keep(enums, func(enum catalog.Enum) bool {
		_, ok := referenced[enum.Name]
		return ok
	})
}

func databaseEnumRef(column catalog.Column) (string, bool) {
	if column.UDTName == "" {
		return "", false
	}

	switch strings.ToUpper(column.DataType) {
	case "USER-DEFINED":
		return column.UDTName, true
	case "ARRAY":
		ref := strings.TrimPrefix(column.UDTName, "_")
		return ref, ref != ""
	case "":
		return column.UDTName, true
	default:
		return "", false
	}
}

func filterDependencies(in map[string][]string, keptTables map[string]schemamodel.Table) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for table, deps := range in {
		if !tableReferenceAllowed(keptTables, table) {
			continue
		}
		out[table] = keep(deps, func(dep string) bool {
			return tableReferenceAllowed(keptTables, dep)
		})
	}
	return out
}

func filterNamedDependencies(
	in map[string][]string,
	allowed map[string]struct{},
	defaultSchema string,
) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for name, deps := range in {
		if !schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(name), defaultSchema)) {
			continue
		}
		out[name] = keep(deps, func(dep string) bool {
			return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(dep), defaultSchema))
		})
	}
	return out
}

func filterSelfReferencingForeignKeys(
	in map[string][]schemamodel.SelfReferencingFK,
	keptTables map[string]schemamodel.Table,
) map[string][]schemamodel.SelfReferencingFK {
	if in == nil {
		return nil
	}
	out := make(map[string][]schemamodel.SelfReferencingFK, len(in))
	for table, refs := range in {
		if tableReferenceAllowed(keptTables, table) {
			out[table] = refs
		}
	}
	return out
}

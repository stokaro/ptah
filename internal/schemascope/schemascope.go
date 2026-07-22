// Package schemascope applies Atlas-style schema allow-lists to Ptah schema IRs.
package schemascope

import (
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
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
// selected schemas. Empty schema filters leave db unchanged.
func FilterGenerated(db *goschema.Database, schemas []string) *goschema.Database {
	return FilterGeneratedWithDefaultSchema(db, schemas, "")
}

// FilterGeneratedWithDefaultSchema returns a shallow copy of db containing only
// objects in the selected schemas, treating unqualified objects as belonging to
// defaultSchema when it is set.
func FilterGeneratedWithDefaultSchema(
	db *goschema.Database,
	schemas []string,
	defaultSchema string,
) *goschema.Database {
	if db == nil {
		return nil
	}
	allowed := schemaSet(schemas)
	if len(allowed) == 0 {
		return db
	}

	filtered := *db
	keptStructs := make(map[string]struct{})
	keptTables := make(map[string]goschema.Table)
	filtered.Tables = keep(db.Tables, func(table goschema.Table) bool {
		if !schemaAllowed(allowed, effectiveSchema(table.Schema, defaultSchema)) {
			return false
		}
		keptStructs[table.StructName] = struct{}{}
		keptTables[table.QualifiedName()] = table
		return true
	})
	filtered.Schemas = keep(db.Schemas, func(schema goschema.Schema) bool {
		return schemaAllowed(allowed, effectiveSchema(schema.Name, defaultSchema))
	})
	filtered.Fields = keep(db.Fields, func(field goschema.Field) bool {
		if _, ok := keptStructs[field.StructName]; !ok {
			return false
		}
		return true
	})
	filtered.Fields = stripOutOfScopeFieldFKs(filtered.Fields, keptTables)
	filtered.Indexes = keep(db.Indexes, func(index goschema.Index) bool {
		return generatedStructOrTableAllowed(keptStructs, keptTables, index.StructName, index.TableName)
	})
	filtered.Constraints = keep(db.Constraints, func(constraint goschema.Constraint) bool {
		if !generatedStructOrTableAllowed(keptStructs, keptTables, constraint.StructName, constraint.Table) {
			return false
		}
		return foreignTableAllowed(keptTables, constraint.ForeignTable)
	})
	filtered.EmbeddedFields = keep(db.EmbeddedFields, func(field goschema.EmbeddedField) bool {
		_, ok := keptStructs[field.StructName]
		return ok
	})
	filtered.Functions = keep(db.Functions, func(function goschema.Function) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, function.StructName, function.Name, defaultSchema)
	})
	filtered.Views = keep(db.Views, func(view goschema.View) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, view.StructName, view.Name, defaultSchema)
	})
	filtered.MaterializedViews = keep(db.MaterializedViews, func(view goschema.MaterializedView) bool {
		return generatedNamedObjectAllowed(allowed, keptStructs, view.StructName, view.Name, defaultSchema)
	})
	filtered.Triggers = keep(db.Triggers, func(trigger goschema.Trigger) bool {
		return tableReferenceAllowed(keptTables, trigger.Table)
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy goschema.RLSPolicy) bool {
		return tableReferenceAllowed(keptTables, policy.Table)
	})
	filtered.RLSEnabledTables = keep(db.RLSEnabledTables, func(table goschema.RLSEnabledTable) bool {
		return tableReferenceAllowed(keptTables, table.Table)
	})
	filtered.Grants = keep(db.Grants, func(grant goschema.Grant) bool {
		return grantAllowed(allowed, keptTables, grant, defaultSchema)
	})
	filtered.Enums = keepReferencedGeneratedEnums(db.Enums, filtered.Fields)
	filtered.Dependencies = filterDependencies(db.Dependencies, keptTables)
	filtered.FunctionDependencies = filterNamedDependencies(db.FunctionDependencies, allowed, defaultSchema)
	filtered.SelfReferencingForeignKeys = filterSelfReferencingForeignKeys(db.SelfReferencingForeignKeys, keptTables)

	return &filtered
}

// FilterDatabase returns a shallow copy of db containing only objects in the
// selected schemas. Empty schema filters leave db unchanged.
func FilterDatabase(db *dbschematypes.DBSchema, schemas []string) *dbschematypes.DBSchema {
	return FilterDatabaseWithDefaultSchema(db, schemas, "")
}

// FilterDatabaseWithDefaultSchema returns a shallow copy of db containing only
// objects in the selected schemas, treating unqualified objects as belonging to
// defaultSchema when it is set.
func FilterDatabaseWithDefaultSchema(
	db *dbschematypes.DBSchema,
	schemas []string,
	defaultSchema string,
) *dbschematypes.DBSchema {
	if db == nil {
		return nil
	}
	allowed := schemaSet(schemas)
	if len(allowed) == 0 {
		return db
	}

	filtered := *db
	keptTables := make(map[string]struct{})
	filtered.Tables = keep(db.Tables, func(table dbschematypes.DBTable) bool {
		if !schemaAllowed(allowed, effectiveSchema(table.Schema, defaultSchema)) {
			return false
		}
		keptTables[table.QualifiedName()] = struct{}{}
		keptTables[table.Name] = struct{}{}
		return true
	})
	filtered.Indexes = keep(db.Indexes, func(index dbschematypes.DBIndex) bool {
		return schemaAllowed(allowed, effectiveSchema(index.Schema, defaultSchema))
	})
	filtered.Constraints = keep(db.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		if !schemaAllowed(allowed, effectiveSchema(constraint.Schema, defaultSchema)) {
			return false
		}
		return constraint.ForeignSchema == "" ||
			schemaAllowed(allowed, effectiveSchema(constraint.ForeignSchema, defaultSchema))
	})
	filtered.Extensions = keep(db.Extensions, func(extension dbschematypes.DBExtension) bool {
		return schemaAllowed(allowed, effectiveSchema(extension.Schema, defaultSchema))
	})
	filtered.Views = keep(db.Views, func(view dbschematypes.DBView) bool {
		return schemaAllowed(allowed, effectiveSchema(view.Schema, defaultSchema))
	})
	filtered.MatViews = keep(db.MatViews, func(view dbschematypes.DBMatView) bool {
		return schemaAllowed(allowed, effectiveSchema(view.Schema, defaultSchema))
	})
	filtered.Triggers = keep(db.Triggers, func(trigger dbschematypes.DBTrigger) bool {
		return schemaAllowed(allowed, effectiveSchema(trigger.Schema, defaultSchema)) &&
			tableKeyAllowed(keptTables, trigger.QualifiedTable())
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy dbschematypes.DBRLSPolicy) bool {
		return dbTableReferenceAllowed(keptTables, policy.Table)
	})
	filtered.Grants = keep(db.Grants, func(grant dbschematypes.DBGrant) bool {
		return dbGrantAllowed(allowed, keptTables, grant, defaultSchema)
	})
	filtered.Enums = keepReferencedDatabaseEnums(db.Enums, filtered.Tables)

	return &filtered
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

func effectiveSchema(schema string, defaultSchema string) string {
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
	keptTables map[string]goschema.Table,
	structName string,
	tableName string,
) bool {
	if _, ok := keptStructs[structName]; ok {
		return true
	}
	return tableReferenceAllowed(keptTables, tableName)
}

func generatedNamedObjectAllowed(
	allowed map[string]struct{},
	keptStructs map[string]struct{},
	structName string,
	name string,
	defaultSchema string,
) bool {
	if _, ok := keptStructs[structName]; ok {
		return true
	}
	return schemaAllowed(allowed, effectiveSchema(schemaFromQualifiedName(name), defaultSchema))
}

func tableReferenceAllowed(keptTables map[string]goschema.Table, tableName string) bool {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return false
	}
	if _, ok := keptTables[tableName]; ok {
		return true
	}
	for _, table := range keptTables {
		if table.Name == tableName {
			return true
		}
	}
	return false
}

func foreignTableAllowed(keptTables map[string]goschema.Table, foreignTable string) bool {
	foreignTable = strings.TrimSpace(foreignTable)
	if foreignTable == "" {
		return true
	}
	return tableReferenceAllowed(keptTables, foreignTable)
}

func stripOutOfScopeFieldFKs(fields []goschema.Field, keptTables map[string]goschema.Table) []goschema.Field {
	out := append([]goschema.Field(nil), fields...)
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
	name = strings.TrimSpace(name)
	schema, _, ok := strings.Cut(name, ".")
	if !ok {
		return ""
	}
	return strings.TrimSpace(schema)
}

func grantAllowed(
	allowed map[string]struct{},
	keptTables map[string]goschema.Table,
	grant goschema.Grant,
	defaultSchema string,
) bool {
	if grant.OnSchema != "" {
		return schemaAllowed(allowed, effectiveSchema(grant.OnSchema, defaultSchema))
	}
	if grant.OnTable != "" {
		return tableReferenceAllowed(keptTables, grant.OnTable)
	}
	return false
}

func dbGrantAllowed(
	allowed map[string]struct{},
	keptTables map[string]struct{},
	grant dbschematypes.DBGrant,
	defaultSchema string,
) bool {
	if strings.EqualFold(grant.ObjectType, "SCHEMA") {
		return schemaAllowed(allowed, effectiveSchema(grant.ObjectName, defaultSchema))
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
	_, table, ok := strings.Cut(strings.TrimSpace(tableName), ".")
	if !ok {
		return false
	}
	return tableKeyAllowed(keptTables, table)
}

func keepReferencedGeneratedEnums(enums []goschema.Enum, fields []goschema.Field) []goschema.Enum {
	referenced := make(map[string]struct{})
	for _, field := range fields {
		if strings.HasPrefix(field.Type, "enum_") {
			referenced[field.Type] = struct{}{}
		}
	}
	return keep(enums, func(enum goschema.Enum) bool {
		_, ok := referenced[enum.Name]
		return ok
	})
}

func keepReferencedDatabaseEnums(enums []dbschematypes.DBEnum, tables []dbschematypes.DBTable) []dbschematypes.DBEnum {
	referenced := make(map[string]struct{})
	for _, table := range tables {
		for _, column := range table.Columns {
			ref, ok := databaseEnumRef(column)
			if ok {
				referenced[ref] = struct{}{}
			}
		}
	}
	return keep(enums, func(enum dbschematypes.DBEnum) bool {
		_, ok := referenced[enum.Name]
		return ok
	})
}

func databaseEnumRef(column dbschematypes.DBColumn) (string, bool) {
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

func filterDependencies(in map[string][]string, keptTables map[string]goschema.Table) map[string][]string {
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
	in map[string][]goschema.SelfReferencingFK,
	keptTables map[string]goschema.Table,
) map[string][]goschema.SelfReferencingFK {
	if in == nil {
		return nil
	}
	out := make(map[string][]goschema.SelfReferencingFK, len(in))
	for table, refs := range in {
		if tableReferenceAllowed(keptTables, table) {
			out[table] = refs
		}
	}
	return out
}

package atlasfilter

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// projectGenerated applies the schema universe and include selectors to the
// generated-schema IR. Children of a selected table (fields, indexes,
// constraints, triggers, policies, grants, seed data) ride along with it;
// referenced support objects (enums, domains, composite types, ranges,
// sequences owned by selected tables, roles named by selected grants) are
// retained as dependencies even when no selector names them.
func (s *scopeSelection) projectGenerated(db *goschema.Database) *goschema.Database {
	out := cloneGenerated(db)
	out.Tables = keep(db.Tables, func(table goschema.Table) bool {
		return s.selected(typeList("table"), table.Schema, table.Name)
	})
	keptByStruct := generatedTableByStruct(out.Tables)

	out.Fields = keep(db.Fields, func(field goschema.Field) bool {
		_, ok := keptByStruct[field.StructName]
		return ok
	})
	out.EmbeddedFields = keep(db.EmbeddedFields, func(field goschema.EmbeddedField) bool {
		_, ok := keptByStruct[field.StructName]
		return ok
	})
	out.Indexes = keep(db.Indexes, func(index goschema.Index) bool {
		_, ok := generatedIndexTable(keptByStruct, index)
		return ok
	})
	out.Constraints = keep(db.Constraints, func(constraint goschema.Constraint) bool {
		_, ok := generatedConstraintTable(keptByStruct, constraint)
		return ok
	})
	out.Triggers = keep(db.Triggers, func(trigger goschema.Trigger) bool {
		_, ok := generatedObjectTable(keptByStruct, trigger.StructName, trigger.Table)
		return ok
	})
	out.RLSPolicies = keep(db.RLSPolicies, func(policy goschema.RLSPolicy) bool {
		_, ok := generatedObjectTable(keptByStruct, policy.StructName, policy.Table)
		return ok
	})
	out.RLSEnabledTables = keep(db.RLSEnabledTables, func(table goschema.RLSEnabledTable) bool {
		_, ok := generatedObjectTable(keptByStruct, table.StructName, table.Table)
		return ok
	})
	out.ManagedData = keep(db.ManagedData, func(data goschema.ManagedData) bool {
		return generatedTableNameKept(out.Tables, data.Table)
	})

	s.projectGeneratedTopLevel(db, out)
	s.projectGeneratedSupport(db, out)
	out.Schemas = s.keepGeneratedSchemas(db, out)

	out.Dependencies = nil
	out.FunctionDependencies = nil
	out.SelfReferencingForeignKeys = nil
	goschema.Finalize(out)
	return out
}

// projectGeneratedTopLevel selects independently includable top-level
// resources: views, materialized views, functions, extensions, and roles.
// Views, materialized views, and functions carry their schema in an optional
// "schema." name prefix; extensions and roles are database-scoped and skip
// the schema universe.
func (s *scopeSelection) projectGeneratedTopLevel(db, out *goschema.Database) {
	out.Views = keep(db.Views, func(view goschema.View) bool {
		return s.selectedQualifiedName(typeList("view"), view.Name)
	})
	out.MaterializedViews = keep(db.MaterializedViews, func(view goschema.MaterializedView) bool {
		return s.selectedQualifiedName(typeList("materialized_view"), view.Name)
	})
	out.Functions = keep(db.Functions, func(function goschema.Function) bool {
		return s.selectedQualifiedName(typeList("function"), function.Name)
	})
	out.Extensions = keep(db.Extensions, func(extension goschema.Extension) bool {
		return s.selectedNames(typeList("extension"), extension.Name)
	})
	out.Sequences = keep(db.Sequences, func(sequence goschema.Sequence) bool {
		if s.selected(typeList("sequence"), sequence.Schema, sequence.Name) {
			return true
		}
		return generatedTableNameKept(out.Tables, sequenceOwnerReference(sequence.OwnedBy))
	})
	out.Grants = keep(db.Grants, func(grant goschema.Grant) bool {
		return s.generatedGrantSelected(out, grant)
	})
	out.Roles = keep(db.Roles, func(role goschema.Role) bool {
		if s.selectedNames(typeList("role"), role.Name) {
			return true
		}
		return generatedGrantRoleReferenced(out.Grants, role.Name)
	})
}

// projectGeneratedSupport retains type objects used by selected tables even
// when no selector names them: enums, domains, composite types, and ranges
// referenced by kept column types.
func (s *scopeSelection) projectGeneratedSupport(db, out *goschema.Database) {
	referenced := generatedFieldTypeSet(out.Fields)
	out.Enums = keepEnumObjects(s, db.Enums, referenced,
		func(e goschema.Enum) (string, string) { return e.Schema, e.Name })
	out.Domains = keepTypeObjects(s, db.Domains, "domain", referenced,
		func(d goschema.Domain) (string, string) { return d.Schema, d.Name })
	out.CompositeTypes = keepTypeObjects(s, db.CompositeTypes, "composite_type", referenced,
		func(c goschema.CompositeType) (string, string) { return c.Schema, c.Name })
	out.Ranges = keepTypeObjects(s, db.Ranges, "range", referenced,
		func(r goschema.Range) (string, string) { return r.Schema, r.Name })
}

// keepEnumObjects keeps enums that are either selected by the scope or
// referenced by kept column types. Current enum models carry schema and name
// separately. An empty schema retains support for SQL-source enum values whose
// legacy conversion parked the qualifier in Name.
func keepEnumObjects[T any](
	s *scopeSelection,
	items []T,
	referenced map[string]struct{},
	key func(T) (schema, name string),
) []T {
	return keep(items, func(item T) bool {
		schema, name := enumIdentity(key(item))
		if s.selected(typeList("enum"), schema, name) {
			return true
		}
		return typeNameReferenced(referenced, schema, name)
	})
}

func enumIdentity(schema, name string) (resolvedSchema, resolvedName string) {
	if strings.TrimSpace(schema) != "" {
		return schema, name
	}
	return splitQualified(name)
}

// keepTypeObjects keeps type objects that are either selected by the scope or
// referenced by kept column types.
func keepTypeObjects[T any](
	s *scopeSelection,
	items []T,
	resourceType string,
	referenced map[string]struct{},
	key func(T) (schema, name string),
) []T {
	return keep(items, func(item T) bool {
		schema, name := key(item)
		if s.selected(typeList(resourceType), schema, name) {
			return true
		}
		return typeNameReferenced(referenced, schema, name)
	})
}

// generatedGrantSelected keeps grants whose target survives the selection.
// Table and sequence grants ride along with their kept target. Schema-level
// grants ride along with the schema universe; when include selectors narrow
// the selection they are kept only for schemas named by --schema.
func (s *scopeSelection) generatedGrantSelected(out *goschema.Database, grant goschema.Grant) bool {
	switch {
	case grant.OnTable != "":
		return generatedTableNameKept(out.Tables, grant.OnTable)
	case grant.OnSequence != "":
		return generatedSequenceNameKept(out.Sequences, grant.OnSequence)
	case grant.OnSchema != "":
		if !s.schemaAllowed(grant.OnSchema) {
			return false
		}
		if len(s.selectors) == 0 {
			return true
		}
		_, named := s.allowed[strings.TrimSpace(grant.OnSchema)]
		return named
	default:
		return false
	}
}

// keepGeneratedSchemas keeps schema declarations that own selected objects,
// so CREATE SCHEMA statements survive for everything the projection kept.
// Without include selectors every universe schema stays.
func (s *scopeSelection) keepGeneratedSchemas(db, out *goschema.Database) []goschema.Schema {
	owning := make(map[string]struct{})
	for _, table := range out.Tables {
		owning[s.effectiveSchema(table.Schema)] = struct{}{}
	}
	for _, sequence := range out.Sequences {
		owning[s.effectiveSchema(sequence.Schema)] = struct{}{}
	}
	for _, domain := range out.Domains {
		owning[s.effectiveSchema(domain.Schema)] = struct{}{}
	}
	for _, composite := range out.CompositeTypes {
		owning[s.effectiveSchema(composite.Schema)] = struct{}{}
	}
	for _, item := range out.Ranges {
		owning[s.effectiveSchema(item.Schema)] = struct{}{}
	}
	for _, view := range out.Views {
		schema, _ := splitQualified(view.Name)
		owning[s.effectiveSchema(schema)] = struct{}{}
	}
	for _, view := range out.MaterializedViews {
		schema, _ := splitQualified(view.Name)
		owning[s.effectiveSchema(schema)] = struct{}{}
	}
	for _, function := range out.Functions {
		schema, _ := splitQualified(function.Name)
		owning[s.effectiveSchema(schema)] = struct{}{}
	}
	for _, enum := range out.Enums {
		schema, _ := enumIdentity(enum.Schema, enum.Name)
		owning[s.effectiveSchema(schema)] = struct{}{}
	}
	return keep(db.Schemas, func(schema goschema.Schema) bool {
		if !s.schemaAllowed(schema.Name) {
			return false
		}
		if len(s.selectors) == 0 {
			return true
		}
		_, ok := owning[strings.TrimSpace(schema.Name)]
		return ok
	})
}

// generatedTableNameKept reports whether name refers to one of the kept
// tables, resolving unqualified references like table annotations do.
func generatedTableNameKept(tables []goschema.Table, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, table := range tables {
		if tableMatchesName(table, name) {
			return true
		}
	}
	return false
}

// sequenceOwnerReference resolves an OWNED BY association ("table.column" or
// "schema.table.column") to its table reference ("table" or "schema.table").
func sequenceOwnerReference(ownedBy string) string {
	parts := strings.Split(strings.TrimSpace(ownedBy), ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], ".")
}

func generatedSequenceNameKept(sequences []goschema.Sequence, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, sequence := range sequences {
		if name == sequence.Name || name == sequence.QualifiedName() {
			return true
		}
	}
	return false
}

func generatedGrantRoleReferenced(grants []goschema.Grant, role string) bool {
	for _, grant := range grants {
		if strings.EqualFold(grant.Role, role) {
			return true
		}
	}
	return false
}

// generatedFieldTypeSet collects lowercase column type spellings of the kept
// fields, so type objects those columns use can be retained as dependencies.
func generatedFieldTypeSet(fields []goschema.Field) map[string]struct{} {
	types := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		normalized := normalizeTypeReference(field.Type)
		if normalized != "" {
			types[normalized] = struct{}{}
		}
	}
	return types
}

// typeNameReferenced reports whether a type object's bare or qualified name
// is used as a kept column type.
func typeNameReferenced(referenced map[string]struct{}, schema, name string) bool {
	if _, ok := referenced[normalizeTypeReference(name)]; ok {
		return true
	}
	qualified := strings.TrimSpace(schema)
	if qualified == "" {
		return false
	}
	_, ok := referenced[normalizeTypeReference(qualified+"."+name)]
	return ok
}

// normalizeTypeReference folds a column type spelling to a comparable form:
// lowercase, without array suffixes or type parameters.
func normalizeTypeReference(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSuffix(value, "[]")
	value, _, _ = strings.Cut(value, "(")
	return strings.TrimSpace(value)
}

// foreignReferenceTableName extracts the table part of a foreign key
// reference such as "users(id)" or "public.users(id)".
func foreignReferenceTableName(reference string) string {
	table, _, _ := strings.Cut(strings.TrimSpace(reference), "(")
	return strings.TrimSpace(table)
}

package atlasfilter

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// projectGenerated applies the schema universe and include selectors to the
// generated-schema IR. Children of a selected table (fields, indexes,
// constraints, triggers, policies, grants, seed data) ride along with it;
// referenced support objects (enums, domains, composite types, ranges,
// sequences owned by selected tables, roles named by selected grants) are
// retained as dependencies even when no selector names them.
func (s *scopeSelection) projectGenerated(db *schemamodel.Database) (*schemamodel.Database, error) {
	out := cloneGenerated(db)
	out.Tables = keep(db.Tables, func(table schemamodel.Table) bool {
		return s.selected(typeList("table"), table.Schema, table.Name)
	})
	keptByStruct := generatedTableByStruct(out.Tables)
	keptTables := make(map[tableIdentity]struct{}, len(out.Tables))
	for _, table := range out.Tables {
		keptTables[s.tableIdentity(table.Schema, table.Name)] = struct{}{}
	}
	var err error
	out.FeatureObjects, out.FeatureCoverage, err = s.bindings.Select(db.FeatureObjects, db.FeatureCoverage, func(schema, table string) bool {
		return s.tableKept(keptTables, schema, table)
	}, s.vocabulary.independentKinds()...)
	if err != nil {
		return nil, err
	}

	out.Fields = keep(db.Fields, func(field schemamodel.Field) bool {
		_, ok := keptByStruct[field.StructName]
		return ok
	})
	features, err := s.selectOwnerFeatures(
		out.FeatureObjects, out.FeatureCoverage, generatedFieldTypeSet(out.Fields),
	)
	if err != nil {
		return nil, err
	}
	out.FeatureObjects, out.FeatureCoverage = features.objects, features.coverage
	out.EmbeddedFields = keep(db.EmbeddedFields, func(field schemamodel.EmbeddedField) bool {
		_, ok := keptByStruct[field.StructName]
		return ok
	})
	out.Indexes = keep(db.Indexes, func(index schemamodel.Index) bool {
		_, ok := generatedIndexTable(keptByStruct, index)
		return ok
	})
	out.Constraints = keep(db.Constraints, func(constraint schemamodel.Constraint) bool {
		_, ok := generatedConstraintTable(keptByStruct, constraint)
		return ok
	})
	out.Triggers = keep(db.Triggers, func(trigger schemamodel.Trigger) bool {
		_, ok := generatedObjectTable(keptByStruct, trigger.StructName, trigger.Table)
		return ok
	})
	out.RLSPolicies = keep(db.RLSPolicies, func(policy schemamodel.RLSPolicy) bool {
		_, ok := generatedObjectTable(keptByStruct, policy.StructName, policy.Table)
		return ok
	})
	out.RLSEnabledTables = keep(db.RLSEnabledTables, func(table schemamodel.RLSEnabledTable) bool {
		_, ok := generatedObjectTable(keptByStruct, table.StructName, table.Table)
		return ok
	})
	out.ManagedData = keep(db.ManagedData, func(data schemamodel.ManagedData) bool {
		return generatedTableNameKept(out.Tables, data.Table)
	})

	s.projectGeneratedTopLevel(db, out, features.roles)
	s.projectGeneratedSupport(db, out)
	extensionSchemas, err := s.projectExtensions(&out.FeatureObjects, &out.FeatureCoverage)
	if err != nil {
		return nil, err
	}
	out.Schemas = s.keepGeneratedSchemas(db, out, slices.Concat(extensionSchemas, features.schemas))

	out.Dependencies = nil
	out.FunctionDependencies = nil
	out.SelfReferencingForeignKeys = nil
	schemamodel.Finalize(out)
	return out, nil
}

// projectGeneratedTopLevel selects independently includable top-level
// resources: views, materialized views, functions, sequences, and roles.
// Views, materialized views, and functions carry their schema in an optional
// "schema." name prefix. Roles are database-scoped and skip the schema
// universe. Extensions are projected after support objects, when the selection
// knows whether a non-extension resource matched.
func (s *scopeSelection) projectGeneratedTopLevel(db, out *schemamodel.Database, featureRoles []string) {
	out.Views = keep(db.Views, func(view schemamodel.View) bool {
		return s.selectedQualifiedName(typeList("view"), view.Name)
	})
	out.MaterializedViews = keep(db.MaterializedViews, func(view schemamodel.MaterializedView) bool {
		return s.selectedQualifiedName(typeList("materialized_view"), view.Name)
	})
	out.Functions = keep(db.Functions, func(function schemamodel.Function) bool {
		return s.selectedQualifiedName(typeList("function"), function.Name)
	})
	out.Sequences = keep(db.Sequences, func(sequence schemamodel.Sequence) bool {
		if s.selected(typeList("sequence"), sequence.Schema, sequence.Name) {
			return true
		}
		return generatedTableNameKept(out.Tables, sequenceOwnerReference(sequence.OwnedBy))
	})
	columnSequences := s.generatedColumnSequences(db, out)
	out.Grants = keep(db.Grants, func(grant schemamodel.Grant) bool {
		return s.generatedGrantSelected(out, columnSequences, grant)
	})
	out.RevokedGrants = keep(db.RevokedGrants, func(grant schemamodel.Grant) bool {
		return s.generatedGrantSelected(out, columnSequences, grant)
	})
	out.Roles = keep(db.Roles, func(role schemamodel.Role) bool {
		if s.selectedNames(typeList("role"), role.Name) {
			return true
		}
		return slices.Contains(featureRoles, role.Name) || generatedGrantRoleReferenced(out.Grants, role.Name)
	})
}

// projectGeneratedSupport retains the enums used by selected tables even when
// no selector names them. An owner's selections keep the domains, composite
// types and ranges kept columns name.
func (s *scopeSelection) projectGeneratedSupport(db, out *schemamodel.Database) {
	referenced := generatedFieldTypeSet(out.Fields)
	out.Enums = keepEnumObjects(s, db.Enums, referenced,
		func(e schemamodel.Enum) (string, string) { return e.Schema, e.Name })
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

// generatedGrantSelected keeps grants whose target survives the selection.
// Table and sequence grants ride along with their kept target, and a grant on
// the sequence that is part of a column with the column's table, as
// columnSequences answers; see [scopeSelection.generatedColumnSequences].
// Schema-level grants ride along with the schema universe; when include
// selectors narrow the selection they are kept only for schemas named by
// --schema.
func (s *scopeSelection) generatedGrantSelected(
	out *schemamodel.Database,
	columnSequences map[tableIdentity]struct{},
	grant schemamodel.Grant,
) bool {
	switch {
	case grant.OnDatabase:
		return s.selectedNames(typeList("role"), grant.Role)
	case grant.OnTable != "":
		return generatedTableNameKept(out.Tables, grant.OnTable)
	case grant.OnSequence != "":
		schema, name := splitQualified(grant.OnSequence)
		return generatedSequenceNameKept(out.Sequences, grant.OnSequence) ||
			s.columnSequenceKept(columnSequences, schema, name)
	case grant.OnRoutine != "":
		// A routine grant rides its routine, which the projection keeps by
		// name, every overload together.
		return slices.ContainsFunc(out.Functions, func(function schemamodel.Function) bool {
			return function.Name == grant.OnRoutine
		})
	case grant.OnObject != "":
		return s.vocabulary.objectGrantKept(out.FeatureObjects, grant.ObjectKind, grant.OnObject)
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
func (s *scopeSelection) keepGeneratedSchemas(db, out *schemamodel.Database, supportSchemas []string) []schemamodel.Schema {
	owning := make(map[string]struct{})
	for _, table := range out.Tables {
		owning[s.effectiveSchema(table.Schema)] = struct{}{}
	}
	for _, sequence := range out.Sequences {
		owning[s.effectiveSchema(sequence.Schema)] = struct{}{}
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
	for _, schema := range supportSchemas {
		owning[schema] = struct{}{}
	}
	return keep(db.Schemas, func(schema schemamodel.Schema) bool {
		if !s.schemaAllowed(schema.Name) {
			return false
		}
		if len(s.selectors) == 0 {
			return true
		}
		_, ok := owning[schema.Name]
		return ok
	})
}

// generatedTableNameKept reports whether name refers to one of the kept
// tables, resolving unqualified references like table annotations do.
func generatedTableNameKept(tables []schemamodel.Table, name string) bool {
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

func generatedSequenceNameKept(sequences []schemamodel.Sequence, name string) bool {
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

func generatedGrantRoleReferenced(grants []schemamodel.Grant, role string) bool {
	for _, grant := range grants {
		if strings.EqualFold(grant.Role, role) {
			return true
		}
	}
	return false
}

// generatedFieldTypeSet collects lowercase column type spellings of the kept
// fields, so type objects those columns use can be retained as dependencies.
func generatedFieldTypeSet(fields []schemamodel.Field) map[string]struct{} {
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

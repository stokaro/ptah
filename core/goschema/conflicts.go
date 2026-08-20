package goschema

import (
	"fmt"
	"path"
	"reflect"
	"strings"

	"go.5x5.cz/ptah/internal/tableref"
)

const ambiguousTableScope = "\x00"

// tableScopeResolver resolves source-specific Go struct associations to stable
// database table identities. Composite sources can describe the same table
// through different struct names, so conflict detection must compare the
// resolved table name rather than parser provenance.
type tableScopeResolver struct {
	byStruct    map[string]string
	byPlainName map[string]string
	byQualified map[string]string
}

func newTableScopeResolver(tables []Table) tableScopeResolver {
	resolver := tableScopeResolver{
		byStruct:    make(map[string]string, len(tables)),
		byPlainName: make(map[string]string, len(tables)),
		byQualified: make(map[string]string, len(tables)),
	}
	for _, table := range tables {
		qualifiedName := table.QualifiedName()
		addTableScope(resolver.byStruct, table.StructName, qualifiedName)
		addTableScope(resolver.byPlainName, table.Name, qualifiedName)
		resolver.byQualified[qualifiedName] = qualifiedName
	}
	return resolver
}

func addTableScope(scopes map[string]string, key, qualifiedName string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	if previous, exists := scopes[key]; exists && previous != qualifiedName {
		scopes[key] = ambiguousTableScope
		return
	}
	scopes[key] = qualifiedName
}

func (r tableScopeResolver) resolve(structName, explicitTable string) string {
	structName = strings.TrimSpace(structName)
	explicitTable = strings.TrimSpace(explicitTable)
	if explicitTable == "" {
		return resolvedTableScope(r.byStruct, structName, structName)
	}
	if qualifiedName, exists := r.byQualified[explicitTable]; exists {
		return qualifiedName
	}
	explicitRef, ok := tableref.Parse(explicitTable)
	if !ok || explicitRef.Qualified {
		return explicitTable
	}
	if qualifiedName := resolvedTableScope(r.byStruct, structName, ""); qualifiedName != "" {
		tableRef, valid := tableref.Parse(qualifiedName)
		if valid && explicitRef.Name == tableRef.Name {
			return qualifiedName
		}
	}
	return resolvedTableScope(r.byPlainName, explicitRef.Name, explicitTable)
}

func resolvedTableScope(scopes map[string]string, key, fallback string) string {
	scope, exists := scopes[key]
	if !exists || scope == ambiguousTableScope {
		return fallback
	}
	return scope
}

// reconcileTableOwners maps source-local Go struct names onto one stable owner
// per database table before sources are flattened. Two packages may both use a
// type named User for different schema-qualified tables, while different source
// formats may use different parser names for the same table. Retaining the raw
// names after flattening would either merge unrelated columns or detach columns
// from the deduplicated table.
func reconcileTableOwners(sources []*Database) error {
	owners := compositeTableOwners(sources)
	for sourceIndex, source := range sources {
		sourceTables, err := sourceTableIdentities(source)
		if err != nil {
			return err
		}
		sourceScope := ""
		if len(sources) > 1 {
			sourceScope = fmt.Sprintf("composite-source-%d", sourceIndex)
		}
		rebindTableOwners(
			source,
			sourceTables,
			owners,
			sourceScope,
		)
	}
	return nil
}

func compositeTableOwners(sources []*Database) map[string]string {
	owners := make(map[string]string)
	tablesByStruct := make(map[string]map[string]struct{})
	for _, source := range sources {
		for _, table := range source.Tables {
			qualifiedName := table.QualifiedName()
			if _, exists := owners[qualifiedName]; !exists {
				owner := strings.TrimSpace(table.StructName)
				if owner == "" {
					owner = qualifiedName
				}
				owners[qualifiedName] = owner
			}
			structName := strings.TrimSpace(table.StructName)
			if structName == "" {
				continue
			}
			if tablesByStruct[structName] == nil {
				tablesByStruct[structName] = make(map[string]struct{})
			}
			tablesByStruct[structName][qualifiedName] = struct{}{}
		}
	}

	for _, tableNames := range tablesByStruct {
		if len(tableNames) < 2 {
			continue
		}
		for tableName := range tableNames {
			owners[tableName] = tableName
		}
	}
	return owners
}

func sourceTableIdentities(source *Database) (map[string]string, error) {
	identities := make(map[string]string, len(source.Tables))
	for _, table := range source.Tables {
		structName := strings.TrimSpace(table.StructName)
		if structName == "" {
			continue
		}
		qualifiedName := table.QualifiedName()
		if previous, exists := identities[structName]; exists && previous != qualifiedName {
			return nil, fmt.Errorf(
				"ambiguous Go struct %q maps to both table %q and table %q within one schema source",
				structName,
				previous,
				qualifiedName,
			)
		}
		identities[structName] = qualifiedName
	}
	return identities, nil
}

func rebindTableOwners(
	source *Database,
	sourceTables,
	owners map[string]string,
	sourceScope string,
) {
	helperTypes := sourceHelperTypes(source, sourceTables)
	ownerFor := func(structName string) (string, bool) {
		tableName, exists := sourceTables[structName]
		if !exists {
			return structName, false
		}
		return owners[tableName], true
	}

	for index := range source.Tables {
		source.Tables[index].StructName, _ = ownerFor(source.Tables[index].StructName)
	}
	for index := range source.Fields {
		field := &source.Fields[index]
		owner, tableOwned := ownerFor(field.StructName)
		if tableOwned {
			field.StructName = owner
			continue
		}
		field.StructName = scopeHelperType(sourceScope, field.StructName, helperTypes)
	}
	for index := range source.Indexes {
		source.Indexes[index].StructName, _ = ownerFor(source.Indexes[index].StructName)
	}
	for index := range source.Constraints {
		source.Constraints[index].StructName, _ = ownerFor(source.Constraints[index].StructName)
	}
	for index := range source.EmbeddedFields {
		field := &source.EmbeddedFields[index]
		embeddedOwner, embeddedTableOwned := ownerFor(field.EmbeddedTypeName)
		if embeddedTableOwned {
			field.EmbeddedTypeName = embeddedOwner
		} else {
			field.EmbeddedTypeName = scopeHelperType(
				sourceScope,
				field.EmbeddedTypeName,
				helperTypes,
			)
		}
		owner, tableOwned := ownerFor(field.StructName)
		if tableOwned {
			field.StructName = owner
			continue
		}
		field.StructName = scopeHelperType(sourceScope, field.StructName, helperTypes)
	}
	for index := range source.Triggers {
		source.Triggers[index].StructName, _ = ownerFor(source.Triggers[index].StructName)
	}
	for index := range source.RLSPolicies {
		source.RLSPolicies[index].StructName, _ = ownerFor(source.RLSPolicies[index].StructName)
	}
	for index := range source.RLSEnabledTables {
		source.RLSEnabledTables[index].StructName, _ = ownerFor(source.RLSEnabledTables[index].StructName)
	}
	for index := range source.Grants {
		source.Grants[index].StructName, _ = ownerFor(source.Grants[index].StructName)
	}
	for index := range source.ManagedData {
		source.ManagedData[index].StructName, _ = ownerFor(source.ManagedData[index].StructName)
	}
}

func sourceHelperTypes(source *Database, sourceTables map[string]string) map[string]struct{} {
	helpers := make(map[string]struct{})
	for _, field := range source.Fields {
		if _, tableOwned := sourceTables[field.StructName]; !tableOwned {
			helpers[field.StructName] = struct{}{}
		}
	}
	for _, field := range source.EmbeddedFields {
		if _, tableOwned := sourceTables[field.StructName]; !tableOwned {
			helpers[field.StructName] = struct{}{}
		}
	}
	return helpers
}

func scopeHelperType(
	sourceScope,
	structName string,
	helperTypes map[string]struct{},
) string {
	if _, exists := helperTypes[structName]; exists {
		return scopedGoTypeIdentity(sourceScope, unscopedGoTypeIdentity(structName))
	}
	return structName
}

func scopedGoTypeIdentity(scope, name string) string {
	if scope == "" {
		return name
	}
	return scope + "\x00" + name
}

func unscopedGoTypeIdentity(name string) string {
	_, unscoped, found := strings.Cut(name, "\x00")
	if found {
		return unscoped
	}
	return name
}

func isCompositeHelperType(name string) bool {
	return strings.Contains(name, "\x00")
}

func validateDuplicateSchemaObjectDefinitions(r *Database) error {
	validator := compositeDefinitionValidator{
		database: r,
		resolver: newTableScopeResolver(r.Tables),
	}
	validations := []func() error{
		validator.schemas,
		validator.tables,
		validator.fields,
		validator.indexes,
		validator.constraints,
		validator.enums,
		validator.embeddedFields,
		validator.extensions,
		validator.functions,
		validator.sequences,
		validator.domains,
		validator.compositeTypes,
		validator.ranges,
		validator.views,
		validator.materializedViews,
		validator.triggers,
		validator.rlsPolicies,
		validator.rlsEnabledTables,
		validator.roles,
		validator.managedData,
	}
	for _, validate := range validations {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

type compositeDefinitionValidator struct {
	database *Database
	resolver tableScopeResolver
}

func (v compositeDefinitionValidator) schemas() error {
	return validateNamedDefinitions(
		v.database.Schemas,
		func(schema Schema) string { return schema.Name },
		func(schema Schema) Schema { return schema },
		func(schema Schema, _ string) error {
			return fmt.Errorf("conflicting schema %q definitions", schema.Name)
		},
	)
}

func (v compositeDefinitionValidator) tables() error {
	return validateNamedDefinitions(
		v.database.Tables,
		func(table Table) string { return table.QualifiedName() },
		canonicalTableDefinition,
		func(_ Table, key string) error {
			return fmt.Errorf("conflicting table %q definitions", key)
		},
	)
}

func (v compositeDefinitionValidator) fields() error {
	return validateNamedDefinitions(
		v.database.Fields,
		func(field Field) string {
			return v.resolver.resolve(field.StructName, "") + "." + field.Name
		},
		canonicalFieldDefinition,
		func(field Field, _ string) error {
			scope := unscopedGoTypeIdentity(v.resolver.resolve(field.StructName, ""))
			return fmt.Errorf("conflicting field %q definitions on table %q", field.Name, scope)
		},
	)
}

func (v compositeDefinitionValidator) indexes() error {
	return validateNamedDefinitions(
		v.database.Indexes,
		func(index Index) string {
			return v.resolver.resolve(index.StructName, index.TableName) + "." + index.Name
		},
		canonicalIndexDefinition,
		func(index Index, _ string) error {
			scope := v.resolver.resolve(index.StructName, index.TableName)
			return fmt.Errorf("conflicting index %q definitions on table %q", index.Name, scope)
		},
	)
}

func (v compositeDefinitionValidator) constraints() error {
	return validateNamedDefinitions(
		v.database.Constraints,
		func(constraint Constraint) string {
			scope := v.resolver.resolve(constraint.StructName, constraint.Table)
			return constraintIdentity(scope, constraint)
		},
		canonicalConstraintDefinition,
		func(constraint Constraint, _ string) error {
			scope := v.resolver.resolve(constraint.StructName, constraint.Table)
			return fmt.Errorf("conflicting constraint %q definitions in scope %q", constraint.Name, scope)
		},
	)
}

func (v compositeDefinitionValidator) enums() error {
	return validateNamedDefinitions(
		v.database.Enums,
		func(enum Enum) string { return enum.Name },
		func(enum Enum) Enum { return enum },
		func(enum Enum, _ string) error {
			return fmt.Errorf("conflicting enum %q definitions", enum.Name)
		},
	)
}

func (v compositeDefinitionValidator) embeddedFields() error {
	return validateNamedDefinitions(
		v.database.EmbeddedFields,
		func(field EmbeddedField) string {
			return v.resolver.resolve(field.StructName, "") + "\x00" + field.EmbeddedTypeName
		},
		canonicalEmbeddedFieldDefinition,
		func(field EmbeddedField, _ string) error {
			scope := unscopedGoTypeIdentity(v.resolver.resolve(field.StructName, ""))
			return fmt.Errorf(
				"conflicting embedded field %q definitions on table %q",
				unscopedGoTypeIdentity(field.EmbeddedTypeName),
				scope,
			)
		},
	)
}

func (v compositeDefinitionValidator) extensions() error {
	return validateNamedDefinitions(
		v.database.Extensions,
		func(extension Extension) string { return extension.Name },
		func(extension Extension) Extension { return extension },
		func(extension Extension, _ string) error {
			return fmt.Errorf("conflicting extension %q definitions", extension.Name)
		},
	)
}

func (v compositeDefinitionValidator) functions() error {
	return validateNamedDefinitions(
		v.database.Functions,
		func(function Function) string { return function.Name },
		canonicalFunctionDefinition,
		func(function Function, _ string) error {
			return fmt.Errorf("conflicting function %q definitions", function.Name)
		},
	)
}

func (v compositeDefinitionValidator) sequences() error {
	return validateNamedDefinitions(
		v.database.Sequences,
		func(sequence Sequence) string { return sequence.QualifiedName() },
		canonicalSequenceDefinition,
		func(_ Sequence, key string) error {
			return fmt.Errorf("conflicting sequence %q definitions", key)
		},
	)
}

func (v compositeDefinitionValidator) domains() error {
	return validateNamedDefinitions(
		v.database.Domains,
		func(domain Domain) string { return domain.QualifiedName() },
		canonicalDomainDefinition,
		func(_ Domain, key string) error {
			return fmt.Errorf("conflicting domain %q definitions", key)
		},
	)
}

func (v compositeDefinitionValidator) compositeTypes() error {
	return validateNamedDefinitions(
		v.database.CompositeTypes,
		func(composite CompositeType) string { return composite.QualifiedName() },
		canonicalCompositeTypeDefinition,
		func(_ CompositeType, key string) error {
			return fmt.Errorf("conflicting composite type %q definitions", key)
		},
	)
}

func (v compositeDefinitionValidator) ranges() error {
	return validateNamedDefinitions(
		v.database.Ranges,
		func(rangeType Range) string { return rangeType.QualifiedName() },
		canonicalRangeDefinition,
		func(_ Range, key string) error {
			return fmt.Errorf("conflicting range %q definitions", key)
		},
	)
}

func (v compositeDefinitionValidator) views() error {
	return validateNamedDefinitions(
		v.database.Views,
		func(view View) string { return view.Name },
		canonicalViewDefinition,
		func(view View, _ string) error {
			return fmt.Errorf("conflicting view %q definitions", view.Name)
		},
	)
}

func (v compositeDefinitionValidator) materializedViews() error {
	return validateNamedDefinitions(
		v.database.MaterializedViews,
		func(view MaterializedView) string { return view.Name },
		canonicalMaterializedViewDefinition,
		func(view MaterializedView, _ string) error {
			return fmt.Errorf("conflicting materialized view %q definitions", view.Name)
		},
	)
}

func (v compositeDefinitionValidator) triggers() error {
	return validateNamedDefinitions(
		v.database.Triggers,
		func(trigger Trigger) string {
			return v.resolver.resolve(trigger.StructName, trigger.Table) + "." + trigger.Name
		},
		canonicalTriggerDefinition,
		func(trigger Trigger, _ string) error {
			scope := v.resolver.resolve(trigger.StructName, trigger.Table)
			return fmt.Errorf("conflicting trigger %q definitions on table %q", trigger.Name, scope)
		},
	)
}

func (v compositeDefinitionValidator) rlsPolicies() error {
	return validateNamedDefinitions(
		v.database.RLSPolicies,
		func(policy RLSPolicy) string {
			return v.resolver.resolve(policy.StructName, policy.Table) + "." + policy.Name
		},
		canonicalRLSPolicyDefinition,
		func(policy RLSPolicy, _ string) error {
			scope := v.resolver.resolve(policy.StructName, policy.Table)
			return fmt.Errorf("conflicting RLS policy %q definitions on table %q", policy.Name, scope)
		},
	)
}

func (v compositeDefinitionValidator) rlsEnabledTables() error {
	return validateNamedDefinitions(
		v.database.RLSEnabledTables,
		func(enabled RLSEnabledTable) string {
			return v.resolver.resolve(enabled.StructName, enabled.Table)
		},
		canonicalRLSEnabledTableDefinition,
		func(enabled RLSEnabledTable, _ string) error {
			scope := v.resolver.resolve(enabled.StructName, enabled.Table)
			return fmt.Errorf("conflicting RLS enablement definitions on table %q", scope)
		},
	)
}

func (v compositeDefinitionValidator) roles() error {
	return validateNamedDefinitions(
		v.database.Roles,
		func(role Role) string { return role.Name },
		canonicalRoleDefinition,
		func(role Role, _ string) error {
			return fmt.Errorf("conflicting role %q definitions", role.Name)
		},
	)
}

func (v compositeDefinitionValidator) managedData() error {
	return validateNamedDefinitions(
		v.database.ManagedData,
		managedDataSourceIdentity,
		canonicalManagedDataDefinition,
		func(data ManagedData, _ string) error {
			return fmt.Errorf(
				"conflicting managed data key definitions for file %q on table %q",
				path.Join(data.SourceDir, data.File),
				QualifyTableName(data.Schema, data.Table),
			)
		},
	)
}

func validateNamedDefinitions[T any](
	definitions []T,
	identity func(T) string,
	canonicalize func(T) T,
	conflict func(T, string) error,
) error {
	seen := make(map[string]T, len(definitions))
	for _, definition := range definitions {
		key := identity(definition)
		canonical := canonicalize(definition)
		if previous, exists := seen[key]; exists && !equivalentDefinitions(previous, canonical) {
			return conflict(definition, key)
		}
		seen[key] = canonical
	}
	return nil
}

func equivalentDefinitions[T any](left, right T) bool {
	return equivalentDefinitionValues(reflect.ValueOf(left), reflect.ValueOf(right))
}

func equivalentDefinitionValues(left, right reflect.Value) bool {
	if !left.IsValid() || !right.IsValid() {
		return left.IsValid() == right.IsValid()
	}
	if left.Type() != right.Type() {
		return false
	}
	switch left.Kind() {
	case reflect.Interface, reflect.Pointer:
		return equivalentIndirectDefinitionValues(left, right)
	case reflect.Slice, reflect.Array:
		return equivalentSequenceDefinitionValues(left, right)
	case reflect.Map:
		return equivalentMapDefinitionValues(left, right)
	case reflect.Struct:
		return equivalentStructDefinitionValues(left, right)
	case reflect.Bool:
		return left.Bool() == right.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return left.Int() == right.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return left.Uint() == right.Uint()
	case reflect.Float32, reflect.Float64:
		return left.Float() == right.Float()
	case reflect.Complex64, reflect.Complex128:
		return left.Complex() == right.Complex()
	case reflect.String:
		return left.String() == right.String()
	case reflect.Chan, reflect.UnsafePointer:
		return left.Pointer() == right.Pointer()
	case reflect.Func:
		return left.IsNil() && right.IsNil()
	}
	return false
}

func equivalentIndirectDefinitionValues(left, right reflect.Value) bool {
	if left.IsNil() || right.IsNil() {
		return left.IsNil() == right.IsNil()
	}
	return equivalentDefinitionValues(left.Elem(), right.Elem())
}

func equivalentSequenceDefinitionValues(left, right reflect.Value) bool {
	if left.Len() != right.Len() {
		return false
	}
	for index := range left.Len() {
		if !equivalentDefinitionValues(left.Index(index), right.Index(index)) {
			return false
		}
	}
	return true
}

func equivalentMapDefinitionValues(left, right reflect.Value) bool {
	if left.Len() != right.Len() {
		return false
	}
	for _, key := range left.MapKeys() {
		rightValue := right.MapIndex(key)
		if !rightValue.IsValid() || !equivalentDefinitionValues(left.MapIndex(key), rightValue) {
			return false
		}
	}
	return true
}

func equivalentStructDefinitionValues(left, right reflect.Value) bool {
	for index := range left.NumField() {
		if !equivalentDefinitionValues(left.Field(index), right.Field(index)) {
			return false
		}
	}
	return true
}

func canonicalTableDefinition(table Table) Table {
	table.StructName = ""
	return table
}

func canonicalFieldDefinition(field Field) Field {
	field.StructName = ""
	field.FieldName = ""
	return field
}

func canonicalIndexDefinition(index Index) Index {
	index.StructName = ""
	index.TableName = ""
	return index
}

func canonicalConstraintDefinition(constraint Constraint) Constraint {
	constraint.StructName = ""
	constraint.Table = ""
	constraint.ForeignColumns = append([]string(nil), constraint.ForeignColumnsOrDefault()...)
	constraint.ForeignColumn = ""
	return constraint
}

func canonicalEmbeddedFieldDefinition(field EmbeddedField) EmbeddedField {
	field.StructName = ""
	return field
}

func canonicalFunctionDefinition(function Function) Function {
	function.StructName = ""
	function.Canonicalize()
	return function
}

func canonicalSequenceDefinition(sequence Sequence) Sequence {
	sequence.StructName = ""
	sequence.Canonicalize()
	return sequence
}

func canonicalDomainDefinition(domain Domain) Domain {
	domain.StructName = ""
	domain.Canonicalize()
	return domain
}

func canonicalCompositeTypeDefinition(composite CompositeType) CompositeType {
	composite.StructName = ""
	composite.Canonicalize()
	return composite
}

func canonicalRangeDefinition(rangeType Range) Range {
	rangeType.StructName = ""
	rangeType.Canonicalize()
	return rangeType
}

func canonicalViewDefinition(view View) View {
	view.StructName = ""
	return view
}

func canonicalMaterializedViewDefinition(view MaterializedView) MaterializedView {
	view.StructName = ""
	return view
}

func canonicalTriggerDefinition(trigger Trigger) Trigger {
	trigger.StructName = ""
	trigger.Table = ""
	trigger.Canonicalize()
	return trigger
}

func canonicalRLSPolicyDefinition(policy RLSPolicy) RLSPolicy {
	policy.StructName = ""
	policy.Table = ""
	return policy
}

func canonicalRLSEnabledTableDefinition(enabled RLSEnabledTable) RLSEnabledTable {
	enabled.StructName = ""
	enabled.Table = ""
	return enabled
}

func canonicalRoleDefinition(role Role) Role {
	role.StructName = ""
	return role
}

func canonicalManagedDataDefinition(data ManagedData) ManagedData {
	data.StructName = ""
	data.Schema = strings.TrimSpace(data.Schema)
	data.Table = strings.TrimSpace(data.Table)
	data.SourceDir = strings.TrimSpace(data.SourceDir)
	data.File = strings.TrimSpace(data.File)
	data.Keys = append([]string(nil), data.Keys...)
	return data
}

func managedDataSourceIdentity(data ManagedData) string {
	return strings.Join([]string{
		strings.TrimSpace(data.Schema),
		strings.TrimSpace(data.Table),
		strings.TrimSpace(data.SourceDir),
		strings.TrimSpace(data.File),
	}, "\x00")
}

func managedDataDefinitionIdentity(data ManagedData) string {
	return managedDataSourceIdentity(data) + "\x00" + strings.Join(data.Keys, "\x00")
}

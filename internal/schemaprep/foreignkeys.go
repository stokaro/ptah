package schemaprep

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// AssignDefaultForeignKeyNames returns a clone whose unnamed foreign keys have
// stable, dialect-valid names. The input database is never mutated.
func AssignDefaultForeignKeyNames(database *schemamodel.Database, targetPlatform string) *schemamodel.Database {
	if database == nil {
		return nil
	}
	assigned := *database
	assigned.Tables = slices.Clone(database.Tables)
	assigned.EmbeddedFields = slices.Clone(database.EmbeddedFields)
	assigned.Fields = fieldsWithEmbeddedMaterializations(database)
	assigned.Fields, assigned.Constraints = assignDefaultForeignKeyNames(
		assigned.Tables,
		assigned.Fields,
		database.Constraints,
		targetPlatform,
	)
	assigned.SelfReferencingForeignKeys = assignedSelfReferencingForeignKeys(
		database.SelfReferencingForeignKeys,
		assigned.Tables,
		assigned.Fields,
	)
	return &assigned
}

// fieldsWithEmbeddedMaterializations accepts both raw parser output and a
// finalized schema. ProcessEmbeddedFields intentionally rebuilds generated
// columns from source declarations, but a merged schema keeps source-only
// helper declarations in Database.EmbeddedSources. Preserve the already
// materialized columns in that case, then add only expansions that are absent.
func fieldsWithEmbeddedMaterializations(database *schemamodel.Database) []schemamodel.Field {
	fields := slices.Clone(database.Fields)
	seen := make(map[fieldIdentity]struct{}, len(fields))
	for _, field := range fields {
		seen[fieldIdentity{structName: field.StructName, name: field.Name}] = struct{}{}
	}
	for _, field := range schemamodel.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields) {
		identity := fieldIdentity{structName: field.StructName, name: field.Name}
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		fields = append(fields, field)
	}
	return fields
}

type fieldIdentity struct {
	structName string
	name       string
}

// WithDefaultFieldForeignKeyName returns field with a stable, dialect-valid
// name when it declares an unnamed foreign key. It leaves the input unchanged.
// Whole-schema callers should prefer [AssignDefaultForeignKeyNames], which also
// resolves collisions across the target's constraint-name scope.
func WithDefaultFieldForeignKeyName(
	tableName string,
	field schemamodel.Field,
	targetPlatform string,
) schemamodel.Field {
	if field.Foreign == "" || field.ForeignKeyName != "" {
		return field
	}
	base := defaultFieldForeignKeyName(tableName, field)
	field.ForeignKeyName = generatedForeignKeyName(base, fieldForeignKeyIdentity(field), targetPlatform)
	return field
}

// WithDefaultConstraintForeignKeyName returns constraint with a stable,
// dialect-valid name when it is an unnamed foreign-key constraint. It leaves
// the input unchanged. Whole-schema callers should prefer
// [AssignDefaultForeignKeyNames] so collisions are allocated together.
func WithDefaultConstraintForeignKeyName(
	tableName string,
	constraint schemamodel.Constraint,
	targetPlatform string,
) schemamodel.Constraint {
	if !IsForeignKeyConstraint(constraint) || constraint.Name != "" {
		return constraint
	}
	base := defaultConstraintForeignKeyName(tableName, constraint)
	constraint.Name = generatedForeignKeyName(base, constraintForeignKeyIdentity(constraint), targetPlatform)
	return constraint
}

func assignedSelfReferencingForeignKeys(
	foreignKeys map[string][]schemamodel.SelfReferencingFK,
	tables []schemamodel.Table,
	fields []schemamodel.Field,
) map[string][]schemamodel.SelfReferencingFK {
	if foreignKeys == nil {
		return nil
	}
	assigned := make(map[string][]schemamodel.SelfReferencingFK, len(foreignKeys))
	for tableName, tableForeignKeys := range foreignKeys {
		cloned := slices.Clone(tableForeignKeys)
		table := foreignKeyOwnerTable(tables, tableName)
		for i := range cloned {
			cloned[i].ForeignKeyName = assignedSelfReferencingForeignKeyName(table, cloned[i], fields)
		}
		assigned[tableName] = cloned
	}
	return assigned
}

func foreignKeyOwnerTable(tables []schemamodel.Table, tableName string) *schemamodel.Table {
	for i := range tables {
		if tables[i].QualifiedName() == tableName || tables[i].Name == tableName {
			return &tables[i]
		}
	}
	return nil
}

func assignedSelfReferencingForeignKeyName(
	table *schemamodel.Table,
	foreignKey schemamodel.SelfReferencingFK,
	fields []schemamodel.Field,
) string {
	if table == nil {
		return foreignKey.ForeignKeyName
	}
	for _, field := range fields {
		if field.StructName == table.StructName && field.Name == foreignKey.FieldName && field.Foreign == foreignKey.Foreign {
			return field.ForeignKeyName
		}
	}
	return foreignKey.ForeignKeyName
}

func assignDefaultForeignKeyNames(
	tables []schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
	targetPlatform string,
) ([]schemamodel.Field, []schemamodel.Constraint) {
	assignedFields := slices.Clone(fields)
	assignedConstraints := slices.Clone(constraints)
	allocations := make(map[string]*foreignKeyNameAllocation)
	for _, table := range tables {
		reserved, counts := foreignKeyNameReservations(table, assignedFields, assignedConstraints, targetPlatform)
		scope := foreignKeyNameAllocationScope(table, targetPlatform)
		allocation := allocations[scope]
		if allocation == nil {
			allocation = &foreignKeyNameAllocation{allocated: make(map[string]struct{}), counts: make(map[string]int)}
			allocations[scope] = allocation
		}
		maps.Copy(allocation.allocated, reserved)
		for name, count := range counts {
			allocation.counts[name] += count
		}
	}
	for _, table := range tables {
		allocation := allocations[foreignKeyNameAllocationScope(table, targetPlatform)]
		assignFieldForeignKeyNames(table, assignedFields, allocation.counts, allocation.allocated, targetPlatform)
		assignConstraintForeignKeyNames(table, assignedConstraints, allocation.counts, allocation.allocated, targetPlatform)
	}
	return assignedFields, assignedConstraints
}

type foreignKeyNameAllocation struct {
	allocated map[string]struct{}
	counts    map[string]int
}

func foreignKeyNameAllocationScope(table schemamodel.Table, targetPlatform string) string {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB:
		return "database"
	case platform.SQLServer, platform.Spanner:
		schema := strings.TrimSpace(table.Schema)
		if schema == "" {
			schema = identifier.ForDialect(targetPlatform).DefaultSchema
		}
		return "schema:" + strings.ToLower(schema)
	default:
		return "table:" + table.QualifiedName()
	}
}

func foreignKeyNameReservations(
	table schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
	targetPlatform string,
) (map[string]struct{}, map[string]int) {
	reserved := make(map[string]struct{})
	counts := make(map[string]int)
	for _, field := range fields {
		if field.StructName != table.StructName || field.Foreign == "" {
			continue
		}
		if field.ForeignKeyName != "" && !field.GeneratedFromEmbedded {
			reserved[foreignKeyNameReservationKey(field.ForeignKeyName, targetPlatform)] = struct{}{}
			continue
		}
		base := defaultFieldForeignKeyName(table.Name, field)
		candidate := generatedForeignKeyName(base, fieldForeignKeyIdentity(field), targetPlatform)
		counts[foreignKeyNameReservationKey(candidate, targetPlatform)]++
	}
	for _, constraint := range constraints {
		if !IsForeignKeyConstraint(constraint) || !ConstraintBelongsToTable(constraint, table) {
			continue
		}
		if constraint.Name != "" {
			reserved[foreignKeyNameReservationKey(constraint.Name, targetPlatform)] = struct{}{}
			continue
		}
		base := defaultConstraintForeignKeyName(table.Name, constraint)
		candidate := generatedForeignKeyName(base, constraintForeignKeyIdentity(constraint), targetPlatform)
		counts[foreignKeyNameReservationKey(candidate, targetPlatform)]++
	}
	return reserved, counts
}

func assignFieldForeignKeyNames(
	table schemamodel.Table,
	fields []schemamodel.Field,
	counts map[string]int,
	allocated map[string]struct{},
	targetPlatform string,
) {
	for i := range fields {
		field := &fields[i]
		if field.StructName != table.StructName || field.Foreign == "" ||
			(field.ForeignKeyName != "" && !field.GeneratedFromEmbedded) {
			continue
		}
		base := defaultFieldForeignKeyName(table.Name, *field)
		candidate := generatedForeignKeyName(base, fieldForeignKeyIdentity(*field), targetPlatform)
		field.ForeignKeyName = allocateForeignKeyName(
			base,
			counts[foreignKeyNameReservationKey(candidate, targetPlatform)],
			fieldForeignKeyIdentity(*field),
			allocated,
			targetPlatform,
		)
	}
}

func defaultFieldForeignKeyName(tableName string, field schemamodel.Field) string {
	if field.GeneratedFromEmbedded && field.ForeignKeyName != "" {
		return field.ForeignKeyName
	}
	return GenerateForeignKeyName(tableName, field.Name)
}

func assignConstraintForeignKeyNames(
	table schemamodel.Table,
	constraints []schemamodel.Constraint,
	counts map[string]int,
	allocated map[string]struct{},
	targetPlatform string,
) {
	for i := range constraints {
		constraint := &constraints[i]
		if constraint.Name != "" || !IsForeignKeyConstraint(*constraint) || !ConstraintBelongsToTable(*constraint, table) {
			continue
		}
		base := defaultConstraintForeignKeyName(table.Name, *constraint)
		candidate := generatedForeignKeyName(base, constraintForeignKeyIdentity(*constraint), targetPlatform)
		constraint.Name = allocateForeignKeyName(
			base,
			counts[foreignKeyNameReservationKey(candidate, targetPlatform)],
			constraintForeignKeyIdentity(*constraint),
			allocated,
			targetPlatform,
		)
	}
}

func defaultConstraintForeignKeyName(tableName string, constraint schemamodel.Constraint) string {
	columnName := strings.Join(constraint.Columns, "_")
	if columnName == "" {
		columnName = "foreign_key"
	}
	return GenerateForeignKeyName(tableName, columnName)
}

func fieldForeignKeyIdentity(field schemamodel.Field) string {
	return strings.Join([]string{"field", field.StructName, field.Name, field.Foreign, field.OnDelete, field.OnUpdate}, "\x00")
}

func constraintForeignKeyIdentity(constraint schemamodel.Constraint) string {
	return strings.Join([]string{
		"constraint",
		constraint.StructName,
		constraint.Table,
		strings.Join(constraint.Columns, ","),
		constraint.ForeignTable,
		strings.Join(constraint.ForeignColumnsOrDefault(), ","),
		constraint.OnDelete,
		constraint.OnUpdate,
	}, "\x00")
}

func allocateForeignKeyName(
	base string,
	candidateCount int,
	identity string,
	allocated map[string]struct{},
	targetPlatform string,
) string {
	candidate := generatedForeignKeyName(base, identity, targetPlatform)
	reservationKey := foreignKeyNameReservationKey(candidate, targetPlatform)
	if _, conflict := allocated[reservationKey]; candidateCount == 1 && !conflict {
		allocated[reservationKey] = struct{}{}
		return candidate
	}
	suffix := foreignKeyNameHashSuffix(base, identity)
	candidate = foreignKeyNameWithSuffix(base, suffix, targetPlatform)
	for ordinal := 2; ; ordinal++ {
		reservationKey = foreignKeyNameReservationKey(candidate, targetPlatform)
		if _, conflict := allocated[reservationKey]; !conflict {
			allocated[reservationKey] = struct{}{}
			return candidate
		}
		candidate = foreignKeyNameWithSuffix(base, suffix+fmt.Sprintf("_%d", ordinal), targetPlatform)
	}
}

func foreignKeyNameReservationKey(name, targetPlatform string) string {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB, platform.SQLServer, platform.Spanner:
		return strings.ToLower(name)
	default:
		return name
	}
}

func generatedForeignKeyName(base, identity, targetPlatform string) string {
	if foreignKeyNameFits(base, targetPlatform) {
		return base
	}
	return foreignKeyNameWithSuffix(base, foreignKeyNameHashSuffix(base, identity), targetPlatform)
}

func foreignKeyNameHashSuffix(base, identity string) string {
	digest := sha256.Sum256([]byte(base + "\x00" + identity))
	return fmt.Sprintf("_%x", digest[:4])
}

func foreignKeyNameFits(name, targetPlatform string) bool {
	switch {
	case platform.NormalizeDialect(targetPlatform) == platform.SQLServer,
		platform.NormalizeDialect(targetPlatform) == platform.Spanner:
		return len([]rune(name)) <= 128
	case platform.IsPostgresFamily(targetPlatform):
		return len(name) <= 63
	case isMySQLFamilyTarget(targetPlatform):
		return len([]rune(name)) <= 64
	default:
		return true
	}
}

func foreignKeyNameWithSuffix(base, suffix, targetPlatform string) string {
	switch {
	case platform.NormalizeDialect(targetPlatform) == platform.SQLServer,
		platform.NormalizeDialect(targetPlatform) == platform.Spanner:
		return truncateIdentifierCharacters(base, 128-len([]rune(suffix))) + suffix
	case platform.IsPostgresFamily(targetPlatform):
		return truncateIdentifierBytes(base, 63-len(suffix)) + suffix
	case isMySQLFamilyTarget(targetPlatform):
		return truncateIdentifierCharacters(base, 64-len([]rune(suffix))) + suffix
	default:
		return base + suffix
	}
}

func truncateIdentifierBytes(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	end := 0
	for index := range value {
		if index > maxBytes {
			break
		}
		end = index
	}
	return value[:end]
}

func truncateIdentifierCharacters(value string, maxCharacters int) string {
	runes := []rune(value)
	if len(runes) <= maxCharacters {
		return value
	}
	return string(runes[:maxCharacters])
}

func isMySQLFamilyTarget(targetPlatform string) bool {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

package dbschematogo

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

// ConvertDBSchemaToGoSchema converts a database schema to goschema format
// This is needed for down migrations where we use the current DB state as the target
func ConvertDBSchemaToGoSchema(dbSchema *dbschematypes.DBSchema) *goschema.Database {
	database := newDatabase()
	convertEnums(database, dbSchema.Enums)

	// Index single-column FOREIGN KEY constraints by table.column so the
	// reconstructed fields can carry the foreign reference and its referential
	// actions. This is what lets a down migration restore the prior ON DELETE /
	// ON UPDATE action of a field-level FK (issue #189): the down path treats
	// the introspected (pre-change) database as the target, so the old action
	// must survive the round-trip into goschema.
	fkByColumn := indexForeignKeysByColumn(dbSchema)
	primaryKeysByTable := compositePrimaryKeysByTable(dbSchema)
	compositePKColumns := compositePrimaryKeyColumns(primaryKeysByTable)
	tableStructNames := convertTablesAndFields(database, dbSchema, fkByColumn, primaryKeysByTable, compositePKColumns)

	database.Indexes = convertIndexes(dbSchema, tableStructNames)
	database.Constraints = convertConstraints(dbSchema, tableStructNames)
	convertExtensions(database, dbSchema.Extensions)
	convertRLSPolicies(database, dbSchema.RLSPolicies, tableStructNames)
	convertFunctions(database, dbSchema.Functions)
	convertViews(database, dbSchema.Views)
	convertMaterializedViews(database, dbSchema.MatViews)
	convertTriggers(database, dbSchema.Triggers)
	convertRoles(database, dbSchema.Roles)
	database.Grants = convertGrants(dbSchema.Grants)
	convertRLSEnabledTables(database, dbSchema.Tables, tableStructNames)

	return database
}

func newDatabase() *goschema.Database {
	return &goschema.Database{
		Tables:            make([]goschema.Table, 0),
		Fields:            make([]goschema.Field, 0),
		Indexes:           make([]goschema.Index, 0),
		Constraints:       make([]goschema.Constraint, 0),
		Enums:             make([]goschema.Enum, 0),
		Extensions:        make([]goschema.Extension, 0),
		Functions:         make([]goschema.Function, 0),
		Views:             make([]goschema.View, 0),
		MaterializedViews: make([]goschema.MaterializedView, 0),
		Triggers:          make([]goschema.Trigger, 0),
		RLSPolicies:       make([]goschema.RLSPolicy, 0),
		RLSEnabledTables:  make([]goschema.RLSEnabledTable, 0),
		Roles:             make([]goschema.Role, 0),
		Grants:            make([]goschema.Grant, 0),
		Dependencies:      make(map[string][]string),
	}
}

func convertEnums(database *goschema.Database, dbEnums []dbschematypes.DBEnum) {
	for _, dbEnum := range dbEnums {
		database.Enums = append(database.Enums, goschema.Enum{
			Name:   dbEnum.Name,
			Values: dbEnum.Values,
		})
	}
}

func convertTablesAndFields(
	database *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	fkByColumn map[string]foreignKeyInfo,
	primaryKeysByTable map[string][]string,
	compositePKColumns map[string]map[string]bool,
) map[string]string {
	tableStructNames := make(map[string]string, len(dbSchema.Tables))
	for _, dbTable := range dbSchema.Tables {
		// Generate struct name from table name (simple conversion)
		structName := generateStructName(dbTable.Name)
		tableStructNames[dbTable.QualifiedName()] = structName

		table := goschema.Table{
			StructName: structName,
			Name:       dbTable.Name,
			Schema:     dbTable.Schema,
			Comment:    dbTable.Comment,
			PrimaryKey: primaryKeysByTable[dbTable.QualifiedName()],
		}
		database.Tables = append(database.Tables, table)

		// Convert columns to fields
		for _, dbColumn := range dbTable.Columns {
			field := goschema.Field{
				StructName:    structName,
				FieldName:     generateFieldName(dbColumn.Name),
				Name:          dbColumn.Name,
				Type:          goSchemaFieldType(dbColumn),
				Nullable:      dbColumn.IsNullable == "YES",
				Primary:       dbColumn.IsPrimaryKey && !compositePKColumns[dbTable.QualifiedName()][dbColumn.Name],
				AutoInc:       dbColumn.IsAutoIncrement,
				Unique:        dbColumn.IsUnique,
				Charset:       dbColumn.Charset,
				Collate:       dbColumn.Collate,
				GeneratedKind: dbColumn.GeneratedKind,
			}
			if dbColumn.GeneratedExpression != nil {
				field.GeneratedExpression = *dbColumn.GeneratedExpression
			}

			if dbColumn.ColumnDefault != nil {
				setFieldDefaultFromDB(&field, *dbColumn.ColumnDefault)
			}

			// Carry the field-level foreign key (reference + referential actions)
			// so down migrations can reconstruct it with the prior action.
			if fk, ok := fkByColumn[dbTable.QualifiedName()+"."+dbColumn.Name]; ok {
				field.Foreign = fk.foreign
				field.ForeignKeyName = fk.name
				field.OnDelete = fk.onDelete
				field.OnUpdate = fk.onUpdate
			}

			database.Fields = append(database.Fields, field)
		}
	}
	return tableStructNames
}

func convertIndexes(dbSchema *dbschematypes.DBSchema, tableStructNames map[string]string) []goschema.Index {
	constraintBackedIndexes := constraintBackedIndexesByTable(dbSchema)
	indexes := make([]goschema.Index, 0, len(dbSchema.Indexes))
	for _, dbIndex := range dbSchema.Indexes {
		// Skip primary key indexes as they're handled by primary key fields
		if dbIndex.IsPrimary {
			continue
		}
		if _, ok := constraintBackedIndexes[dbIndex.QualifiedTableName()+"."+dbIndex.Name]; ok {
			continue
		}

		index := goschema.Index{
			StructName:    structNameForTable(tableStructNames, dbIndex.QualifiedTableName(), dbIndex.TableName),
			Name:          dbIndex.Name,
			TableName:     dbIndex.QualifiedTableName(),
			Fields:        dbIndex.Columns,
			Unique:        dbIndex.IsUnique,
			Condition:     dbIndex.Condition,
			NullsDistinct: cloneBoolPtr(dbIndex.NullsDistinct),
			Type:          dbIndex.Type,
			Granularity:   dbIndex.Granularity,
		}
		indexes = append(indexes, index)
	}
	return indexes
}

func convertExtensions(database *goschema.Database, dbExtensions []dbschematypes.DBExtension) {
	for _, dbExtension := range dbExtensions {
		extension := goschema.Extension{
			Name:        dbExtension.Name,
			IfNotExists: true, // Default to true for down migrations for safety
			Version:     dbExtension.Version,
		}

		// Set comment if available
		if dbExtension.Comment != nil {
			extension.Comment = *dbExtension.Comment
		}

		database.Extensions = append(database.Extensions, extension)
	}
}

func convertRLSPolicies(
	database *goschema.Database,
	dbPolicies []dbschematypes.DBRLSPolicy,
	tableStructNames map[string]string,
) {
	for _, dbPolicy := range dbPolicies {
		policy := goschema.RLSPolicy{
			StructName:          structNameForTable(tableStructNames, dbPolicy.Table, dbPolicy.Table),
			Name:                dbPolicy.Name,
			Table:               dbPolicy.Table,
			PolicyFor:           dbPolicy.PolicyFor,
			ToRoles:             dbPolicy.ToRoles,
			UsingExpression:     dbPolicy.UsingExpression,
			WithCheckExpression: dbPolicy.WithCheckExpression,
			Comment:             dbPolicy.Comment,
		}
		database.RLSPolicies = append(database.RLSPolicies, policy)
	}
}

func convertFunctions(database *goschema.Database, dbFunctions []dbschematypes.DBFunction) {
	for _, dbFunction := range dbFunctions {
		function := goschema.Function{
			StructName: "", // Functions are not associated with specific structs in DB schema
			Name:       dbFunction.Name,
			Parameters: dbFunction.Parameters,
			Returns:    dbFunction.Returns,
			Language:   dbFunction.Language,
			Security:   dbFunction.Security,
			Volatility: dbFunction.Volatility,
			Body:       dbFunction.Body,
			Comment:    dbFunction.Comment,
		}
		database.Functions = append(database.Functions, function)
	}
}

func convertViews(database *goschema.Database, dbViews []dbschematypes.DBView) {
	for _, dbView := range dbViews {
		database.Views = append(database.Views, goschema.View{
			Name:      dbView.QualifiedName(),
			Body:      dbView.Body,
			WithCheck: strings.EqualFold(dbView.CheckOption, "LOCAL") || strings.EqualFold(dbView.CheckOption, "CASCADED"),
			Comment:   dbView.Comment,
		})
	}
}

func convertMaterializedViews(database *goschema.Database, dbViews []dbschematypes.DBMatView) {
	for _, dbView := range dbViews {
		materializedView := goschema.MaterializedView{
			Name:            dbView.QualifiedName(),
			Body:            dbView.Body,
			RefreshStrategy: dbView.RefreshStrategy,
			Comment:         dbView.Comment,
		}
		materializedView.Canonicalize()
		database.MaterializedViews = append(database.MaterializedViews, materializedView)
	}
}

func convertTriggers(database *goschema.Database, dbTriggers []dbschematypes.DBTrigger) {
	for _, dbTrigger := range dbTriggers {
		trigger := goschema.Trigger{
			Name:    dbTrigger.Name,
			Table:   dbTrigger.QualifiedTable(),
			Timing:  dbTrigger.Timing,
			Event:   dbTrigger.Event,
			ForEach: dbTrigger.ForEach,
			Body:    dbTrigger.Body,
			Comment: dbTrigger.Comment,
		}
		trigger.Canonicalize()
		database.Triggers = append(database.Triggers, trigger)
	}
}

func convertRoles(database *goschema.Database, dbRoles []dbschematypes.DBRole) {
	for _, dbRole := range dbRoles {
		role := goschema.Role{
			StructName:  "", // Roles are not associated with specific structs in DB schema
			Name:        dbRole.Name,
			Login:       dbRole.Login,
			Password:    "", // Not available in current DBRole for security
			Superuser:   dbRole.Superuser,
			CreateDB:    dbRole.CreateDB,
			CreateRole:  dbRole.CreateRole,
			Inherit:     dbRole.Inherit,
			Replication: dbRole.Replication,
			Comment:     dbRole.Comment,
		}
		database.Roles = append(database.Roles, role)
	}
}

func convertRLSEnabledTables(
	database *goschema.Database,
	dbTables []dbschematypes.DBTable,
	tableStructNames map[string]string,
) {
	for _, dbTable := range dbTables {
		if dbTable.RLSEnabled {
			rlsEnabledTable := goschema.RLSEnabledTable{
				StructName: structNameForTable(tableStructNames, dbTable.QualifiedName(), dbTable.Name),
				Table:      dbTable.Name,
				Comment:    "", // Comment not available in DBTable for RLS enablement
			}
			database.RLSEnabledTables = append(database.RLSEnabledTables, rlsEnabledTable)
		}
	}
}

func compositePrimaryKeyColumns(primaryKeysByTable map[string][]string) map[string]map[string]bool {
	result := make(map[string]map[string]bool, len(primaryKeysByTable))
	for tableName, columns := range primaryKeysByTable {
		columnSet := make(map[string]bool, len(columns))
		for _, column := range columns {
			columnSet[column] = true
		}
		result[tableName] = columnSet
	}
	return result
}

func compositePrimaryKeysByTable(dbSchema *dbschematypes.DBSchema) map[string][]string {
	result := make(map[string][]string)
	for _, constraint := range dbSchema.Constraints {
		if !strings.EqualFold(constraint.Type, "PRIMARY KEY") {
			continue
		}
		columns := constraint.ColumnNamesOrDefault()
		if len(columns) <= 1 {
			continue
		}
		result[constraint.QualifiedTableName()] = columns
	}
	return result
}

func convertConstraints(dbSchema *dbschematypes.DBSchema, tableStructNames map[string]string) []goschema.Constraint {
	constraints := make([]goschema.Constraint, 0, len(dbSchema.Constraints))
	for _, dbConstraint := range dbSchema.Constraints {
		constraint, ok := convertConstraint(dbConstraint, tableStructNames)
		if ok {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

func convertConstraint(dbConstraint dbschematypes.DBConstraint, tableStructNames map[string]string) (goschema.Constraint, bool) {
	constraintType := strings.ToUpper(dbConstraint.Type)
	columns := dbConstraint.ColumnNamesOrDefault()
	switch constraintType {
	case "PRIMARY KEY":
		return goschema.Constraint{}, false
	case "FOREIGN KEY":
		if len(columns) <= 1 {
			return goschema.Constraint{}, false
		}
	case "UNIQUE":
		if len(columns) <= 1 {
			return goschema.Constraint{}, false
		}
	case "CHECK":
		if dbConstraint.CheckClause == nil || strings.TrimSpace(*dbConstraint.CheckClause) == "" {
			return goschema.Constraint{}, false
		}
		if isPostgresSyntheticNotNullCheck(dbConstraint) {
			return goschema.Constraint{}, false
		}
	case "EXCLUDE":
		if dbConstraint.UsingMethod == nil || dbConstraint.ExcludeElements == nil {
			return goschema.Constraint{}, false
		}
	default:
		return goschema.Constraint{}, false
	}

	return goschema.Constraint{
		StructName:      structNameForTable(tableStructNames, dbConstraint.QualifiedTableName(), dbConstraint.TableName),
		Name:            dbConstraint.Name,
		Type:            constraintType,
		Table:           dbConstraint.QualifiedTableName(),
		UsingMethod:     derefString(dbConstraint.UsingMethod),
		ExcludeElements: derefString(dbConstraint.ExcludeElements),
		WhereCondition:  derefString(dbConstraint.WhereCondition),
		CheckExpression: derefString(dbConstraint.CheckClause),
		Columns:         columns,
		IncludeColumns:  append([]string(nil), dbConstraint.IncludeColumns...),
		NullsDistinct:   cloneBoolPtr(dbConstraint.NullsDistinct),
		ForeignTable:    dbConstraint.QualifiedForeignTableName(),
		ForeignColumn:   firstString(dbConstraint.ForeignColumnsOrDefault()),
		ForeignColumns:  dbConstraint.ForeignColumnsOrDefault(),
		OnDelete:        derefString(dbConstraint.DeleteRule),
		OnUpdate:        derefString(dbConstraint.UpdateRule),
	}, true
}

func constraintBackedIndexesByTable(dbSchema *dbschematypes.DBSchema) map[string]struct{} {
	result := make(map[string]struct{}, len(dbSchema.Constraints))
	for _, constraint := range dbSchema.Constraints {
		switch strings.ToUpper(constraint.Type) {
		case "PRIMARY KEY", "UNIQUE", "EXCLUDE":
			result[constraint.QualifiedTableName()+"."+constraint.Name] = struct{}{}
		}
	}
	return result
}

func isPostgresSyntheticNotNullCheck(constraint dbschematypes.DBConstraint) bool {
	if constraint.CheckClause == nil || !strings.HasSuffix(constraint.Name, "_not_null") {
		return false
	}
	checkClause := strings.TrimSpace(strings.ToUpper(*constraint.CheckClause))
	if !strings.HasSuffix(checkClause, " IS NOT NULL") {
		return false
	}
	return strings.Count(checkClause, " IS NOT NULL") == 1
}

func structNameForTable(tableStructNames map[string]string, qualifiedTableName, fallbackTableName string) string {
	if structName, ok := tableStructNames[qualifiedTableName]; ok {
		return structName
	}
	return generateStructName(fallbackTableName)
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func goSchemaFieldType(dbColumn dbschematypes.DBColumn) string {
	if strings.EqualFold(dbColumn.DataType, "USER-DEFINED") && dbColumn.UDTName != "" {
		return dbColumn.UDTName
	}
	if dbColumn.ColumnType != "" {
		return dbColumn.ColumnType
	}
	if sizedType := sizedColumnType(dbColumn); sizedType != "" {
		return sizedType
	}
	return dbColumn.DataType
}

func sizedColumnType(dbColumn dbschematypes.DBColumn) string {
	dataType := strings.ToLower(strings.TrimSpace(dbColumn.DataType))
	switch dataType {
	case "character varying", "varchar":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("VARCHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "character", "char":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("CHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "numeric", "decimal":
		if dbColumn.NumericPrecision != nil && dbColumn.NumericScale != nil {
			return fmt.Sprintf("NUMERIC(%d,%d)", *dbColumn.NumericPrecision, *dbColumn.NumericScale)
		}
		if dbColumn.NumericPrecision != nil {
			return fmt.Sprintf("NUMERIC(%d)", *dbColumn.NumericPrecision)
		}
	}
	return ""
}

func setFieldDefaultFromDB(field *goschema.Field, defaultSQL string) {
	if dbDefaultLooksLikeExpression(defaultSQL) {
		field.DefaultExpr = defaultSQL
		return
	}
	field.Default = defaultSQL
}

func dbDefaultLooksLikeExpression(defaultSQL string) bool {
	value := strings.TrimSpace(defaultSQL)
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return false
	}
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return false
	}
	return true
}

func convertGrants(dbGrants []dbschematypes.DBGrant) []goschema.Grant {
	grants := make([]goschema.Grant, 0, len(dbGrants))
	for _, dbGrant := range dbGrants {
		grant := goschema.Grant{
			Role:       dbGrant.Role,
			Privileges: []string{dbGrant.Privilege},
			WithOption: dbGrant.WithOption,
			GrantedBy:  dbGrant.GrantedBy,
		}
		if strings.EqualFold(dbGrant.ObjectType, "SCHEMA") {
			grant.OnSchema = dbGrant.ObjectName
		} else {
			grant.OnTable = dbGrant.QualifiedTarget()
		}
		grant.Canonicalize()
		grants = append(grants, grant)
	}
	return grants
}

// foreignKeyInfo holds the field-level pieces reconstructed from a database
// FOREIGN KEY constraint.
type foreignKeyInfo struct {
	name     string // constraint name
	foreign  string // "table(column)" reference
	onDelete string // ON DELETE action (NO ACTION normalized away later)
	onUpdate string // ON UPDATE action
}

// indexForeignKeysByColumn maps table.column -> reconstructed FK info for every
// single-column FOREIGN KEY constraint in the database schema. Multi-column FKs
// are not field-level and are skipped (they are represented as table-level
// constraints, which this converter does not yet round-trip).
func indexForeignKeysByColumn(dbSchema *dbschematypes.DBSchema) map[string]foreignKeyInfo {
	result := make(map[string]foreignKeyInfo)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" || c.ColumnName == "" || c.ForeignTable == nil || len(c.ColumnNamesOrDefault()) != 1 {
			continue
		}
		foreignTable := c.QualifiedForeignTableName()
		foreignColumn := ""
		if foreignColumns := c.ForeignColumnsOrDefault(); len(foreignColumns) == 1 {
			foreignColumn = foreignColumns[0]
		}
		foreign := foreignTable
		if foreignColumn != "" {
			foreign = foreignTable + "(" + foreignColumn + ")"
		}
		result[c.QualifiedTableName()+"."+c.ColumnName] = foreignKeyInfo{
			name:     c.Name,
			foreign:  foreign,
			onDelete: derefString(c.DeleteRule),
			onUpdate: derefString(c.UpdateRule),
		}
	}
	return result
}

// derefString returns the pointed-to string or "" when nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// generateStructName converts a table name to a Go struct name
func generateStructName(tableName string) string {
	// Simple conversion: remove underscores and capitalize
	parts := strings.Split(tableName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}

// generateFieldName converts a column name to a Go field name
func generateFieldName(columnName string) string {
	// Simple conversion: remove underscores and capitalize
	parts := strings.Split(columnName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}

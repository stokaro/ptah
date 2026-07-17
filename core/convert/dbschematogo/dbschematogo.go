package dbschematogo

import (
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

// ConvertDBSchemaToGoSchema converts a database schema to goschema format
// This is needed for down migrations where we use the current DB state as the target
func ConvertDBSchemaToGoSchema(dbSchema *dbschematypes.DBSchema) *goschema.Database {
	database := &goschema.Database{
		Tables:            make([]goschema.Table, 0),
		Fields:            make([]goschema.Field, 0),
		Indexes:           make([]goschema.Index, 0),
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

	// Convert enums
	for _, dbEnum := range dbSchema.Enums {
		database.Enums = append(database.Enums, goschema.Enum{
			Name:   dbEnum.Name,
			Values: dbEnum.Values,
		})
	}

	// Index single-column FOREIGN KEY constraints by table.column so the
	// reconstructed fields can carry the foreign reference and its referential
	// actions. This is what lets a down migration restore the prior ON DELETE /
	// ON UPDATE action of a field-level FK (issue #189): the down path treats
	// the introspected (pre-change) database as the target, so the old action
	// must survive the round-trip into goschema.
	fkByColumn := indexForeignKeysByColumn(dbSchema)

	// Convert tables and their columns
	for _, dbTable := range dbSchema.Tables {
		// Generate struct name from table name (simple conversion)
		structName := generateStructName(dbTable.Name)

		table := goschema.Table{
			StructName: structName,
			Name:       dbTable.Name,
			Schema:     dbTable.Schema,
			Comment:    dbTable.Comment,
		}
		database.Tables = append(database.Tables, table)

		// Convert columns to fields
		for _, dbColumn := range dbTable.Columns {
			field := goschema.Field{
				StructName:    structName,
				FieldName:     generateFieldName(dbColumn.Name),
				Name:          dbColumn.Name,
				Type:          dbColumn.DataType,
				Nullable:      dbColumn.IsNullable == "YES",
				Primary:       dbColumn.IsPrimaryKey,
				AutoInc:       dbColumn.IsAutoIncrement,
				Unique:        dbColumn.IsUnique,
				Charset:       dbColumn.Charset,
				Collate:       dbColumn.Collate,
				GeneratedKind: dbColumn.GeneratedKind,
			}
			if dbColumn.GeneratedExpression != nil {
				field.GeneratedExpression = *dbColumn.GeneratedExpression
			}

			// Set default value if present
			if dbColumn.ColumnDefault != nil {
				field.Default = *dbColumn.ColumnDefault
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

	// Convert indexes
	for _, dbIndex := range dbSchema.Indexes {
		// Skip primary key indexes as they're handled by primary key fields
		if dbIndex.IsPrimary {
			continue
		}

		index := goschema.Index{
			StructName: generateStructName(dbIndex.TableName),
			Name:       dbIndex.Name,
			TableName:  dbIndex.QualifiedTableName(),
			Fields:     dbIndex.Columns,
			Unique:     dbIndex.IsUnique,
		}
		database.Indexes = append(database.Indexes, index)
	}

	// Convert extensions
	for _, dbExtension := range dbSchema.Extensions {
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

	// Convert RLS policies
	for _, dbPolicy := range dbSchema.RLSPolicies {
		policy := goschema.RLSPolicy{
			StructName:          generateStructName(dbPolicy.Table),
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

	// Convert functions
	for _, dbFunction := range dbSchema.Functions {
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

	for _, dbView := range dbSchema.Views {
		database.Views = append(database.Views, goschema.View{
			Name:      dbView.QualifiedName(),
			Body:      dbView.Body,
			WithCheck: strings.EqualFold(dbView.CheckOption, "LOCAL") || strings.EqualFold(dbView.CheckOption, "CASCADED"),
			Comment:   dbView.Comment,
		})
	}

	for _, dbView := range dbSchema.MatViews {
		materializedView := goschema.MaterializedView{
			Name:            dbView.QualifiedName(),
			Body:            dbView.Body,
			RefreshStrategy: dbView.RefreshStrategy,
			Comment:         dbView.Comment,
		}
		materializedView.Canonicalize()
		database.MaterializedViews = append(database.MaterializedViews, materializedView)
	}

	for _, dbTrigger := range dbSchema.Triggers {
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

	// Convert roles
	for _, dbRole := range dbSchema.Roles {
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

	database.Grants = convertGrants(dbSchema.Grants)

	// Convert RLS enabled tables from tables that have RLS enabled
	for _, dbTable := range dbSchema.Tables {
		if dbTable.RLSEnabled {
			rlsEnabledTable := goschema.RLSEnabledTable{
				StructName: generateStructName(dbTable.Name),
				Table:      dbTable.Name,
				Comment:    "", // Comment not available in DBTable for RLS enablement
			}
			database.RLSEnabledTables = append(database.RLSEnabledTables, rlsEnabledTable)
		}
	}

	return database
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
		if c.Type != "FOREIGN KEY" || c.ColumnName == "" || c.ForeignTable == nil {
			continue
		}
		foreignTable := c.QualifiedForeignTableName()
		foreignColumn := ""
		if c.ForeignColumn != nil {
			foreignColumn = *c.ForeignColumn
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

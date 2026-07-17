// Package fromschema provides converters for transforming goschema types into AST nodes.
//
// This package serves as a bridge between the high-level schema definitions (goschema.Field,
// goschema.Table, etc.) and the low-level AST nodes that represent SQL DDL statements.
// The converters handle the translation of schema metadata into concrete SQL structures that
// can be rendered by dialect-specific visitors.
//
// # Core Functionality
//
// The package provides converter functions for all major schema elements:
//   - FromField: Converts field definitions to column AST nodes
//   - FromTable: Converts table definitions to CREATE TABLE AST nodes
//   - FromIndex: Converts index definitions to index AST nodes
//   - FromEnum: Converts enum definitions to enum AST nodes
//   - FromDatabase: Converts complete database schema to statement list
//
// # Example Usage
//
// Converting a simple field definition:
//
//	field := goschema.Field{
//		Name:     "email",
//		Type:     "VARCHAR(255)",
//		Nullable: false,
//		Unique:   true,
//		Comment:  "User email address",
//	}
//	column := fromschema.FromField(field, nil)
//
// Converting a complete database schema:
//
//	database := goschema.Database{
//		Tables: []goschema.Table{...},
//		Fields: []goschema.Field{...},
//		Indexes: []goschema.Index{...},
//		Enums: []goschema.Enum{...},
//	}
//	statements := fromschema.FromDatabase(database, "postgres")
//
// Platform-specific usage:
//
//	// Convert for MySQL with platform-specific overrides
//	mysqlStatements := fromschema.FromDatabase(database, "mysql")
//
//	// Convert for PostgreSQL with platform-specific overrides
//	postgresStatements := fromschema.FromDatabase(database, "postgres")
//
//	// Convert without platform-specific overrides (uses defaults)
//	defaultStatements := fromschema.FromDatabase(database, "")
package fromschema

import (
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
)

// escapeSQLStringLiteral properly escapes a string value for use in SQL string literals.
// It escapes single quotes by doubling them according to SQL standard and wraps the result in single quotes.
// This prevents SQL injection attacks when embedding user-provided values in SQL statements.
func escapeSQLStringLiteral(value string) string {
	// Escape single quotes by doubling them (SQL standard)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

// GenerateForeignKeyName generates a consistent foreign key constraint name
// following the convention: fk_{table_name}_{field_name}.
//
// This is the single source of truth for the conventional FK name used when a
// field-level foreign= annotation omits an explicit foreign_key_name. The
// schemadiff comparator (when synthesizing a field-level FK for drift
// comparison) and the dialect planners (when emitting the CREATE/ALTER) both
// derive the name from here so the synthesized name always lines up with the
// name actually written to the database.
func GenerateForeignKeyName(tableName, fieldName string) string {
	return "fk_" + strings.ToLower(tableName) + "_" + strings.ToLower(fieldName)
}

func applyPlatformOverrides(field goschema.Field, targetPlatform string) goschema.Field {
	fieldType := field.Type
	checkConstraint := field.Check
	checkName := field.CheckName
	comment := field.Comment
	charset := field.Charset
	collate := field.Collate
	defaultValue := field.Default
	defaultSet := field.DefaultSet
	defaultExpr := field.DefaultExpr

	// Apply built-in platform-specific type conversions for MySQL/MariaDB
	if targetPlatform == "mysql" || targetPlatform == "mariadb" {
		switch fieldType {
		case "SERIAL":
			fieldType = "INT"
			// Note: AutoInc flag should already be set for SERIAL fields
		case "BIGSERIAL":
			fieldType = "BIGINT"
			// Note: AutoInc flag should already be set for BIGSERIAL fields
		}
	}

	// Apply platform-specific overrides if available
	if targetPlatform == "" || field.Overrides == nil {
		newField := field // Shallow copy to avoid modifying original field
		newField.Type = fieldType
		newField.Check = checkConstraint
		newField.CheckName = checkName
		newField.Comment = comment
		newField.Charset = charset
		newField.Collate = collate
		newField.Default = defaultValue
		newField.DefaultSet = defaultSet
		newField.DefaultExpr = defaultExpr
		return newField
	}

	platformOverrides, exists := field.Overrides[targetPlatform]
	if !exists {
		newField := field // Shallow copy to avoid modifying original field
		newField.Type = fieldType
		newField.Check = checkConstraint
		newField.CheckName = checkName
		newField.Comment = comment
		newField.Charset = charset
		newField.Collate = collate
		newField.Default = defaultValue
		newField.DefaultSet = defaultSet
		newField.DefaultExpr = defaultExpr
		return newField
	}

	// Override type if specified
	if typeOverride, ok := platformOverrides["type"]; ok {
		fieldType = typeOverride
	}
	// Override check constraint if specified
	if checkOverride, ok := platformOverrides["check"]; ok {
		checkConstraint = checkOverride
	}
	// Override check constraint name if specified
	if checkNameOverride, ok := platformOverrides["check_name"]; ok {
		checkName = checkNameOverride
	}
	// Override comment if specified
	if commentOverride, ok := platformOverrides["comment"]; ok {
		comment = commentOverride
	}
	// Override column charset/collation if specified.
	if charsetOverride, ok := platformOverrides["charset"]; ok {
		charset = charsetOverride
	}
	if collateOverride, ok := platformOverrides["collate"]; ok {
		collate = collateOverride
	}
	// Override default value if specified
	if defaultOverride, ok := platformOverrides["default"]; ok {
		defaultValue = defaultOverride
		defaultSet = true
		defaultExpr = "" // Clear expression if literal default is overridden
	}
	// Override default expression if specified
	if defaultExprOverride, ok := platformOverrides["default_expr"]; ok {
		defaultExpr = defaultExprOverride
		defaultValue = "" // Clear literal if expression default is overridden
		defaultSet = false
	}

	newField := field // Shallow copy to avoid modifying original field
	newField.Type = fieldType
	newField.Check = checkConstraint
	newField.CheckName = checkName
	newField.Comment = comment
	newField.Charset = charset
	newField.Collate = collate
	newField.Default = defaultValue
	newField.DefaultSet = defaultSet
	newField.DefaultExpr = defaultExpr

	return newField
}

func handleEnumTypesForMySQLLike(field goschema.Field, enums []goschema.Enum, targetPlatform string) goschema.Field {
	// Handle enum types for MySQL/MariaDB platforms
	if !strings.HasPrefix(field.Type, "enum_") {
		return field
	}

	if enums == nil {
		return field
	}
	// Validate enum field
	validateEnumField(field, enums)

	if targetPlatform != "mysql" && targetPlatform != "mariadb" {
		return field
	}

	fieldType := field.Type

	// For MySQL/MariaDB, convert enum type to inline enum values
	// Find the corresponding global enum
	for _, enum := range enums {
		if enum.Name != field.Type {
			continue
		}

		// Convert to inline ENUM syntax for MySQL/MariaDB
		quotedValues := make([]string, len(enum.Values))
		for i, value := range enum.Values {
			quotedValues[i] = escapeSQLStringLiteral(value)
		}
		fieldType = fmt.Sprintf("ENUM(%s)", strings.Join(quotedValues, ", "))
		break
	}

	newField := field
	newField.Type = fieldType
	return newField
}

// FromField converts a goschema.Field to an ast.ColumnNode with comprehensive attribute mapping.
//
// This function transforms a high-level field definition into a concrete column AST node,
// handling all supported column attributes including constraints, defaults, foreign keys,
// enum validation, and platform-specific overrides.
//
// # Parameters
//
//   - field: The schema field definition containing all column metadata
//   - enums: Global enum definitions used for enum type validation (can be nil)
//   - targetPlatform: Target database platform for applying platform-specific overrides (e.g., "postgres", "mysql", "mariadb")
//
// # Supported Attributes
//
//   - Basic properties: name, type, nullable
//   - Constraints: primary key, unique, auto-increment
//   - Defaults: literal values and function calls
//   - Validation: check constraints
//   - Relationships: foreign key references
//   - Documentation: column comments
//   - Platform overrides: dialect-specific type mappings
//
// # Examples
//
// Basic field with constraints:
//
//	field := goschema.Field{
//		Name:     "email",
//		Type:     "VARCHAR(255)",
//		Nullable: false,
//		Unique:   true,
//		Comment:  "User email address",
//	}
//	column := FromField(field, nil)
//	// Results in: email VARCHAR(255) NOT NULL UNIQUE COMMENT 'User email address'
//
// Field with foreign key:
//
//	field := goschema.Field{
//		Name:           "user_id",
//		Type:           "INTEGER",
//		Nullable:       false,
//		Foreign:        "users(id)",
//		ForeignKeyName: "fk_posts_user",
//	}
//	column := FromField(field, nil)
//	// Results in: user_id INTEGER NOT NULL REFERENCES users(id)
//
// Field with default values:
//
//	field := goschema.Field{
//		Name:        "created_at",
//		Type:        "TIMESTAMP",
//		Nullable:    false,
//		DefaultExpr: "NOW()",
//	}
//	column := FromField(field, nil)
//	// Results in: created_at TIMESTAMP NOT NULL DEFAULT NOW()
//
// # Platform-Specific Overrides
//
// The function supports platform-specific overrides through the field.Overrides map.
// These overrides allow different database platforms to use different configurations:
//
//	field := goschema.Field{
//		Name: "data",
//		Type: "JSONB",
//		Overrides: map[string]map[string]string{
//			"mysql":   {"type": "JSON"},
//			"mariadb": {"type": "LONGTEXT", "check": "JSON_VALID(data)"},
//		},
//	}
//	// For MySQL: data JSON
//	// For MariaDB: data LONGTEXT CHECK (JSON_VALID(data))
//	// For PostgreSQL: data JSONB (default)
//
// # Return Value
//
// Returns a fully configured *ast.ColumnNode ready for SQL generation by dialect-specific visitors.
// The returned node contains all the attributes specified in the input field, with platform-specific
// overrides applied when a matching platform is specified.
func FromField(field goschema.Field, enums []goschema.Enum, targetPlatform string) *ast.ColumnNode {
	field = applyPlatformOverrides(field, targetPlatform)
	field = handleEnumTypesForMySQLLike(field, enums, targetPlatform)

	column := ast.NewColumn(field.Name, field.Type)

	// Set nullable - only override default if explicitly set to false
	// The default behavior should be nullable=true (which ast.NewColumn already sets)
	if !field.Nullable {
		column.SetNotNull()
	}

	// Set constraints
	if field.Primary {
		column.SetPrimary()
	}
	if field.Unique {
		column.SetUnique()
	}
	if field.AutoInc {
		column.SetAutoIncrement()
	}

	// Set default values (using potentially overridden values)
	switch {
	case field.DefaultSet || field.Default != "":
		column.SetDefault(field.Default)
	case field.DefaultExpr != "":
		column.SetDefaultExpression(field.DefaultExpr)
	}

	// Set check constraint (using potentially overridden value)
	if field.Check != "" {
		column.SetCheck(field.Check)
		if field.CheckName != "" {
			column.SetCheckName(field.CheckName)
		}
	}
	if field.GeneratedExpression != "" {
		column.SetGenerated(field.GeneratedExpression, field.GeneratedKind)
	}
	if field.UpdateExpression != "" {
		column.SetUpdateExpression(field.UpdateExpression)
	}
	if field.Charset != "" {
		column.SetCharset(field.Charset)
	}
	if field.Collate != "" {
		column.SetCollate(field.Collate)
	}

	// Set comment (using potentially overridden value)
	if field.Comment != "" {
		column.SetComment(field.Comment)
	}

	// Set foreign key reference
	if fkRef := ParseForeignKeyReference(field.Foreign); fkRef != nil {
		column.SetForeignKey(fkRef.Table, fkRef.Column, field.ForeignKeyName)
		column.ForeignKey.OnDelete = field.OnDelete
		column.ForeignKey.OnUpdate = field.OnUpdate
	}

	return column
}

// FromFieldWithoutForeignKeys converts a goschema.Field to an AST ColumnNode without foreign key constraints.
//
// This function is identical to FromField but excludes foreign key constraints from the column definition.
// It's used during two-phase table creation where foreign key constraints are added separately
// via ALTER TABLE statements to avoid circular dependency issues.
//
// Parameters:
//   - field: The field definition from the parsed Go schema
//   - enums: Available enum definitions for type validation
//   - targetPlatform: Target database platform for platform-specific handling
//
// Returns:
//   - *ast.ColumnNode: Column definition without foreign key constraints
func FromFieldWithoutForeignKeys(field goschema.Field, enums []goschema.Enum, targetPlatform string) *ast.ColumnNode {
	// Apply platform-specific overrides if available
	field = applyPlatformOverrides(field, targetPlatform)
	field = handleEnumTypesForMySQLLike(field, enums, targetPlatform)

	// Create column with basic properties
	column := ast.NewColumn(field.Name, field.Type)

	// Set nullable (default is true, so only set if false)
	if !field.Nullable {
		column.SetNotNull()
	}

	// Set primary key
	if field.Primary {
		column.SetPrimary()
	}

	// Set unique constraint
	if field.Unique {
		column.SetUnique()
	}

	// Set auto increment
	if field.AutoInc {
		column.SetAutoIncrement()
	}

	// Set default value (using potentially overridden value)
	if field.DefaultSet || field.Default != "" {
		column.SetDefault(field.Default)
	}

	// Set default expression (using potentially overridden value)
	if field.DefaultExpr != "" {
		column.SetDefaultExpression(field.DefaultExpr)
	}

	// Set check constraint (using potentially overridden value)
	if field.Check != "" {
		column.SetCheck(field.Check)
		if field.CheckName != "" {
			column.SetCheckName(field.CheckName)
		}
	}
	if field.GeneratedExpression != "" {
		column.SetGenerated(field.GeneratedExpression, field.GeneratedKind)
	}
	if field.UpdateExpression != "" {
		column.SetUpdateExpression(field.UpdateExpression)
	}
	if field.Charset != "" {
		column.SetCharset(field.Charset)
	}
	if field.Collate != "" {
		column.SetCollate(field.Collate)
	}

	// Set comment (using potentially overridden value)
	if field.Comment != "" {
		column.SetComment(field.Comment)
	}

	// NOTE: Foreign key constraints are intentionally excluded in this function
	// They should be added separately via ALTER TABLE statements

	return column
}

func applyTablePlatformOverrides(createTable *ast.CreateTableNode, table goschema.Table, targetPlatform string) goschema.Table {
	// Apply platform-specific overrides if available
	if targetPlatform == "" || table.Overrides == nil {
		return table
	}
	tableComment := table.Comment
	tableEngine := table.Engine
	tableAutoIncrement := table.AutoIncrement
	tableCharset := table.Charset
	tableCollate := table.Collate
	tableStrict := table.Strict
	tableWithoutRowID := table.WithoutRowID

	platformOverrides, exists := table.Overrides[targetPlatform]
	if !exists {
		return table
	}

	tableComment = overrideString(platformOverrides, "comment", tableComment)
	tableEngine = overrideString(platformOverrides, "engine", tableEngine)
	tableAutoIncrement = overrideString(platformOverrides, "auto_increment", tableAutoIncrement)
	tableCharset = overrideString(platformOverrides, "charset", tableCharset)
	tableCollate = overrideString(platformOverrides, "collate", tableCollate)
	tableStrict = overrideBool(platformOverrides, "strict", tableStrict)
	tableWithoutRowID = overrideBool(platformOverrides, "without_rowid", tableWithoutRowID)

	// Apply any other platform-specific options
	for key, value := range platformOverrides {
		if !isKnownTablePlatformOverride(key) {
			createTable.SetOption(strings.ToUpper(key), value)
		}
	}

	newTable := table
	newTable.Comment = tableComment
	newTable.Engine = tableEngine
	newTable.AutoIncrement = tableAutoIncrement
	newTable.Charset = tableCharset
	newTable.Collate = tableCollate
	newTable.Strict = tableStrict
	newTable.WithoutRowID = tableWithoutRowID
	return newTable
}

func overrideString(overrides map[string]string, key, current string) string {
	if value, ok := overrides[key]; ok {
		return value
	}
	return current
}

func overrideBool(overrides map[string]string, key string, current bool) bool {
	value, ok := overrides[key]
	if !ok {
		return current
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return current
	}
	return parsed
}

func isKnownTablePlatformOverride(key string) bool {
	knownKeys := []string{
		"comment",
		"engine",
		"auto_increment",
		"charset",
		"collate",
		"strict",
		"without_rowid",
	}
	return slices.Contains(knownKeys, key)
}

// FromTable converts a goschema.Table to an ast.CreateTableNode with all associated columns and constraints.
//
// This function creates a complete table definition by combining table metadata with its associated
// field definitions. It handles table-level properties, adds all matching columns, creates
// composite constraints, and applies platform-specific overrides.
//
// # Parameters
//
//   - table: The table directive containing table-level metadata
//   - fields: All schema fields; only those matching table.StructName are included
//   - enums: Global enum definitions passed to field conversion (can be nil)
//   - targetPlatform: Target database platform for applying platform-specific overrides
//
// # Table Features
//
//   - Table naming and comments
//   - Database-specific options (e.g., MySQL ENGINE)
//   - Composite primary keys
//   - Column definitions with full attribute support
//   - Automatic field filtering by struct name
//
// # Examples
//
// Basic table with simple primary key:
//
//	table := goschema.Table{
//		StructName: "User",
//		Name:       "users",
//		Comment:    "Application users",
//	}
//	fields := []goschema.Field{
//		{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
//		{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false, Unique: true},
//	}
//	createTable := FromTable(table, fields, nil)
//
// Table with composite primary key:
//
//	table := goschema.Table{
//		StructName: "UserRole",
//		Name:       "user_roles",
//		PrimaryKey: []string{"user_id", "role_id"},
//	}
//	fields := []goschema.Field{
//		{StructName: "UserRole", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
//		{StructName: "UserRole", Name: "role_id", Type: "INTEGER", Foreign: "roles(id)"},
//	}
//	createTable := FromTable(table, fields, nil)
//
// MySQL table with engine specification:
//
//	table := goschema.Table{
//		StructName: "Product",
//		Name:       "products",
//		Engine:     "InnoDB",
//		Comment:    "Product catalog",
//	}
//	createTable := FromTable(table, fields, nil)
//
// # Platform-Specific Overrides
//
// The function supports platform-specific table overrides through the table.Overrides map:
//
//	table := goschema.Table{
//		Name: "products",
//		Overrides: map[string]map[string]string{
//			"mysql":   {"engine": "InnoDB", "comment": "Product catalog"},
//			"mariadb": {"engine": "InnoDB", "charset": "utf8mb4"},
//		},
//	}
//
// # Return Value
//
// Returns a fully configured *ast.CreateTableNode ready for SQL generation.
// The node contains the table definition with all columns, constraints, and platform-specific options.
func FromTable(table goschema.Table, fields []goschema.Field, enums []goschema.Enum, targetPlatform string) *ast.CreateTableNode {
	createTable := ast.NewCreateTable(table.QualifiedName())

	newTable := applyTablePlatformOverrides(createTable, table, targetPlatform)

	// Start with base table values
	tableComment := newTable.Comment
	tableEngine := newTable.Engine

	// Set table comment (using potentially overridden value)
	if tableComment != "" {
		createTable.Comment = tableComment
	}

	// Set database-specific options (using potentially overridden value)
	if tableEngine != "" {
		createTable.SetOption("ENGINE", tableEngine)
	}
	if newTable.AutoIncrement != "" {
		createTable.SetOption("AUTO_INCREMENT", newTable.AutoIncrement)
	}
	if newTable.Charset != "" {
		createTable.SetOption("CHARSET", newTable.Charset)
	}
	if newTable.Collate != "" {
		createTable.SetOption("COLLATE", newTable.Collate)
	}
	if targetPlatform == "sqlite" {
		if newTable.WithoutRowID {
			createTable.SetOption("WITHOUT_ROWID", "true")
		}
		if newTable.Strict {
			createTable.SetOption("STRICT", "true")
		}
	}

	// Add columns for fields that belong to this table
	tableLevelPK := tableNeedsPrimaryKeyConstraint(newTable)
	for _, field := range fields {
		if field.StructName == table.StructName {
			if tableLevelPK && slices.Contains(newTable.PrimaryKey, field.Name) {
				field.Primary = false
			}
			column := FromField(field, enums, targetPlatform)
			createTable.AddColumn(column)
		}
	}

	// Add composite primary key constraint if specified
	if tableLevelPK {
		constraint := newPrimaryKeyConstraint(newTable)
		createTable.AddConstraint(constraint)
	}

	return createTable
}

func tableNeedsPrimaryKeyConstraint(table goschema.Table) bool {
	if len(table.PrimaryKeyParts) > 0 {
		return len(table.PrimaryKeyParts) > 1 || primaryKeyPartsHaveAttributes(table.PrimaryKeyParts)
	}
	return len(table.PrimaryKey) > 1
}

func primaryKeyPartsHaveAttributes(parts []goschema.PrimaryKeyPart) bool {
	for _, part := range parts {
		if part.Prefix != "" || part.Desc {
			return true
		}
	}
	return false
}

func newPrimaryKeyConstraint(table goschema.Table) *ast.ConstraintNode {
	if len(table.PrimaryKeyParts) == 0 {
		return ast.NewPrimaryKeyConstraint(table.PrimaryKey...)
	}
	columns := make([]string, 0, len(table.PrimaryKeyParts))
	columnParts := make([]ast.ConstraintColumn, 0, len(table.PrimaryKeyParts))
	for _, part := range table.PrimaryKeyParts {
		columns = append(columns, part.Name)
		columnParts = append(columnParts, ast.ConstraintColumn{
			Name:   part.Name,
			Prefix: part.Prefix,
			Desc:   part.Desc,
		})
	}
	return &ast.ConstraintNode{
		Type:        ast.PrimaryKeyConstraint,
		Columns:     columns,
		ColumnParts: columnParts,
	}
}

// FromConstraint converts a goschema.Constraint to an AST table constraint.
func FromConstraint(constraint goschema.Constraint) *ast.ConstraintNode {
	switch strings.ToUpper(constraint.Type) {
	case "PRIMARY KEY":
		return ast.NewPrimaryKeyConstraint(constraint.Columns...)
	case "UNIQUE":
		return ast.NewUniqueConstraint(constraint.Name, constraint.Columns...)
	case "FOREIGN KEY":
		return ast.NewForeignKeyConstraint(constraint.Name, constraint.Columns, &ast.ForeignKeyRef{
			Table:    constraint.ForeignTable,
			Column:   constraint.ForeignColumn,
			OnDelete: constraint.OnDelete,
			OnUpdate: constraint.OnUpdate,
		})
	case "CHECK":
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       constraint.Name,
			Expression: constraint.CheckExpression,
		}
	case "EXCLUDE":
		return ast.NewExcludeConstraint(constraint.Name, constraint.UsingMethod, constraint.ExcludeElements).
			SetWhereCondition(constraint.WhereCondition)
	default:
		return nil
	}
}

func addTableConstraints(createTable *ast.CreateTableNode, table goschema.Table, constraints []goschema.Constraint) {
	for _, constraint := range constraints {
		if !constraintBelongsToTable(constraint, table) {
			continue
		}

		node := FromConstraint(constraint)
		if node != nil {
			createTable.AddConstraint(node)
		}
	}
}

func constraintBelongsToTable(constraint goschema.Constraint, table goschema.Table) bool {
	if constraint.Table != "" {
		return constraint.Table == table.Name || constraint.Table == table.QualifiedName()
	}
	return constraint.StructName == table.StructName
}

// FromIndex converts a goschema.Index to an ast.IndexNode for database index creation.
//
// This function transforms index metadata into an AST node that can be rendered
// as CREATE INDEX statements by dialect-specific visitors. It supports both
// single-column and composite indexes with optional uniqueness constraints.
//
// # Parameters
//
//   - index: The schema index definition containing index metadata
//
// # Index Features
//
//   - Single-column and composite indexes
//   - Unique and non-unique indexes
//   - Index comments for documentation
//   - Automatic table association
//
// # Examples
//
// Simple single-column index:
//
//	index := goschema.Index{
//		Name:       "idx_users_email",
//		StructName: "users",
//		Fields:     []string{"email"},
//		Comment:    "Index for email lookups",
//	}
//	indexNode := FromIndex(index)
//
// Unique composite index:
//
//	index := goschema.Index{
//		Name:       "idx_user_roles_unique",
//		StructName: "user_roles",
//		Fields:     []string{"user_id", "role_id"},
//		Unique:     true,
//		Comment:    "Ensure unique user-role combinations",
//	}
//	indexNode := FromIndex(index)
//
// # Return Value
//
// Returns a fully configured *ast.IndexNode ready for SQL generation.
// The node contains the index name, target table, column list, and all specified options.
func FromIndex(index goschema.Index) *ast.IndexNode {
	// Use TableName if specified, otherwise fall back to StructName
	tableName := index.TableName
	if tableName == "" {
		tableName = index.StructName
	}

	indexNode := ast.NewIndex(index.Name, tableName, indexFields(index)...)
	if len(index.Parts) > 0 {
		indexNode.SetParts(toASTIndexParts(index.Parts))
	}

	// Set unique constraint
	if index.Unique {
		indexNode.Unique = true
	}

	// Set comment
	if index.Comment != "" {
		indexNode.Comment = index.Comment
	}

	// Set dialect-specific features. Type covers both PG (GIN/GIST/BTREE/HASH)
	// and CH (minmax/set/bloom_filter/...) — the renderer interprets it.
	if index.Type != "" {
		indexNode.Type = index.Type
	}

	if index.Parser != "" {
		indexNode.Parser = index.Parser
	}

	if index.Condition != "" {
		indexNode.Condition = index.Condition
	}

	if index.Operator != "" {
		indexNode.Operator = index.Operator
	}

	// Granularity is ClickHouse-only; non-ClickHouse renderers ignore it.
	// Zero propagates unchanged and signals "use renderer default".
	indexNode.Granularity = index.Granularity

	// Set IF NOT EXISTS for idempotent migrations
	indexNode.IfNotExists = true

	return indexNode
}

// FromExtension converts a goschema.Extension to an ast.ExtensionNode for PostgreSQL extension creation.
//
// This function transforms extension metadata into an AST node that can be rendered
// as CREATE EXTENSION statements for PostgreSQL databases.
//
// # Parameters
//
//   - extension: The schema extension definition containing extension metadata
//
// # Extension Features
//
//   - Extension name specification (pg_trgm, postgis, etc.)
//   - IF NOT EXISTS clause support
//   - Version specification for specific extension versions
//   - Extension comments for documentation
//
// # Examples
//
// Basic extension:
//
//	extension := goschema.Extension{
//		Name:        "pg_trgm",
//		IfNotExists: true,
//		Comment:     "Enable trigram similarity search",
//	}
//	extensionNode := FromExtension(extension)
//
// Extension with version:
//
//	extension := goschema.Extension{
//		Name:        "postgis",
//		Version:     "3.0",
//		IfNotExists: true,
//		Comment:     "Geographic data support",
//	}
//	extensionNode := FromExtension(extension)
//
// # Return Value
//
// Returns a fully configured *ast.ExtensionNode ready for SQL generation.
// The node contains the extension name, version, and all specified options.
func FromExtension(extension goschema.Extension) *ast.ExtensionNode {
	extensionNode := ast.NewExtension(extension.Name)

	// Set IF NOT EXISTS
	if extension.IfNotExists {
		extensionNode.SetIfNotExists()
	}

	// Set version
	if extension.Version != "" {
		extensionNode.SetVersion(extension.Version)
	}

	// Set comment
	if extension.Comment != "" {
		extensionNode.SetComment(extension.Comment)
	}

	return extensionNode
}

// FromEnum converts a goschema.Enum to an ast.EnumNode for database enum type creation.
//
// This function transforms a global enum definition into an AST node that can be rendered
// as CREATE TYPE statements (primarily for PostgreSQL) or equivalent enum handling for
// other database systems.
//
// # Parameters
//
//   - enum: The global enum definition containing the enum name and allowed values
//
// # Examples
//
// Simple status enum:
//
//	enum := goschema.Enum{
//		Name:   "status_type",
//		Values: []string{"active", "inactive", "pending"},
//	}
//	enumNode := FromEnum(enum)
//
// User role enum:
//
//	enum := goschema.Enum{
//		Name:   "user_role",
//		Values: []string{"admin", "moderator", "user", "guest"},
//	}
//	enumNode := FromEnum(enum)
//
// # Database Support
//
// Enum support varies by database:
//   - PostgreSQL: Native ENUM types via CREATE TYPE
//   - MySQL: ENUM column types
//   - SQLite: CHECK constraints with IN clauses
//   - Other databases: Various enum-like implementations
//
// # Return Value
//
// Returns an *ast.EnumNode ready for SQL generation by dialect-specific visitors.
// The visitor implementation determines how the enum is rendered for each database type.
func FromEnum(enum goschema.Enum) *ast.EnumNode {
	return ast.NewEnum(enum.Name, enum.Values...)
}

// FromFunction converts a goschema.Function to an ast.CreateFunctionNode.
//
// This function creates a PostgreSQL function definition from the parsed function metadata.
// It handles all function attributes including parameters, return type, language, security,
// volatility, and function body.
//
// # Parameters
//
//   - function: The function definition containing all function metadata
//
// # Return Value
//
// Returns a fully configured *ast.CreateFunctionNode ready for SQL generation.
func FromFunction(function goschema.Function) *ast.CreateFunctionNode {
	functionNode := ast.NewCreateFunction(function.Name).
		SetParameters(function.Parameters).
		SetReturns(function.Returns).
		SetLanguage(function.Language).
		SetSecurity(function.Security).
		SetVolatility(function.Volatility).
		SetBody(function.Body).
		SetComment(function.Comment)

	return functionNode
}

// FromView converts a goschema.View to an ast.CreateViewNode.
func FromView(view goschema.View) *ast.CreateViewNode {
	viewNode := ast.NewCreateView(view.Name).
		SetBody(view.Body).
		SetWithCheck(view.WithCheck).
		SetComment(view.Comment)
	return viewNode
}

// FromMaterializedView converts a goschema.MaterializedView to an
// ast.CreateMaterializedViewNode.
func FromMaterializedView(view goschema.MaterializedView) *ast.CreateMaterializedViewNode {
	view.Canonicalize()
	viewNode := ast.NewCreateMaterializedView(view.Name).
		SetBody(view.Body).
		SetRefreshStrategy(view.RefreshStrategy).
		SetComment(view.Comment)
	return viewNode
}

// FromTrigger converts a goschema.Trigger to an ast.CreateTriggerNode.
func FromTrigger(trigger goschema.Trigger) *ast.CreateTriggerNode {
	trigger.Canonicalize()
	triggerNode := ast.NewCreateTrigger(trigger.Name, trigger.Table).
		SetTiming(trigger.Timing).
		SetEvent(trigger.Event).
		SetForEach(trigger.ForEach).
		SetBody(trigger.Body).
		SetFunctionName(trigger.FunctionName()).
		SetComment(trigger.Comment)
	return triggerNode
}

// FromRLSPolicy converts a goschema.RLSPolicy to an ast.CreatePolicyNode.
//
// This function creates a PostgreSQL RLS policy definition from the parsed policy metadata.
// It handles all policy attributes including target table, policy type, target roles,
// and policy expressions.
//
// # Parameters
//
//   - policy: The RLS policy definition containing all policy metadata
//
// # Return Value
//
// Returns a fully configured *ast.CreatePolicyNode ready for SQL generation.
func FromRLSPolicy(policy goschema.RLSPolicy) *ast.CreatePolicyNode {
	policyNode := ast.NewCreatePolicy(policy.Name, policy.Table).
		SetPolicyFor(policy.PolicyFor).
		SetToRoles(policy.ToRoles).
		SetUsingExpression(policy.UsingExpression).
		SetWithCheckExpression(policy.WithCheckExpression).
		SetComment(policy.Comment)

	return policyNode
}

// FromRLSEnabledTable converts a goschema.RLSEnabledTable to an ast.AlterTableEnableRLSNode.
//
// This function creates a PostgreSQL ALTER TABLE ENABLE ROW LEVEL SECURITY statement
// from the parsed RLS enablement metadata.
//
// # Parameters
//
//   - rlsEnabled: The RLS enablement definition containing table and comment metadata
//
// # Return Value
//
// Returns a fully configured *ast.AlterTableEnableRLSNode ready for SQL generation.
func FromRLSEnabledTable(rlsEnabled goschema.RLSEnabledTable) *ast.AlterTableEnableRLSNode {
	rlsNode := ast.NewAlterTableEnableRLS(rlsEnabled.Table).
		SetComment(rlsEnabled.Comment)

	return rlsNode
}

// FromRole converts a goschema.Role to an ast.CreateRoleNode.
//
// This function creates a PostgreSQL role definition from the parsed role metadata.
// It handles all role attributes including login capabilities, password, privileges,
// and other role properties.
//
// # Parameters
//
//   - role: The role definition containing all role metadata
//
// # Return Value
//
// Returns a fully configured *ast.CreateRoleNode ready for SQL generation.
func FromRole(role goschema.Role) *ast.CreateRoleNode {
	roleNode := ast.NewCreateRole(role.Name).
		SetLogin(role.Login).
		SetPassword(role.Password).
		SetSuperuser(role.Superuser).
		SetCreateDB(role.CreateDB).
		SetCreateRole(role.CreateRole).
		SetInherit(role.Inherit).
		SetReplication(role.Replication).
		SetComment(role.Comment)

	return roleNode
}

// FromGrant converts a goschema.Grant to an ast.GrantPrivilegeNode.
func FromGrant(grant goschema.Grant) *ast.GrantPrivilegeNode {
	grant.Canonicalize()
	objectType := "TABLE"
	objectName := grant.OnTable
	if grant.OnSchema != "" {
		objectType = "SCHEMA"
		objectName = grant.OnSchema
	}
	return ast.NewGrantPrivilege(grant.Role, objectType, objectName, grant.Privileges).
		SetWithOption(grant.WithOption).
		SetComment(grant.Comment)
}

// FromDatabase converts a complete goschema.Database to an ast.StatementList containing all DDL statements.
//
// This function creates a comprehensive database schema by converting all schema elements
// (schemas, enums, tables, indexes, embedded fields) into their corresponding AST nodes. The statements are ordered
// to ensure proper dependency resolution during SQL execution, with platform-specific
// overrides applied throughout.
//
// # Parameters
//
//   - database: The complete database schema containing all schemas, tables, fields, indexes, enums, and embedded fields
//   - targetPlatform: Target database platform for applying platform-specific overrides
//
// # Statement Ordering
//
// The function generates statements in the following order to respect dependencies:
//  1. Schema definitions (CREATE SCHEMA statements)
//  2. Enum type definitions (CREATE TYPE statements)
//  3. Table definitions (CREATE TABLE statements) with embedded fields processed
//  4. Extension definitions
//  5. Dialect-specific objects such as roles, functions, views, RLS policies, grants, and triggers
//  6. Index definitions (CREATE INDEX statements)
//
// This ordering ensures that:
//   - Schemas are created before tables that reference them
//   - Enum types are created before tables that reference them
//   - Tables are created before indexes that reference them
//   - Foreign key dependencies are handled by the table creation order
//   - Embedded fields are processed and converted to regular fields before table creation
//
// # Embedded Field Processing
//
// The function processes embedded fields before creating tables, supporting four modes:
//   - "inline": Expands embedded struct fields as individual table columns
//   - "json": Serializes the entire embedded struct into a single JSON/JSONB column
//   - "relation": Creates a foreign key relationship to another table
//   - "skip": Completely ignores the embedded field during schema generation
//
// # Examples
//
// Converting a complete database schema:
//
//	database := goschema.Database{
//		Enums: []goschema.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []goschema.Table{
//			{StructName: "User", Name: "users", Comment: "User accounts"},
//		},
//		Fields: []goschema.Field{
//			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
//			{StructName: "User", Name: "status", Type: "user_status", Nullable: false},
//		},
//		EmbeddedFields: []goschema.EmbeddedField{
//			{StructName: "User", Mode: "inline", EmbeddedTypeName: "Timestamps"},
//		},
//		Indexes: []goschema.Index{
//			{Name: "idx_users_status", StructName: "users", Fields: []string{"status"}},
//		},
//	}
//	statements := FromDatabase(database)
//
// # Platform-Specific Processing
//
// All schema elements (tables, fields, embedded fields) are processed with platform-specific overrides
// applied based on the targetPlatform parameter. This ensures that the generated
// AST nodes contain the appropriate configurations for the target database.
//
// # Return Value
//
// Returns an *ast.StatementList containing all DDL statements in proper execution order.
// The statement list can be processed by dialect-specific visitors to generate SQL.
func FromDatabase(database goschema.Database, targetPlatform string) *ast.StatementList {
	statements := &ast.StatementList{
		Statements: make([]ast.Node, 0),
	}

	// Process embedded fields to generate additional fields for each table
	allFields := ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)

	// 1. Add schema definitions first (they may be referenced by tables)
	appendSchemaStatements(statements, database.Schemas)

	// 2. Add enum definitions (they may be referenced by tables)
	for _, enum := range database.Enums {
		enumNode := FromEnum(enum)
		statements.Statements = append(statements.Statements, enumNode)
	}

	// 3. Add table definitions (they may be referenced by indexes)
	// Use the combined field list that includes embedded field expansions
	for _, table := range database.Tables {
		tableNode := FromTable(table, allFields, database.Enums, targetPlatform)
		addTableConstraints(tableNode, table, database.Constraints)
		statements.Statements = append(statements.Statements, tableNode)
	}

	// 4. Add extension definitions (PostgreSQL-specific)
	for _, extension := range database.Extensions {
		extensionNode := FromExtension(extension)
		statements.Statements = append(statements.Statements, extensionNode)
	}

	// 5. Add PostgreSQL-specific features (functions and RLS)
	// These features are only supported by PostgreSQL, so we only generate them for PostgreSQL dialects
	isPostgreSQL := strings.ToLower(targetPlatform) == "postgresql" || strings.ToLower(targetPlatform) == "postgres"

	if isPostgreSQL {
		// Add PostgreSQL roles (they may be referenced by RLS policies)
		for _, role := range database.Roles {
			roleNode := FromRole(role)
			statements.Statements = append(statements.Statements, roleNode)
		}

		// Add PostgreSQL functions (they may be referenced by RLS policies)
		for _, function := range database.Functions {
			functionNode := FromFunction(function)
			statements.Statements = append(statements.Statements, functionNode)
		}

		for _, view := range database.Views {
			viewNode := FromView(view)
			statements.Statements = append(statements.Statements, viewNode)
		}

		for _, view := range database.MaterializedViews {
			viewNode := FromMaterializedView(view)
			statements.Statements = append(statements.Statements, viewNode)
		}

		// Add RLS enablement statements (must come before policies)
		for _, rlsEnabled := range database.RLSEnabledTables {
			rlsNode := FromRLSEnabledTable(rlsEnabled)
			statements.Statements = append(statements.Statements, rlsNode)
		}

		// Add RLS policies (depend on functions and RLS being enabled)
		for _, rlsPolicy := range database.RLSPolicies {
			policyNode := FromRLSPolicy(rlsPolicy)
			statements.Statements = append(statements.Statements, policyNode)
		}

		for _, grant := range database.Grants {
			grantNode := FromGrant(grant)
			statements.Statements = append(statements.Statements, grantNode)
		}

		for _, trigger := range database.Triggers {
			triggerNode := FromTrigger(trigger)
			statements.Statements = append(statements.Statements, triggerNode)
		}
	}

	if !isPostgreSQL && (strings.EqualFold(targetPlatform, "mysql") || strings.EqualFold(targetPlatform, "mariadb")) {
		for _, view := range database.Views {
			viewNode := FromView(view)
			statements.Statements = append(statements.Statements, viewNode)
		}
		for _, trigger := range database.Triggers {
			triggerNode := FromTrigger(trigger)
			statements.Statements = append(statements.Statements, triggerNode)
		}
	}

	// 7. Add index definitions last
	// Create a mapping from struct names to table names for proper index table resolution
	structToTableMap := createStructToTableMap(database.Tables)
	for _, index := range database.Indexes {
		indexNode := FromIndexWithTableMapping(index, structToTableMap)
		statements.Statements = append(statements.Statements, indexNode)
	}

	return statements
}

func appendSchemaStatements(statements *ast.StatementList, schemas []goschema.Schema) {
	for _, schema := range schemas {
		statements.Statements = append(statements.Statements, &ast.CreateSchemaNode{
			Name:        schema.Name,
			IfNotExists: true,
			Comment:     schema.Comment,
			Charset:     schema.Charset,
			Collate:     schema.Collate,
		})
	}
}

// createStructToTableMap creates a mapping from struct names to table names.
// This is used to resolve the correct table names for indexes.
func createStructToTableMap(tables []goschema.Table) map[string]string {
	structToTableMap := make(map[string]string)
	for _, table := range tables {
		structToTableMap[table.StructName] = table.QualifiedName()
	}
	return structToTableMap
}

// FromIndexWithTableMapping converts a goschema.Index to an ast.IndexNode with proper table name resolution.
// This function is similar to FromIndex but uses a struct-to-table mapping to resolve the correct table names.
func FromIndexWithTableMapping(index goschema.Index, structToTableMap map[string]string) *ast.IndexNode {
	// Determine the target table name
	tableName := index.TableName
	if tableName == "" {
		// If no explicit table name, try to resolve from struct name
		if mappedTableName, exists := structToTableMap[index.StructName]; exists {
			tableName = mappedTableName
		} else {
			// Fall back to struct name if no mapping found
			tableName = index.StructName
		}
	}

	indexNode := ast.NewIndex(index.Name, tableName, indexFields(index)...)
	if len(index.Parts) > 0 {
		indexNode.SetParts(toASTIndexParts(index.Parts))
	}

	// Set unique constraint
	if index.Unique {
		indexNode.Unique = true
	}

	// Set comment
	if index.Comment != "" {
		indexNode.Comment = index.Comment
	}

	// Set dialect-specific features. Type covers both PG (GIN/GIST/BTREE/HASH)
	// and CH (minmax/set/bloom_filter/...) — the renderer interprets it.
	if index.Type != "" {
		indexNode.Type = index.Type
	}

	if index.Parser != "" {
		indexNode.Parser = index.Parser
	}

	if index.Condition != "" {
		indexNode.Condition = index.Condition
	}

	if index.Operator != "" {
		indexNode.Operator = index.Operator
	}

	// Granularity is ClickHouse-only; non-ClickHouse renderers ignore it.
	indexNode.Granularity = index.Granularity

	// Set IF NOT EXISTS for idempotent migrations
	indexNode.IfNotExists = true

	return indexNode
}

func toASTIndexParts(parts []goschema.IndexPart) []ast.IndexPart {
	astParts := make([]ast.IndexPart, 0, len(parts))
	for _, part := range parts {
		astParts = append(astParts, ast.IndexPart{
			Name:   part.Name,
			Expr:   part.Expr,
			Prefix: part.Prefix,
			Desc:   part.Desc,
		})
	}
	return astParts
}

func indexFields(index goschema.Index) []string {
	if len(index.Parts) == 0 {
		return index.Fields
	}
	fields := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		if part.Expr != "" {
			fields = append(fields, part.Expr)
			continue
		}
		fields = append(fields, part.Name)
	}
	return fields
}

// ParseForeignKeyReference parses a foreign key reference string into an ast.ForeignKeyRef.
//
// The foreign key reference string should be in the format "table(column)" or just "table"
// (which defaults to referencing the "id" column).
//
// Examples:
//   - "users(id)" -> references users.id
//   - "users" -> references users.id (default)
//   - "categories(slug)" -> references categories.slug
//
// Returns nil if the reference string is malformed.
func ParseForeignKeyReference(foreign string) *ast.ForeignKeyRef {
	if foreign == "" {
		return nil
	}

	// Check if it contains parentheses for column specification
	if strings.Contains(foreign, "(") && strings.Contains(foreign, ")") {
		// Parse "table(column)" format
		parts := strings.Split(foreign, "(")
		if len(parts) != 2 {
			return nil
		}

		table := strings.TrimSpace(parts[0])
		columnPart := strings.TrimSpace(parts[1])

		// Remove closing parenthesis
		if !strings.HasSuffix(columnPart, ")") {
			return nil
		}
		column := strings.TrimSuffix(columnPart, ")")

		return &ast.ForeignKeyRef{
			Table:  table,
			Column: column,
		}
	}

	// Default to "id" column if no column specified
	return &ast.ForeignKeyRef{
		Table:  strings.TrimSpace(foreign),
		Column: "id",
	}
}

// validateEnumField validates that enum field values are consistent with global enum definitions.
//
// This function performs validation for fields with enum types, ensuring that:
//   - The referenced global enum exists
//   - Any field-specific enum values are a subset of the global enum values
//
// Validation warnings are logged but do not stop the conversion process, allowing for
// graceful handling of incomplete or evolving schema definitions.
func validateEnumField(field goschema.Field, enums []goschema.Enum) {
	if !strings.HasPrefix(field.Type, "enum_") {
		return
	}

	// Find the corresponding global enum
	var globalEnum *goschema.Enum
	for _, enum := range enums {
		if enum.Name == field.Type {
			globalEnum = &enum
			break
		}
	}

	// If no global enum found, this might be an issue but we don't panic
	// as the field might be using a custom enum type
	if globalEnum == nil {
		return
	}

	// If field has enum values, validate they match the global enum
	if len(field.Enum) > 0 {
		// Check that all field enum values exist in the global enum
		globalEnumMap := make(map[string]bool)
		for _, value := range globalEnum.Values {
			globalEnumMap[value] = true
		}

		for _, fieldValue := range field.Enum {
			if fieldValue != "" && !globalEnumMap[fieldValue] {
				// Log warning - in a real implementation, you might want to use a proper logger
				// For now, we'll just continue without panicking
				_ = fieldValue // Suppress unused variable warning
			}
		}
	}
}

// ProcessEmbeddedFields processes embedded fields and generates corresponding schema fields based on embedding modes.
//
// This function is the core processor for handling embedded struct fields in Go structs, transforming them
// into appropriate database schema fields according to the specified embedding mode. It supports four
// distinct modes of embedding that provide different approaches to handling complex data structures
// in relational databases.
//
// # Parameters
//
//   - embeddedFields: Collection of embedded field definitions to process
//   - originalFields: Complete collection of schema fields from all parsed structs
//
// # Embedding Modes
//
// The function supports four embedding modes, each serving different architectural patterns:
//
// 1. **"inline"**: Expands embedded struct fields as individual table columns
// 2. **"json"**: Serializes the entire embedded struct into a single JSON/JSONB column
// 3. **"relation"**: Creates a foreign key relationship to another table
// 4. **"skip"**: Completely ignores the embedded field during schema generation
//
// # Return Value
//
// Returns a combined slice of goschema.Field containing both the original fields and
// the generated fields from embedded field processing. This combined list is ready
// for use in table creation.
func ProcessEmbeddedFields(embeddedFields []goschema.EmbeddedField, originalFields []goschema.Field) []goschema.Field {
	// Start with the original fields
	allFields := make([]goschema.Field, len(originalFields))
	copy(allFields, originalFields)

	// Process embedded fields for each struct
	structNames := goschema.UniqueStructNames(embeddedFields)
	for _, structName := range structNames {
		generatedFields := processEmbeddedFieldsForStruct(embeddedFields, originalFields, structName)
		allFields = append(allFields, generatedFields...)
	}

	return allFields
}

func processEmbeddedInlineMode(generatedFields []goschema.Field, embedded goschema.EmbeddedField, allFields []goschema.Field, allEmbeddedFields []goschema.EmbeddedField, structName string) []goschema.Field {
	// INLINE MODE: Expand embedded struct fields as individual table columns
	generatedFields = processEmbeddedInlineModeRecursive(generatedFields, embedded, allFields, allEmbeddedFields, structName)
	return generatedFields
}

// processEmbeddedInlineModeRecursive recursively processes embedded fields in inline mode.
// This handles nested embedded structs by recursively expanding embedded fields within embedded types.
func processEmbeddedInlineModeRecursive(generatedFields []goschema.Field, embedded goschema.EmbeddedField, allFields []goschema.Field, allEmbeddedFields []goschema.EmbeddedField, structName string) []goschema.Field {
	// Step 1: Add direct fields from the embedded type
	for _, field := range allFields {
		if field.StructName != embedded.EmbeddedTypeName {
			continue
		}
		// Clone the field and reassign to target struct
		newField := field
		newField.StructName = structName

		// Apply prefix to column name if specified
		if embedded.Prefix != "" {
			newField.Name = embedded.Prefix + field.Name
		}

		generatedFields = append(generatedFields, newField)
	}

	// Step 2: Recursively process embedded fields within the embedded type
	for _, nestedEmbedded := range allEmbeddedFields {
		if nestedEmbedded.StructName != embedded.EmbeddedTypeName {
			continue
		}

		// Only process inline mode embedded fields recursively
		if nestedEmbedded.Mode == "inline" {
			// Create a new embedded field with the target struct name and combined prefix
			recursiveEmbedded := nestedEmbedded
			recursiveEmbedded.StructName = structName

			// Combine prefixes: if the parent has a prefix, prepend it to the nested prefix
			if embedded.Prefix != "" {
				if recursiveEmbedded.Prefix != "" {
					recursiveEmbedded.Prefix = embedded.Prefix + recursiveEmbedded.Prefix
				} else {
					recursiveEmbedded.Prefix = embedded.Prefix
				}
			}

			// Recursively process the nested embedded field
			generatedFields = processEmbeddedInlineModeRecursive(generatedFields, recursiveEmbedded, allFields, allEmbeddedFields, structName)
		}
	}

	return generatedFields
}

func processEmbeddedJSONMode(generatedFields []goschema.Field, embedded goschema.EmbeddedField, structName string) []goschema.Field {
	// JSON MODE: Serialize embedded struct into a single JSON/JSONB column
	columnName := embedded.Name
	if columnName == "" {
		// Auto-generate column name: "Meta" -> "meta_data"
		columnName = strings.ToLower(embedded.EmbeddedTypeName) + "_data"
	}

	columnType := embedded.Type
	if columnType == "" {
		columnType = "JSONB" // Default to PostgreSQL JSONB for best performance
	}

	// Create the JSON column field
	generatedFields = append(generatedFields, goschema.Field{
		StructName: structName,
		FieldName:  embedded.EmbeddedTypeName,
		Name:       columnName,
		Type:       columnType,
		Nullable:   embedded.Nullable,
		Comment:    embedded.Comment,
		Overrides:  embedded.Overrides, // Platform-specific type overrides (JSON vs JSONB vs TEXT)
	})

	return generatedFields
}

func processEmbeddedRelationMode(generatedFields []goschema.Field, embedded goschema.EmbeddedField, structName string) []goschema.Field {
	// RELATION MODE: Create a foreign key field linking to another table
	if embedded.Field == "" || embedded.Ref == "" {
		// Skip incomplete relation definitions - both field name and reference are required
		return generatedFields
	}

	// Intelligent type inference based on reference pattern
	refType := "INTEGER" // Default assumption: numeric primary key
	if strings.Contains(embedded.Ref, "VARCHAR") || strings.Contains(embedded.Ref, "TEXT") ||
		strings.Contains(strings.ToLower(embedded.Ref), "uuid") {
		// Reference suggests string-based key (likely UUID)
		refType = "VARCHAR(36)" // Standard UUID length
	}

	// Generate automatic foreign key constraint name following convention
	foreignKeyName := GenerateForeignKeyName(structName, embedded.Field)

	// Create platform-specific overrides for MySQL/MariaDB compatibility
	// MySQL/MariaDB use INT for SERIAL types, so foreign keys should also use INT
	overrides := make(map[string]map[string]string)
	if refType == "INTEGER" {
		overrides["mysql"] = map[string]string{"type": "INT"}
		overrides["mariadb"] = map[string]string{"type": "INT"}
	}

	// Create the foreign key field
	generatedFields = append(generatedFields, goschema.Field{
		StructName:     structName,
		FieldName:      embedded.EmbeddedTypeName,
		Name:           embedded.Field,    // e.g., "user_id"
		Type:           refType,           // INTEGER or VARCHAR(36)
		Nullable:       embedded.Nullable, // Can the relationship be optional?
		Foreign:        embedded.Ref,      // e.g., "users(id)"
		ForeignKeyName: foreignKeyName,    // e.g., "fk_posts_user_id"
		OnDelete:       embedded.OnDelete, // ON DELETE action (CASCADE, SET NULL, etc.)
		OnUpdate:       embedded.OnUpdate, // ON UPDATE action (CASCADE, SET NULL, etc.)
		Comment:        embedded.Comment,  // Documentation for the relationship
		Overrides:      overrides,         // Platform-specific type overrides
	})

	return generatedFields
}

// processEmbeddedFieldsForStruct processes embedded fields for a specific struct and generates corresponding schema fields.
//
// This function implements the core logic for transforming embedded fields into database schema fields
// according to their specified embedding mode. It processes only embedded fields that belong to the
// specified structName.
//
// # Parameters
//
//   - embeddedFields: Collection of embedded field definitions to process
//   - allFields: Complete collection of schema fields from all parsed structs
//   - structName: Name of the target struct to process embedded fields for
//
// # Return Value
//
// Returns a slice of goschema.Field representing the generated database fields for the specified struct.
// Each field is fully configured with appropriate types, constraints, and metadata.
func processEmbeddedFieldsForStruct(embeddedFields []goschema.EmbeddedField, allFields []goschema.Field, structName string) []goschema.Field {
	var generatedFields []goschema.Field

	// Process each embedded field definition
	for _, embedded := range embeddedFields {
		// Filter: only process embedded fields for the target struct
		if embedded.StructName != structName {
			continue
		}

		switch embedded.Mode {
		case "inline":
			// INLINE MODE: Expand embedded struct fields as individual table columns
			generatedFields = processEmbeddedInlineMode(generatedFields, embedded, allFields, embeddedFields, structName)
		case "json":
			// JSON MODE: Serialize embedded struct into a single JSON/JSONB column
			generatedFields = processEmbeddedJSONMode(generatedFields, embedded, structName)
		case "relation":
			// RELATION MODE: Create a foreign key field linking to another table
			generatedFields = processEmbeddedRelationMode(generatedFields, embedded, structName)
		case "skip":
			// SKIP MODE: Completely ignore this embedded field
			continue
		default:
			// DEFAULT MODE: Fall back to inline behavior for unrecognized modes
			slog.Warn("Unrecognized embedding mode for struct - defaulting to inline mode", "mode", embedded.Mode, "struct", structName)
			generatedFields = processEmbeddedInlineMode(generatedFields, embedded, allFields, embeddedFields, structName)
		}
	}

	return generatedFields
}

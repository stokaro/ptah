// Package modelast lowers canonical desired-schema model fragments into SQL
// AST nodes.
//
// Model normalization and derived model facts belong to schemaprep and
// schemamodel. This package owns only the representation boundary: node
// construction, dialect-specific lowering, and executable statement order.
// [WalkDatabase] streams that order to the shipping renderer. [CollectDatabase]
// materializes it only for the public compatibility API that still returns an
// AST statement list.
package modelast

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/schemaprep"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/systemschema"
	"go.5x5.cz/ptah/internal/tablelookup"
	"go.5x5.cz/ptah/internal/tableref"
)

// escapeSQLStringLiteral properly escapes a string value for use in SQL string literals.
// It escapes single quotes by doubling them according to SQL standard and wraps the result in single quotes.
// This prevents SQL injection attacks when embedding user-provided values in SQL statements.
func escapeSQLStringLiteral(value string) string {
	// Escape single quotes by doubling them (SQL standard)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func sqlServerBracketIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}

func defaultGeneratedKind(field schemamodel.Field, targetPlatform string) string {
	if field.GeneratedExpression == "" || field.GeneratedKind != "" {
		return field.GeneratedKind
	}
	switch {
	case isPostgreSQLFamilyPlatform(targetPlatform):
		return "STORED"
	case isMySQLFamilyTarget(targetPlatform):
		return "VIRTUAL"
	case platform.NormalizeDialect(targetPlatform) == platform.SQLServer:
		return "PERSISTED"
	case isSQLiteTarget(targetPlatform):
		return "VIRTUAL"
	default:
		return field.GeneratedKind
	}
}

// isPostgreSQLFamilyPlatform reports whether the target renders PostgreSQL
// object DDL: PostgreSQL itself and the wire-compatible engines.
//
// It survives only where the answer changes how a COLUMN is modeled -- the
// default persistence of a generated column, which PostgreSQL spells STORED and
// SQL Server spells PERSISTED. It no longer decides whether any declared object
// is converted at all: CollectDatabase hands every object to the renderer and the
// capability set answers there, so that `schema render` and `schema apply`
// cannot disagree about the same file (stokaro/ptah#929).
func isPostgreSQLFamilyPlatform(targetPlatform string) bool {
	return platform.IsPostgresFamily(targetPlatform)
}

func supportsExtensionInstallationSchema(targetPlatform string) bool {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.Postgres, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

// typeRawSQLSurvives reports whether the column type is still the one the
// schema author wrote with Atlas HCL's sql() escape hatch.
//
// A platform override or an inlined enum model replaces the type with a
// spelling the author never wrote, and the sql() call that produced the
// original no longer describes it. Carrying the marker across such a rewrite
// would make a writer emit `sql("<the override>")`, attributing the escape
// hatch to text that never went through it.
func typeRawSQLSurvives(field schemamodel.Field, declaredType string) bool {
	return field.TypeRawSQL && field.Type == declaredType
}

// declaredEnum returns the enum the schema declares under the name fieldType,
// or nil when fieldType names something else.
//
// This is the ONLY test for "is this column an enum". Ptah used to additionally
// require the type name to start with "enum_", an undocumented convention that
// appears nowhere in `ptah schema annotations`. An enum called "status_kind" was
// therefore left as the bare type name in CREATE TABLE while the same dialects
// skip standalone CREATE TYPE emission, so the values disappeared entirely and
// the DDL named a type the server had never heard of (stokaro/ptah#931 item 1).
// A declaration is what makes a type an enum; how it is spelled is not.
func declaredEnum(fieldType string, enums []schemamodel.Enum) *schemamodel.Enum {
	for i := range enums {
		if enums[i].Name == fieldType {
			return &enums[i]
		}
	}
	return nil
}

func handleEnumTypes(field schemamodel.Field, enums []schemamodel.Enum, targetPlatform string) schemamodel.Field {
	enum := declaredEnum(field.Type, enums)
	if enum == nil {
		return field
	}

	// Validate enum field
	validateEnumField(field, enums)

	// The inline rewrite is the exact complement of the standalone CREATE TYPE:
	// a dialect models an enum either on the column or as its own type, never
	// both and never neither. Deriving both sides from one predicate is what
	// stops a spelling from suppressing the CREATE TYPE and the inline rewrite
	// at the same time, which is how `--dialect sqlite3` used to drop the enum
	// entirely and render the column as the bare type name `enum_status`.
	if schemaprep.EmitsStandaloneEnumDefinitions(targetPlatform) {
		// A standalone enum type is created in a schema, and a column declared
		// against it has to name the same one. The declared type is matched by
		// bare name -- that is what makes an annotation `type="mood"` find the
		// enum -- so the qualifier is put back here, from the enum's own
		// schema, rather than being expected in the field.
		//
		// Without it, `CREATE TYPE "extra"."mood"` was followed by
		// `CREATE TABLE "extra"."b" ("feeling" mood)`, which resolves through
		// search_path and fails with `type "mood" does not exist` on every
		// schema off the path (stokaro/ptah#1276). An enum with no schema is
		// left exactly as it was, which is every enum a Go annotation, a YAML
		// schema, or a single-schema read can produce.
		field.Type = enum.QualifiedName()
		return field
	}

	return applyInlineEnumModel(field, *enum, targetPlatform)
}

func applyInlineEnumModel(field schemamodel.Field, enum schemamodel.Enum, targetPlatform string) schemamodel.Field {
	quotedValues := make([]string, len(enum.Values))
	for i, value := range enum.Values {
		quotedValues[i] = escapeSQLStringLiteral(value)
	}

	newField := field
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB:
		newField.Type = fmt.Sprintf("ENUM(%s)", strings.Join(quotedValues, ", "))
	case platform.SQLite:
		newField.Type = "TEXT"
		enumCheck := fmt.Sprintf("%s IN (%s)", field.Name, strings.Join(quotedValues, ", "))
		if field.Check != "" {
			enumCheck = fmt.Sprintf("(%s) AND %s", field.Check, enumCheck)
		}
		newField.Check = enumCheck
	case platform.SQLServer:
		newField.Type = "NVARCHAR(255)"
		enumCheck := fmt.Sprintf("%s IN (%s)", sqlServerBracketIdentifier(field.Name), strings.Join(quotedValues, ", "))
		if field.Check != "" {
			enumCheck = fmt.Sprintf("(%s) AND %s", field.Check, enumCheck)
		}
		newField.Check = enumCheck
	case platform.Oracle:
		newField.Type = "VARCHAR2(255)"
		// The column reference is spelled by the same rule the Oracle renderer
		// spells the declaration with, because Oracle refuses a CHECK whose
		// spelling disagrees with the column it constrains: measured,
		// `"view_count" NUMBER(10) CHECK (view_count >= 0)` answers ORA-00904
		// while the two agreeing forms are accepted. sqlident.Ident is what
		// escapeIdentifier there calls, so the two cannot drift apart.
		enumCheck := fmt.Sprintf("%s IN (%s)",
			sqlident.Ident(platform.Oracle, field.Name), strings.Join(quotedValues, ", "))
		if field.Check != "" {
			enumCheck = fmt.Sprintf("(%s) AND %s", field.Check, enumCheck)
		}
		newField.Check = enumCheck
	}
	return newField
}

// FromField converts a schemamodel.Field to an ast.ColumnNode with comprehensive attribute mapping.
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
//	field := schemamodel.Field{
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
//	field := schemamodel.Field{
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
//	field := schemamodel.Field{
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
//	field := schemamodel.Field{
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
// typeIsDeclaredTextSurvives reports whether the column still carries the type
// a catalog stored verbatim.
//
// A platform override or an enum substitution replaces the type with one
// somebody chose for this target, and the fact is about the ORIGINAL: once the
// spelling has been replaced there is nothing verbatim left to protect, and a
// renderer that skipped canonicalization would write the substituted type
// unchanged. It is the rule [typeRawSQLSurvives] applies for the same reason.
func typeIsDeclaredTextSurvives(field schemamodel.Field, declaredType string) bool {
	return field.TypeIsDeclaredText && field.Type == declaredType
}

func FromField(field schemamodel.Field, enums []schemamodel.Enum, targetPlatform string) *ast.ColumnNode {
	declaredType := field.Type
	field = schemaprep.EffectiveFieldForPlatform(field, targetPlatform)
	field = handleEnumTypes(field, enums, targetPlatform)

	column := ast.NewColumn(field.Name, field.Type)
	column.TypeRawSQL = typeRawSQLSurvives(field, declaredType)
	column.TypeIsDeclaredText = typeIsDeclaredTextSurvives(field, declaredType)
	column.EnumType = declaredEnum(declaredType, enums) != nil

	// Set nullable - only override default if explicitly set to false
	// The default behavior should be nullable=true (which ast.NewColumn already sets)
	if !field.Nullable {
		column.SetNotNull()
	}
	// Carried whatever the nullability is, because a name on a NULLABLE column
	// names no constraint and the renderer refuses it -- a refusal that could
	// not fire while the name stopped here (stokaro/ptah#2161,
	// stokaro/ptah#2590).
	//
	// One write, not two. Assigning the guarded name inside the branch above and
	// then this one unconditionally left the second overwriting the first, so
	// keptNotNullConstraintName never took effect and the key-column drop it
	// exists for never happened.
	column.SetNotNullConstraintName(keptNotNullConstraintName(field))

	// Set constraints
	if field.Primary {
		column.SetPrimary()
	}
	if field.Unique {
		column.SetUnique()
	}
	if field.AutoInc {
		column.SetAutoIncrement()
		// SQL Server spells an identity column as AutoInc and never carries a
		// generation mode, which is PostgreSQL's word. Gating the range on the
		// mode alone left IDENTITY(seed, increment) reachable only for columns
		// that declare one, so a seed read off SQL Server never arrived
		// (stokaro/ptah#2196).
		column.IdentityStart = field.IdentityStart
		column.IdentityIncrement = field.IdentityIncrement
	}
	if field.IdentityGeneration != "" {
		column.SetIdentity(field.IdentityGeneration, field.IdentityStart, field.IdentityIncrement)
		column.SetIdentityOptions(field.IdentityOptions)
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
		column.SetGenerated(field.GeneratedExpression, defaultGeneratedKind(field, targetPlatform))
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
	if fkRef := foreignKeyReference(field.Foreign); fkRef != nil {
		column.SetForeignKey(fkRef.Table, fkRef.Column, field.ForeignKeyName)
		column.ForeignKey.OnDelete = field.OnDelete
		column.ForeignKey.OnUpdate = field.OnUpdate
		column.ForeignKey.Deferrable = field.Deferrable
		column.ForeignKey.Initially = field.Initially
	}

	return column
}

// FromFieldWithoutForeignKeys converts a schemamodel.Field to an AST ColumnNode without foreign key constraints.
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
func FromFieldWithoutForeignKeys(field schemamodel.Field, enums []schemamodel.Enum, targetPlatform string) *ast.ColumnNode {
	// Apply platform-specific overrides if available
	declaredType := field.Type
	field = schemaprep.EffectiveFieldForPlatform(field, targetPlatform)
	field = handleEnumTypes(field, enums, targetPlatform)

	// Create column with basic properties
	column := ast.NewColumn(field.Name, field.Type)
	column.TypeRawSQL = typeRawSQLSurvives(field, declaredType)
	column.TypeIsDeclaredText = typeIsDeclaredTextSurvives(field, declaredType)
	column.EnumType = declaredEnum(declaredType, enums) != nil

	// Set nullable (default is true, so only set if false)
	if !field.Nullable {
		column.SetNotNull()
	}
	// Carried unconditionally, for the reason [FromField] states.
	column.SetNotNullConstraintName(keptNotNullConstraintName(field))

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
		// SQL Server spells an identity column as AutoInc and never carries a
		// generation mode, which is PostgreSQL's word. Gating the range on the
		// mode alone left IDENTITY(seed, increment) reachable only for columns
		// that declare one, so a seed read off SQL Server never arrived
		// (stokaro/ptah#2196).
		column.IdentityStart = field.IdentityStart
		column.IdentityIncrement = field.IdentityIncrement
	}
	if field.IdentityGeneration != "" {
		column.SetIdentity(field.IdentityGeneration, field.IdentityStart, field.IdentityIncrement)
		column.SetIdentityOptions(field.IdentityOptions)
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
		column.SetGenerated(field.GeneratedExpression, defaultGeneratedKind(field, targetPlatform))
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

func applyTablePlatformOverrides(createTable *ast.CreateTableNode, table schemamodel.Table, targetPlatform string) schemamodel.Table {
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

	platformOverrides, exists := schemaprep.PlatformOverrideGroup(table.Overrides, targetPlatform)
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

type fieldConverter func(schemamodel.Field, []schemamodel.Enum, string) *ast.ColumnNode

// FromTable converts a schemamodel.Table to an ast.CreateTableNode with all associated columns and constraints.
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
//	table := schemamodel.Table{
//		StructName: "User",
//		Name:       "users",
//		Comment:    "Application users",
//	}
//	fields := []schemamodel.Field{
//		{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
//		{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false, Unique: true},
//	}
//	createTable := FromTable(table, fields, nil)
//
// Table with composite primary key:
//
//	table := schemamodel.Table{
//		StructName: "UserRole",
//		Name:       "user_roles",
//		PrimaryKey: []string{"user_id", "role_id"},
//	}
//	fields := []schemamodel.Field{
//		{StructName: "UserRole", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
//		{StructName: "UserRole", Name: "role_id", Type: "INTEGER", Foreign: "roles(id)"},
//	}
//	createTable := FromTable(table, fields, nil)
//
// MySQL table with engine specification:
//
//	table := schemamodel.Table{
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
//	table := schemamodel.Table{
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
func FromTable(table schemamodel.Table, fields []schemamodel.Field, enums []schemamodel.Enum, targetPlatform string) *ast.CreateTableNode {
	return fromTableWithFieldConverter(table, fields, enums, targetPlatform, FromField)
}

// FromTableWithConstraints is [FromTable] for a caller that also knows the
// table-level constraints the declaration owns.
//
// The difference is the generated names of the table's `checks` entries. Without
// the list, [schemaprep.TableCheckConstraints] names them blind and can hand a synthesized
// CHECK the name an explicit constraint already answers to: a table declaring
// `checks = ["price > 0"]` beside a check named `products_check` over
// `stock >= 0` produced two constraints under one name, and PostgreSQL refuses
// that with `constraint "products_check" for relation "products" already
// exists`.
//
// A planner that adds explicit constraints through their own ALTER statements
// still has to name what goes INSIDE the CREATE around them, so it wants this
// rather than [FromTable] even though it renders the constraints separately.
func FromTableWithConstraints(
	table schemamodel.Table,
	fields []schemamodel.Field,
	enums []schemamodel.Enum,
	targetPlatform string,
	declared []schemamodel.Constraint,
) *ast.CreateTableNode {
	node := fromTableWithFieldConverter(table, fields, enums, targetPlatform, FromField)
	replaceSynthesizedTableChecks(node, table, declared)
	return node
}

func fromTableWithFieldConverter(
	table schemamodel.Table,
	fields []schemamodel.Field,
	enums []schemamodel.Enum,
	targetPlatform string,
	convertField fieldConverter,
) *ast.CreateTableNode {
	createTable := ast.NewCreateTable(renderTableName(table, targetPlatform))

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
	if isSQLiteTarget(targetPlatform) {
		if newTable.WithoutRowID {
			createTable.SetOption("WITHOUT_ROWID", "true")
		}
		if newTable.Strict {
			createTable.SetOption("STRICT", "true")
		}
		// A virtual table is a different statement, not a trailing option, so
		// the SQLite renderer branches on this key before it writes anything.
		// The only producer of a non-empty VirtualModule is the SQLite reader,
		// so a virtual table never reaches another dialect's renderer.
		// See stokaro/ptah#1028.
		if newTable.VirtualModule != "" {
			createTable.SetOption(ast.SQLiteVirtualModuleOption, newTable.VirtualModule)
			createTable.SetOption(ast.SQLiteVirtualArgumentsOption, newTable.VirtualArguments)
		}
	}
	createTable.Partition = toASTPartition(newTable.Partition)
	// Cloned rather than shared: the node travels to a renderer and a planner
	// that must not be able to reach back through a pointer into the schema
	// this was built from (stokaro/ptah#1027).
	createTable.RowTTL = newTable.RowTTL.Clone()
	createTable.RowDeletionPolicy = newTable.RowDeletionPolicy.Clone()
	// Raw SQL the author asked to be appended to CREATE TABLE. It is carried
	// verbatim; see [go.5x5.cz/ptah/core/ast.CreateTableNode.CustomSQL] for why
	// it is not an Options entry (stokaro/ptah#2590).
	createTable.CustomSQL = newTable.CustomSQL

	// Add columns for fields that belong to this table
	tableLevelPK := tableNeedsPrimaryKeyConstraint(newTable, fields)
	for _, field := range fields {
		if field.StructName == table.StructName {
			if tableLevelPK && slices.Contains(newTable.PrimaryKey, field.Name) {
				field.Primary = false
			}
			field = schemaprep.WithDefaultFieldForeignKeyName(newTable.Name, field, targetPlatform)
			createTable.AddColumn(convertField(field, enums, targetPlatform))
		}
	}

	// Add composite primary key constraint if specified
	if tableLevelPK {
		constraint := newPrimaryKeyConstraint(newTable)
		createTable.AddConstraint(constraint)
	}
	// Every entry, and named, because this conversion never sees the schema's
	// constraint list. [addTableConstraints] is the only caller that has both,
	// and it re-derives these once the explicit constraints are visible.
	//
	// One emission, not two. A second, UNNAMED pass over the same table.Checks
	// stood here beside this one, so every declared check rendered twice --
	// `CHECK (price > 0)` and `CONSTRAINT "products_check" CHECK (price > 0)`
	// in one CREATE TABLE. The unnamed copy is the shape this change replaced,
	// because a server-invented name is what stopped the schema converging.
	for _, check := range schemaprep.TableCheckConstraints(newTable, nil) {
		createTable.AddConstraint(FromConstraint(check))
	}

	return createTable
}

func renderTableName(table schemamodel.Table, targetPlatform string) string {
	// SQLite catalog identifiers are already parsed. Preserve their exact
	// components for virtual tables: quoted leading or trailing whitespace is
	// part of the identity, and normalizing it would recreate another object.
	if isSQLiteTarget(targetPlatform) && table.VirtualModule != "" {
		return tableref.CanonicalExact(table.Schema, table.Name)
	}
	if strings.Contains(table.Schema, ".") || strings.Contains(table.Name, ".") {
		return sqlident.Qualified(targetPlatform, table.Schema, table.Name)
	}
	return table.QualifiedName()
}

func fromTableWithoutForeignKeys(
	table schemamodel.Table,
	fields []schemamodel.Field,
	enums []schemamodel.Enum,
	targetPlatform string,
) *ast.CreateTableNode {
	return fromTableWithFieldConverter(table, fields, enums, targetPlatform, FromFieldWithoutForeignKeys)
}

func toASTPartition(partition *schemamodel.PartitionSpec) *ast.PartitionSpec {
	if partition == nil {
		return nil
	}
	parts := make([]ast.PartitionPart, 0, len(partition.Parts))
	for _, part := range partition.Parts {
		parts = append(parts, ast.PartitionPart{Name: part.Name, Expr: part.Expr})
	}
	return &ast.PartitionSpec{Type: partition.Type, Parts: parts}
}

func tableNeedsPrimaryKeyConstraint(table schemamodel.Table, fields []schemamodel.Field) bool {
	// A NAME is a reason on its own. An inline `PRIMARY KEY` has nowhere to
	// carry one, so a named single-column key written that way reached the
	// server as a generated name and nothing reported the difference
	// (stokaro/ptah#2180).
	if table.PrimaryKeyName != "" && (len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	if len(table.PrimaryKeyInclude) > 0 && (len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	// UNIQUE on the same column is a reason of the same kind. The column
	// spelling has one slot for a key and the source declared two, and folding
	// them is not a formatting choice: measured on MariaDB 11.8,
	// `a INT PRIMARY KEY UNIQUE` produces the primary key ALONE, while the
	// source's own `a INT UNIQUE, PRIMARY KEY (a)` produces the primary key and
	// a secondary unique index. MySQL 8.4 keeps both either way, so writing the
	// key as a table constraint is what makes one rendering right on both --
	// and it is what the source wrote (stokaro/ptah#2787).
	if primaryKeyColumnIsUnique(table, fields) {
		return true
	}
	if len(table.PrimaryKeyParts) > 0 {
		return len(table.PrimaryKeyParts) > 1 || primaryKeyPartsHaveAttributes(table.PrimaryKeyParts)
	}
	return len(table.PrimaryKey) > 1
}

// primaryKeyColumnIsUnique reports whether a single-column primary key names a
// column the same table declares UNIQUE.
//
// Single-column only, because a composite key already renders as a table
// constraint and a column inside one carries no key of its own to collide with.
// The name comparison is the one the surrounding conversion uses: a field
// belongs to this table when its StructName matches.
func primaryKeyColumnIsUnique(table schemamodel.Table, fields []schemamodel.Field) bool {
	if len(table.PrimaryKey) != 1 {
		return false
	}
	for _, field := range fields {
		if field.StructName == table.StructName && field.Name == table.PrimaryKey[0] {
			return field.Unique
		}
	}
	return false
}

func primaryKeyPartsHaveAttributes(parts []schemamodel.PrimaryKeyPart) bool {
	for _, part := range parts {
		if part.Prefix != "" || part.Desc {
			return true
		}
	}
	return false
}

func newPrimaryKeyConstraint(table schemamodel.Table) *ast.ConstraintNode {
	if len(table.PrimaryKeyParts) == 0 {
		constraint := ast.NewPrimaryKeyConstraint(table.PrimaryKey...)
		constraint.Name = table.PrimaryKeyName
		constraint.IncludeColumns = table.PrimaryKeyInclude
		return constraint
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
		Type:           ast.PrimaryKeyConstraint,
		Name:           table.PrimaryKeyName,
		Columns:        columns,
		ColumnParts:    columnParts,
		IncludeColumns: table.PrimaryKeyInclude,
	}
}

// FromConstraint converts a schemamodel.Constraint to an AST table constraint.
//
// The comment is attached after the type switch rather than inside each arm.
// Every arm builds its own node, so a fact carried per arm is one a new
// constraint type silently loses -- which is how this converter lost the
// primary key's name and INCLUDE payload (stokaro/ptah#2538).
func FromConstraint(constraint schemamodel.Constraint) *ast.ConstraintNode {
	node := fromConstraintByType(constraint)
	if node == nil {
		return nil
	}
	node.Comment = constraint.Comment
	return node
}

// fromConstraintByType builds the node one constraint type needs.
func fromConstraintByType(constraint schemamodel.Constraint) *ast.ConstraintNode {
	switch strings.ToUpper(constraint.Type) {
	case "PRIMARY KEY":
		// Name and IncludeColumns are carried here for the same reason every
		// other arm below carries them: this one dropped both, so a declared
		// `CONSTRAINT pk_accounts PRIMARY KEY (a) INCLUDE (payload)` rendered as
		// a bare, unnamed `PRIMARY KEY ("a")` on every dialect -- PostgreSQL
		// included, where the server takes the payload. The table-carried
		// spelling already keeps both (newPrimaryKeyConstraint above), so this
		// was the same key losing them for having been written as a constraint
		// (stokaro/ptah#2538; the database-to-model direction was #2199).
		//
		// NullsDistinct is deliberately absent: it is a UNIQUE clause, and
		// PostgreSQL rejects it on a primary key.
		node := ast.NewPrimaryKeyConstraint(constraint.Columns...)
		node.Name = constraint.Name
		node.IncludeColumns = append([]string(nil), constraint.IncludeColumns...)
		return node
	case "UNIQUE":
		node := ast.NewUniqueConstraint(constraint.Name, constraint.Columns...)
		node.IncludeColumns = append([]string(nil), constraint.IncludeColumns...)
		node.NullsDistinct = cloneBoolPtr(constraint.NullsDistinct)
		return node
	case "FOREIGN KEY":
		return ast.NewForeignKeyConstraint(constraint.Name, constraint.Columns, &ast.ForeignKeyRef{
			Table:      constraint.ForeignTable,
			Column:     constraint.ForeignColumn,
			Columns:    constraint.ForeignColumns,
			OnDelete:   constraint.OnDelete,
			OnUpdate:   constraint.OnUpdate,
			Name:       constraint.Name,
			Deferrable: constraint.Deferrable,
			Initially:  constraint.Initially,
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

type tableConstraintMode int

const (
	tableConstraintsWithoutForeignKeys tableConstraintMode = iota
	tableConstraintsWithForeignKeys
)

func addTableConstraints(
	createTable *ast.CreateTableNode,
	table schemamodel.Table,
	constraints []schemamodel.Constraint,
	mode tableConstraintMode,
	targetPlatform string,
) {
	// The table conversion rendered every `checks` entry blind, because it never
	// sees this list. Here it is visible, so the synthesized checks are derived
	// again from the same function the comparator calls -- one namer with one
	// input, rather than a rendered name decided without the explicit
	// constraints and a compared name decided with them.
	replaceSynthesizedTableChecks(createTable, table, constraints)

	for _, constraint := range constraints {
		if !schemaprep.ConstraintBelongsToTable(constraint, table) {
			continue
		}
		if schemaprep.IsForeignKeyConstraint(constraint) && mode != tableConstraintsWithForeignKeys {
			continue
		}
		constraint = schemaprep.WithDefaultConstraintForeignKeyName(table.Name, constraint, targetPlatform)

		node := FromConstraint(constraint)
		if node != nil {
			createTable.AddConstraint(node)
		}
	}
}

// replaceSynthesizedTableChecks re-derives the table's `checks` entries now that
// the explicit constraint list is visible.
//
// The table conversion produced them with a nil list, so both decisions it makes
// were taken blind: whether an explicit CHECK already spells the expression, and
// which generated name is still free. Deriving them again here, through the same
// [schemaprep.TableCheckConstraints] the comparator calls, is what keeps the rendered name
// and the compared name equal. Two lists of "already spelled" or two namers
// would agree when written and stop agreeing the moment one was extended.
//
// Every table-level CHECK node present at this point is one of those synthesized
// entries -- a column's own check lives on the column, and this runs before any
// explicit constraint is added -- so replacing the whole set is exactly the
// set this function owns.
func replaceSynthesizedTableChecks(
	createTable *ast.CreateTableNode,
	table schemamodel.Table,
	declared []schemamodel.Constraint,
) {
	kept := make([]*ast.ConstraintNode, 0, len(createTable.Constraints))
	for _, node := range createTable.Constraints {
		if node.Type == ast.CheckConstraint {
			continue
		}
		kept = append(kept, node)
	}
	createTable.Constraints = kept

	for _, check := range schemaprep.TableCheckConstraints(table, declared) {
		createTable.AddConstraint(FromConstraint(check))
	}
}

// FromIndex converts a schemamodel.Index to an ast.IndexNode for database index creation.
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
//	index := schemamodel.Index{
//		Name:       "idx_users_email",
//		StructName: "users",
//		Fields:     []string{"email"},
//		Comment:    "Index for email lookups",
//	}
//	indexNode := FromIndex(index)
//
// Unique composite index:
//
//	index := schemamodel.Index{
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
func FromIndex(index schemamodel.Index) *ast.IndexNode {
	// Use TableName if specified, otherwise fall back to StructName
	tableName := index.TableName
	if tableName == "" {
		tableName = index.StructName
	}

	indexNode := ast.NewIndex(index.Name, tableName, indexFields(index)...)
	if len(index.Parts) > 0 {
		indexNode.SetParts(toASTIndexParts(index.Parts))
	}
	indexNode.IncludeColumns = index.IncludeColumns
	indexNode.NullsDistinct = cloneBoolPtr(index.NullsDistinct)
	indexNode.StorageParams = maps.Clone(index.StorageParams)

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

// FromExtension converts a schemamodel.Extension to an ast.ExtensionNode for PostgreSQL extension creation.
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
//   - PostgreSQL installation schema
//   - IF NOT EXISTS clause support
//   - Version specification for specific extension versions
//   - Extension comments for documentation
//
// # Examples
//
// Basic extension:
//
//	extension := schemamodel.Extension{
//		Name:        "pg_trgm",
//		IfNotExists: true,
//		Comment:     "Enable trigram similarity search",
//	}
//	extensionNode := FromExtension(extension)
//
// Extension with version:
//
//	extension := schemamodel.Extension{
//		Name:        "postgis",
//		Schema:      "extensions",
//		Version:     "3.0",
//		IfNotExists: true,
//		Comment:     "Geographic data support",
//	}
//	extensionNode := FromExtension(extension)
//
// # Return Value
//
// Returns a fully configured *ast.ExtensionNode ready for SQL generation.
// The node contains the extension name, installation schema, version, and all specified options.
func FromExtension(extension schemamodel.Extension) *ast.ExtensionNode {
	extensionNode := ast.NewExtension(extension.Name)
	if extension.Schema != "" {
		extensionNode.SetSchema(extension.Schema)
	}

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

// FromEnum converts a schemamodel.Enum to an ast.EnumNode for database enum type creation.
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
//	enum := schemamodel.Enum{
//		Name:   "status_type",
//		Values: []string{"active", "inactive", "pending"},
//	}
//	enumNode := FromEnum(enum)
//
// User role enum:
//
//	enum := schemamodel.Enum{
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
func FromEnum(enum schemamodel.Enum) *ast.EnumNode {
	return ast.NewEnum(enum.QualifiedName(), enum.Values...)
}

// qualifyTypeName returns schema.name when schema is set, or name otherwise. The
// renderer splits on "." to escape each part, so the qualified form is safe to
// pass as a CreateTypeNode name.
func qualifyTypeName(schema, name string) string {
	return schemamodel.QualifyTableName(schema, name)
}

// FromDomain converts a schemamodel.Domain into a CreateTypeNode wrapping a domain
// type definition.
func FromDomain(domain schemamodel.Domain) *ast.CreateTypeNode {
	domainDef := ast.NewDomainTypeDef(domain.BaseType)
	if domain.NotNull {
		domainDef.SetNotNull()
	}
	if domain.Default != "" {
		domainDef.SetDefault(domain.Default)
	}
	if domain.DefaultExpr != "" {
		domainDef.SetDefaultExpression(domain.DefaultExpr)
	}
	if domain.Check != "" {
		domainDef.SetCheck(domain.Check)
	}
	node := ast.NewCreateType(qualifyTypeName(domain.Schema, domain.Name), domainDef)
	if domain.Comment != "" {
		node.SetComment(domain.Comment)
	}
	return node
}

// FromCompositeType converts a schemamodel.CompositeType into a CreateTypeNode
// wrapping a composite type definition.
func FromCompositeType(composite schemamodel.CompositeType) *ast.CreateTypeNode {
	fields := make([]*ast.CompositeField, 0, len(composite.Fields))
	for _, field := range composite.Fields {
		fields = append(fields, &ast.CompositeField{Name: field.Name, Type: field.Type})
	}
	node := ast.NewCreateType(qualifyTypeName(composite.Schema, composite.Name), ast.NewCompositeTypeDef(fields...))
	if composite.Comment != "" {
		node.SetComment(composite.Comment)
	}
	return node
}

// FromRange converts a schemamodel.Range into a CreateTypeNode wrapping a range
// type definition.
func FromRange(rangeType schemamodel.Range) *ast.CreateTypeNode {
	rangeDef := ast.NewRangeTypeDef(rangeType.Subtype)
	if rangeType.SubtypeOpClass != "" {
		rangeDef.SetSubtypeOpClass(rangeType.SubtypeOpClass)
	}
	if rangeType.Collation != "" {
		rangeDef.SetCollation(rangeType.Collation)
	}
	if rangeType.Canonical != "" {
		rangeDef.SetCanonical(rangeType.Canonical)
	}
	if rangeType.SubtypeDiff != "" {
		rangeDef.SetSubtypeDiff(rangeType.SubtypeDiff)
	}
	node := ast.NewCreateType(qualifyTypeName(rangeType.Schema, rangeType.Name), rangeDef)
	if rangeType.Comment != "" {
		node.SetComment(rangeType.Comment)
	}
	return node
}

// FromFunction converts a schemamodel.Function to an ast.CreateFunctionNode.
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
func FromFunction(function schemamodel.Function) *ast.CreateFunctionNode {
	functionNode := ast.NewCreateFunction(function.Name).
		SetKind(function.Kind).
		SetParameters(function.Parameters).
		SetReturns(function.Returns).
		SetLanguage(function.Language).
		SetSecurity(function.Security).
		SetVolatility(function.Volatility).
		SetSettings(function.Settings).
		SetBody(function.Body).
		SetComment(function.Comment)

	return functionNode
}

// FromSequence converts a schemamodel.Sequence into a CreateSequenceNode.
//
// The returned node faithfully carries every declared option, including OWNED
// BY. Callers that generate a full schema (see CollectDatabase) deliberately defer
// the OWNED BY association to a separate post-table ALTER SEQUENCE, because a
// sequence referenced by a column DEFAULT must be created before its table
// while OWNED BY requires the table to already exist.
func FromSequence(sequence schemamodel.Sequence) *ast.CreateSequenceNode {
	sequenceNode := ast.NewCreateSequence(sequence.Name)
	if sequence.Schema != "" {
		sequenceNode.SetSchema(sequence.Schema)
	}
	if sequence.IfNotExists {
		sequenceNode.SetIfNotExists()
	}
	if sequence.AsType != "" {
		sequenceNode.SetAs(sequence.AsType)
	}
	if sequence.Start != nil {
		sequenceNode.SetStart(*sequence.Start)
	}
	if sequence.Increment != nil {
		sequenceNode.SetIncrement(*sequence.Increment)
	}
	if sequence.MinValue != nil {
		sequenceNode.SetMinValue(*sequence.MinValue)
	}
	if sequence.MaxValue != nil {
		sequenceNode.SetMaxValue(*sequence.MaxValue)
	}
	if sequence.Cache != nil {
		sequenceNode.SetCache(*sequence.Cache)
	}
	if sequence.Cycle {
		sequenceNode.SetCycle(true)
	}
	if sequence.OwnedBy != "" {
		sequenceNode.SetOwnedBy(sequence.OwnedBy)
	}
	if sequence.Comment != "" {
		sequenceNode.SetComment(sequence.Comment)
	}
	return sequenceNode
}

// sequenceOwnershipNode returns an ALTER SEQUENCE ... OWNED BY node for a
// sequence that declares an owner, or nil when it does not. It exists so schema
// generation can emit the ownership association after the owning table is
// created.
func sequenceOwnershipNode(sequence schemamodel.Sequence) *ast.AlterSequenceNode {
	if sequence.OwnedBy == "" {
		return nil
	}
	node := ast.NewAlterSequence(sequence.Name).SetOwnedBy(sequence.OwnedBy)
	if sequence.Schema != "" {
		node.SetSchema(sequence.Schema)
	}
	return node
}

// FromView converts a schemamodel.View to an ast.CreateViewNode.
func FromView(view schemamodel.View) *ast.CreateViewNode {
	viewNode := ast.NewCreateView(view.Name).
		SetBody(view.Body).
		SetWithCheck(view.WithCheck).
		SetComment(view.Comment)
	viewNode.Attributes = view.Attributes
	return viewNode
}

// FromSynonym converts a schemamodel.Synonym to an ast.CreateSynonymNode.
//
// The alias is qualified here and the target is not. The alias is an object
// this schema declares, so it belongs in the schema the declaration named; the
// target is written exactly as it was given, because its part count is what
// tells SQL Server whether it names this database, another one, or a linked
// server, and adding a qualifier would silently turn a remote reference into a
// local one.
func FromSynonym(synonym schemamodel.Synonym) *ast.CreateSynonymNode {
	return ast.NewCreateSynonym(synonym.QualifiedName()).
		SetTarget(synonym.Target).
		SetComment(synonym.Comment)
}

// appendSynonymStatements adds a CREATE SYNONYM node for each declared synonym.
func appendSynonymStatements(visit func(ast.Node) error, synonyms []schemamodel.Synonym) error {
	for _, synonym := range synonyms {
		if err := visit(FromSynonym(synonym)); err != nil {
			return err
		}
	}
	return nil
}

// FromHypertable converts a schemamodel.Hypertable into the call that makes one.
func FromHypertable(hypertable schemamodel.Hypertable) *ast.CreateHypertableNode {
	return ast.NewCreateHypertable(hypertable.Table, hypertable.Column).
		SetChunkInterval(hypertable.ChunkInterval).
		SetIfNotExists(hypertable.IfNotExists).
		SetComment(hypertable.Comment)
}

// appendHypertableStatements adds one create_hypertable call per declaration.
func appendHypertableStatements(visit func(ast.Node) error, hypertables []schemamodel.Hypertable) error {
	for _, hypertable := range hypertables {
		if err := visit(FromHypertable(hypertable)); err != nil {
			return err
		}
	}
	return nil
}

// FromContinuousAggregate converts a schemamodel.ContinuousAggregate into the
// statement that creates one.
//
// WITH NO DATA is the default the node carries, and it is deliberate: creating
// the aggregate with data materializes the whole history of the hypertable
// underneath it, which is an unbounded amount of work a schema change should
// not start on its own. A refresh is an operation someone runs, not a side
// effect of CREATE.
func FromContinuousAggregate(aggregate schemamodel.ContinuousAggregate) *ast.CreateContinuousAggregateNode {
	return ast.NewCreateContinuousAggregate(aggregate.Name, aggregate.Body).
		SetSchema(aggregate.Schema).
		SetMaterializedOnly(aggregate.MaterializedOnly).
		SetComment(aggregate.Comment)
}

// appendContinuousAggregateStatements adds one CREATE MATERIALIZED VIEW per
// declared aggregate.
func appendContinuousAggregateStatements(
	visit func(ast.Node) error,
	aggregates []schemamodel.ContinuousAggregate,
) error {
	for _, aggregate := range aggregates {
		if err := visit(FromContinuousAggregate(aggregate)); err != nil {
			return err
		}
	}
	return nil
}

// fromExtendedProperty converts a schemamodel.ExtendedProperty into the node that
// writes it.
//
// The operation is always an add. An update is what a COMPARISON produces, from
// a live value that differs; a declaration rendered on its own is a schema
// being created, where nothing is there to update.
//
// The address parts are passed unqualified and unquoted. They reach the
// renderer as string literals rather than identifiers, because that is what
// sp_addextendedproperty takes -- see VisitExtendedProperty for what quoting
// them would write.
func fromExtendedProperty(property schemamodel.ExtendedProperty) *ast.ExtendedPropertyNode {
	return ast.NewExtendedProperty(ast.ExtendedPropertyAdd, property.Name).
		SetOwner(property.Schema, property.Table, property.Column).
		SetValue(property.Value).
		SetComment(property.Comment)
}

// appendExtendedPropertyStatements adds one add-property node per declaration.
func appendExtendedPropertyStatements(
	visit func(ast.Node) error,
	properties []schemamodel.ExtendedProperty,
) error {
	for _, property := range properties {
		if err := visit(fromExtendedProperty(property)); err != nil {
			return err
		}
	}
	return nil
}

// FromMaterializedView converts a schemamodel.MaterializedView to an
// ast.CreateMaterializedViewNode.
func FromMaterializedView(view schemamodel.MaterializedView) *ast.CreateMaterializedViewNode {
	node := ast.NewCreateMaterializedView(view.Name).
		SetBody(view.Body).
		SetComment(view.Comment)
	// Cloned rather than shared: a node handed to a renderer must not be a
	// window onto the schema it came from (stokaro/ptah#1802).
	node.Refresh = view.Refresh.Clone()
	return node
}

func appendForeignKeyConstraintStatements(
	visit func(ast.Node) error,
	tables []schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
	targetPlatform string,
) error {
	if err := appendFieldForeignKeyConstraintStatements(visit, tables, fields, targetPlatform); err != nil {
		return err
	}
	return appendTableForeignKeyConstraintStatements(visit, tables, constraints, targetPlatform)
}

func appendFieldForeignKeyConstraintStatements(
	visit func(ast.Node) error,
	tables []schemamodel.Table,
	fields []schemamodel.Field,
	targetPlatform string,
) error {
	for _, table := range tables {
		for _, field := range fields {
			if field.StructName != table.StructName {
				continue
			}
			field = schemaprep.EffectiveFieldForPlatform(field, targetPlatform)
			if field.Foreign == "" {
				continue
			}
			field = schemaprep.WithDefaultFieldForeignKeyName(table.Name, field, targetPlatform)
			fkRef := foreignKeyReference(field.Foreign)
			if fkRef == nil {
				continue
			}
			fkRef.Table = tablelookup.ResolveReference(tables, table, fkRef.Table)
			fkRef.OnDelete = field.OnDelete
			fkRef.OnUpdate = field.OnUpdate
			fkRef.Deferrable = field.Deferrable
			fkRef.Initially = field.Initially
			fkRef.Name = field.ForeignKeyName
			if err := visit(&ast.AlterTableNode{
				Name: table.QualifiedName(),
				Operations: []ast.AlterOperation{
					&ast.AddConstraintOperation{
						Constraint: ast.NewForeignKeyConstraint(field.ForeignKeyName, []string{field.Name}, fkRef),
					},
				},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendTableForeignKeyConstraintStatements(
	visit func(ast.Node) error,
	tables []schemamodel.Table,
	constraints []schemamodel.Constraint,
	targetPlatform string,
) error {
	for _, table := range tables {
		for _, constraint := range constraints {
			if !schemaprep.ConstraintBelongsToTable(constraint, table) || !schemaprep.IsForeignKeyConstraint(constraint) {
				continue
			}
			constraint = schemaprep.WithDefaultConstraintForeignKeyName(table.Name, constraint, targetPlatform)
			constraint.ForeignTable = tablelookup.ResolveReference(tables, table, constraint.ForeignTable)
			node := FromConstraint(constraint)
			if node == nil {
				continue
			}
			if err := visit(&ast.AlterTableNode{
				Name: table.QualifiedName(),
				Operations: []ast.AlterOperation{
					&ast.AddConstraintOperation{Constraint: node},
				},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// FromTrigger converts a schemamodel.Trigger to an ast.CreateTriggerNode.
func FromTrigger(trigger schemamodel.Trigger) *ast.CreateTriggerNode {
	trigger.Canonicalize()
	// A trigger that names an existing function keeps that name; the renderer
	// then emits only the CREATE TRIGGER, because the function is not Ptah's to
	// define. Otherwise the deterministic Ptah function name is used and the
	// renderer emits the function alongside the trigger.
	functionName := trigger.FunctionName()
	if trigger.ExecuteFunction != "" {
		functionName = trigger.ExecuteFunction
	}
	triggerNode := ast.NewCreateTrigger(trigger.Name, trigger.Table).
		SetTiming(trigger.Timing).
		SetEvent(trigger.Event).
		SetForEach(trigger.ForEach).
		SetBody(trigger.Body).
		SetFunctionName(functionName).
		SetComment(trigger.Comment)
	if trigger.ExecuteFunction != "" {
		triggerNode.SetExternalFunction()
	}
	return triggerNode
}

// FromRLSPolicy converts a schemamodel.RLSPolicy to an ast.CreatePolicyNode.
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
func FromRLSPolicy(policy schemamodel.RLSPolicy) *ast.CreatePolicyNode {
	policyNode := ast.NewCreatePolicy(policy.Name, policy.Table).
		SetPolicyFor(policy.PolicyFor).
		SetToRoles(policy.ToRoles).
		SetUsingExpression(policy.UsingExpression).
		SetWithCheckExpression(policy.WithCheckExpression).
		SetComment(policy.Comment)

	return policyNode
}

// FromRLSEnabledTable converts a schemamodel.RLSEnabledTable to an ast.AlterTableEnableRLSNode.
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
func FromRLSEnabledTable(rlsEnabled schemamodel.RLSEnabledTable) *ast.AlterTableEnableRLSNode {
	rlsNode := ast.NewAlterTableEnableRLS(rlsEnabled.Table).
		SetComment(rlsEnabled.Comment)

	return rlsNode
}

// FromRole converts a schemamodel.Role to an ast.CreateRoleNode.
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
func FromRole(role schemamodel.Role) *ast.CreateRoleNode {
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

// FromGrant converts a schemamodel.Grant to an ast.GrantPrivilegeNode.
func FromGrant(grant schemamodel.Grant) *ast.GrantPrivilegeNode {
	grant.Canonicalize()
	objectType := "TABLE"
	objectName := grant.OnTable
	switch {
	case grant.OnSchema != "":
		objectType = "SCHEMA"
		objectName = grant.OnSchema
	case grant.OnSequence != "":
		objectType = "SEQUENCE"
		objectName = grant.OnSequence
	}
	return ast.NewGrantPrivilege(grant.Role, objectType, objectName, grant.Privileges).
		SetWithOption(grant.WithOption).
		SetComment(grant.Comment)
}

// WalkDatabase converts a complete schemamodel.Database to AST nodes and visits
// them in executable order without materializing a second whole-schema
// representation.
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
//  2. Extension definitions
//  3. Enum type definitions (CREATE TYPE statements)
//  4. Table definitions (CREATE TABLE statements) with embedded fields processed, but without foreign keys
//  5. PostgreSQL roles and functions
//  6. Unique index definitions (CREATE UNIQUE INDEX statements), plus all
//     MySQL-family indexes required before foreign-key creation
//  7. Foreign key constraints (ALTER TABLE statements)
//  8. Dialect-specific objects such as views, RLS policies, grants, and triggers
//  9. Non-unique index definitions (CREATE INDEX statements)
//
// This ordering ensures that:
//   - Schemas are created before tables that reference them
//   - Extensions are created before tables, indexes, or functions that may use them
//   - Enum types are created before tables that reference them
//   - PostgreSQL functions are created before indexes that may use them
//   - Tables are created before indexes that reference them
//   - Unique indexes are created before foreign keys because PostgreSQL can use
//     a unique index as the referenced key
//   - Foreign key dependencies are handled after table creation, so cyclic table references remain executable
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
//	database := schemamodel.Database{
//		Enums: []schemamodel.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []schemamodel.Table{
//			{StructName: "User", Name: "users", Comment: "User accounts"},
//		},
//		Fields: []schemamodel.Field{
//			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
//			{StructName: "User", Name: "status", Type: "user_status", Nullable: false},
//		},
//		EmbeddedFields: []schemamodel.EmbeddedField{
//			{StructName: "User", Mode: "inline", EmbeddedTypeName: "Timestamps"},
//		},
//		Indexes: []schemamodel.Index{
//			{Name: "idx_users_status", StructName: "users", Fields: []string{"status"}},
//		},
//	}
//	statements := CollectDatabase(database, platform.Postgres)
//
// # Platform-Specific Processing
//
// All schema elements (tables, fields, embedded fields) are processed with platform-specific overrides
// applied based on the targetPlatform parameter. This ensures that the generated
// AST nodes contain the appropriate configurations for the target database.
//
// # Every declared object reaches a renderer
//
// Whether an object kind is emitted is NOT a question this function answers.
// Every object the schema declares is converted to a node and handed to the
// target's renderer, which answers from its capability set with a statement, a
// supported equivalent, or a named skip. The dialect predicates left in this
// function decide ORDER (MySQL wants a foreign key's indexes before ADD
// CONSTRAINT) and COLUMN MODELING (SQLite carries foreign keys inline; four
// engines model an enum on the column rather than as a type) -- never presence.
//
// It is written that way because the alternative was measured and it is silent.
// Deleting a node here leaves nothing to report it: `ptah schema render`
// dropped a declared sequence, domain, role or function on five dialects and
// exited 0 with no comment and no warning, fifteen omissions in all, because
// reaching the renderer was gated by a list of dialect names
// (stokaro/ptah#929 item 5). Each earlier fix added a name to one of those
// lists, which closed an instance and left the class. A predicate added here
// that decides whether to append is that defect again.
//
// # Return Value
//
// WalkDatabase stops at the first error returned by visit and returns that
// error unchanged. A nil visitor is refused.
func WalkDatabase(
	database schemamodel.Database,
	targetPlatform string,
	visit func(ast.Node) error,
) error {
	if visit == nil {
		return fmt.Errorf("walk database schema: nil visitor")
	}

	// Normalize once so every conversion path consumes the same names.
	assigned := schemaprep.AssignDefaultForeignKeyNames(&database, targetPlatform)
	database = *schemaprep.QualifyDeclaredUserTypes(assigned, targetPlatform)
	allFields := database.Fields

	if err := appendPreTableStatements(visit, database, targetPlatform); err != nil {
		return err
	}

	// 4. Add table definitions (they may be referenced by indexes)
	// Use the combined field list that includes embedded field expansions
	if err := appendTableStatements(visit, database, allFields, targetPlatform); err != nil {
		return err
	}

	// 5. Roles and functions precede the objects that name them: a grant names
	// a role, and a trigger names a function.
	if err := appendRoleAndFunctionStatements(visit, database); err != nil {
		return err
	}

	// 6. Add unique indexes before foreign keys. PostgreSQL accepts a unique
	// index as the referenced key for a foreign key, so it must exist before
	// the FK constraint is added.
	tableIndexes, viewIndexes := splitMaterializedViewIndexes(database)
	if err := appendUniqueIndexStatements(visit, database.Tables, tableIndexes); err != nil {
		return err
	}
	mysqlFamily := isMySQLFamilyTarget(targetPlatform)
	if mysqlFamily {
		if err := appendNonUniqueIndexStatements(visit, database.Tables, tableIndexes); err != nil {
			return err
		}
	}

	// 7. Add foreign key constraints after all tables and unique indexes exist.
	if !isSQLiteTarget(targetPlatform) {
		if err := appendForeignKeyConstraintStatements(visit, database.Tables, allFields, database.Constraints, targetPlatform); err != nil {
			return err
		}
	}

	// 8. Everything that needs the tables to exist first.
	if err := appendPostTableObjectStatements(visit, database, targetPlatform); err != nil {
		return err
	}

	// 8a. A materialized view's indexes, once the view exists.
	if err := appendMaterializedViewIndexStatements(visit, database, viewIndexes); err != nil {
		return err
	}

	// 8b. Synonyms come after the objects a local target may name. A synonym
	// pointing outside this database has nothing here to wait for, and one
	// pointing at a local table or view has to follow it -- SQL Server does not
	// require the target to exist when the synonym is created, but a script
	// that creates the alias first and the table second reads as though the
	// order did not matter, and the next person reorders it.
	if err := appendSynonymStatements(visit, database.Synonyms); err != nil {
		return err
	}

	// 8b2. A hypertable is a call against a table that must already exist:
	// measured on TimescaleDB 2.29.2, create_hypertable against a missing
	// relation answers `relation "conditions" does not exist`. It also has to
	// come before the data statements below, because the call refuses a table
	// that already holds rows -- `table "loaded" is not empty` -- and would
	// then leave the table ordinary.
	if err := appendHypertableStatements(visit, database.Hypertables); err != nil {
		return err
	}

	// 8b3. A continuous aggregate selects from a hypertable, and TimescaleDB
	// checks that: measured on 2.29.2, WITH (timescaledb.continuous) over an
	// ordinary table answers `invalid continuous aggregate view`. It therefore
	// comes after the create_hypertable calls above rather than with the views.
	if err := appendContinuousAggregateStatements(visit, database.ContinuousAggregates); err != nil {
		return err
	}

	// 8c. Extended properties come after every object one can hang off.
	// sp_addextendedproperty resolves @level1name through the catalog and
	// answers `Cannot find the object ... because it does not exist or you do
	// not have permission` when the table is not there yet, so a property can
	// never precede its owner.
	if err := appendExtendedPropertyStatements(visit, database.ExtendedProperties); err != nil {
		return err
	}

	// 9. Add non-unique indexes last, except on MySQL-family targets where both
	// sides of a foreign key need their declared indexes before ADD CONSTRAINT.
	if !mysqlFamily {
		if err := appendNonUniqueIndexStatements(visit, database.Tables, tableIndexes); err != nil {
			return err
		}
	}

	return nil
}

func appendPreTableStatements(
	visit func(ast.Node) error,
	database schemamodel.Database,
	targetPlatform string,
) error {
	// Schemas come first because tables and extension installation clauses may
	// reference them.
	if err := appendSchemaStatements(visit, schemasForRender(database, targetPlatform)); err != nil {
		return err
	}

	for _, extension := range database.Extensions {
		if err := visit(FromExtension(extension)); err != nil {
			return err
		}
	}

	// A sequence may back a column DEFAULT. Its OWNED BY association is emitted
	// later, after the referenced table and column exist.
	for _, sequence := range database.Sequences {
		sequenceNode := FromSequence(sequence)
		sequenceNode.OwnedBy = ""
		if err := visit(sequenceNode); err != nil {
			return err
		}
	}

	// MySQL, MariaDB, SQLite, and SQL Server model enums on the column itself,
	// so a top-level enum node would render no executable DDL there.
	if schemaprep.EmitsStandaloneEnumDefinitions(targetPlatform) {
		for _, enum := range database.Enums {
			if err := visit(FromEnum(enum)); err != nil {
				return err
			}
		}
	}

	// User-defined types precede tables and are ordered by their references,
	// rather than by kind. Enums are already out and reference nothing.
	return visitDatabaseNodes(visit, orderedUserTypeStatements(database)...)
}

// CollectDatabase collects the nodes [WalkDatabase] visits into an
// ast.StatementList. It remains the compatibility entry point for callers that
// need a complete AST; renderers should consume WalkDatabase directly.
func CollectDatabase(database schemamodel.Database, targetPlatform string) *ast.StatementList {
	statements := &ast.StatementList{
		Statements: make([]ast.Node, 0),
	}
	// The collector never returns an error, and WalkDatabase has no other error
	// source. Keep this wrapper infallible for its existing callers.
	_ = WalkDatabase(database, targetPlatform, func(node ast.Node) error {
		statements.Statements = append(statements.Statements, node)
		return nil
	})
	return statements
}

func visitDatabaseNodes(visit func(ast.Node) error, nodes ...ast.Node) error {
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// orderedUserTypeStatements returns CREATE DOMAIN / CREATE TYPE for every
// domain, range and composite the schema declares, ordered so a type is created
// before whatever names it.
//
// The three kinds share one namespace and reference each other in both
// directions -- `CREATE DOMAIN d AS addr` needs the composite `addr` first,
// `CREATE TYPE addr AS (f d_int)` needs the domain `d_int` first -- so emitting
// kind by kind gets one direction wrong whichever kind goes first, and
// PostgreSQL has no forward declaration for a type. This is the same ordering
// the migration planner applies to the types a diff adds; a schema rendered
// whole has to hold it too, or `ptah generate` writes a script that stops at
// `ERROR: type "addr" does not exist`.
func orderedUserTypeStatements(database schemamodel.Database) []ast.Node {
	total := len(database.Domains) + len(database.Ranges) + len(database.CompositeTypes)
	byName := make(map[string]ast.Node, total)
	userTypes := make([]deporder.UserType, 0, total)

	for _, domain := range database.Domains {
		byName[domain.QualifiedName()] = FromDomain(domain)
		userTypes = append(userTypes, deporder.UserType{Name: domain.QualifiedName(), References: []string{domain.BaseType}})
	}
	for _, rangeType := range database.Ranges {
		byName[rangeType.QualifiedName()] = FromRange(rangeType)
		userTypes = append(userTypes, deporder.UserType{Name: rangeType.QualifiedName(), References: []string{rangeType.Subtype}})
	}
	for _, composite := range database.CompositeTypes {
		references := make([]string, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			references = append(references, field.Type)
		}
		byName[composite.QualifiedName()] = FromCompositeType(composite)
		userTypes = append(userTypes, deporder.UserType{Name: composite.QualifiedName(), References: references})
	}

	nodes := make([]ast.Node, 0, len(userTypes))
	for _, name := range deporder.UserTypesForCreate(userTypes) {
		if node, ok := byName[name]; ok {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func appendTableStatements(
	visit func(ast.Node) error,
	database schemamodel.Database,
	allFields []schemamodel.Field,
	targetPlatform string,
) error {
	sqliteTarget := isSQLiteTarget(targetPlatform)
	mode := tableConstraintsWithoutForeignKeys
	if sqliteTarget {
		mode = tableConstraintsWithForeignKeys
	}
	for _, table := range database.Tables {
		tableNode := fromTableWithoutForeignKeys(table, allFields, database.Enums, targetPlatform)
		if sqliteTarget {
			tableNode = FromTable(table, allFields, database.Enums, targetPlatform)
		}
		addTableConstraints(tableNode, table, database.Constraints, mode, targetPlatform)
		if err := visit(tableNode); err != nil {
			return err
		}
	}
	return nil
}

func appendUniqueIndexStatements(visit func(ast.Node) error, tables []schemamodel.Table, indexes []schemamodel.Index) error {
	return appendMatchingIndexStatements(visit, tables, indexes, func(index schemamodel.Index) bool {
		return index.Unique
	})
}

func appendNonUniqueIndexStatements(visit func(ast.Node) error, tables []schemamodel.Table, indexes []schemamodel.Index) error {
	return appendMatchingIndexStatements(visit, tables, indexes, func(index schemamodel.Index) bool {
		return !index.Unique
	})
}

func appendMatchingIndexStatements(
	visit func(ast.Node) error,
	tables []schemamodel.Table,
	indexes []schemamodel.Index,
	matches func(schemamodel.Index) bool,
) error {
	structToTableMap := createStructToTableMap(tables)
	for _, index := range indexes {
		if !matches(index) {
			continue
		}
		indexNode := FromIndexWithTableMapping(index, structToTableMap)
		if err := visit(indexNode); err != nil {
			return err
		}
	}
	return nil
}

// appendMaterializedViewIndexStatements appends the indexes a materialized view
// carries, after the view itself exists.
//
// Ordering is the engine's rule rather than a preference: `CREATE INDEX ... ON
// mv` before the view answers `relation "mv" does not exist` on PostgreSQL
// 18.4. They cannot go with the table indexes for the same reason -- those are
// emitted before views, so that a unique index can back a foreign key, and no
// foreign key references a materialized view.
func appendMaterializedViewIndexStatements(
	visit func(ast.Node) error,
	database schemamodel.Database,
	viewIndexes []schemamodel.Index,
) error {
	mapping := createStructToViewMap(database.MaterializedViews)
	for _, index := range viewIndexes {
		if err := visit(FromIndexWithTableMapping(index, mapping)); err != nil {
			return err
		}
	}
	return nil
}

// splitMaterializedViewIndexes separates the indexes a materialized view
// carries from every other index.
//
// The split is deliberately narrow: an index resolves to a view only when the
// table resolver found nothing for it AND the relation resolver found a view.
// Everything else -- including an index whose struct name resolves to no
// declared relation at all -- keeps the path it had, because
// FromIndexWithTableMapping's fall back to the struct name is what makes a
// declaration writing the TABLE name in StructName work, and that spelling is
// in the fixtures (stokaro/ptah#1725).
func splitMaterializedViewIndexes(database schemamodel.Database) (tableIndexes, viewIndexes []schemamodel.Index) {
	if len(database.MaterializedViews) == 0 {
		return database.Indexes, nil
	}
	tableOwners := schemamodel.ResolveIndexTableNames(database.Indexes, database.Tables)
	relationOwners := schemamodel.ResolveIndexOwners(database.Indexes, database.Tables, database.MaterializedViews)
	tableIndexes = make([]schemamodel.Index, 0, len(database.Indexes))
	viewIndexes = make([]schemamodel.Index, 0)
	for position, index := range database.Indexes {
		if tableOwners[position] == "" && relationOwners[position] != "" {
			viewIndexes = append(viewIndexes, index)
			continue
		}
		tableIndexes = append(tableIndexes, index)
	}
	return tableIndexes, viewIndexes
}

// createStructToViewMap maps a struct name onto the materialized view it
// declares.
//
// Views alone, with no table entries merged in, because this map is consulted
// only for indexes splitMaterializedViewIndexes already classified as a view's
// -- and an index whose struct also declares a table is never one of those. A
// merged map would need a precedence rule for a collision that cannot occur,
// and unreachable precedence rules are how the wrong one gets picked later.
func createStructToViewMap(views []schemamodel.MaterializedView) map[string]string {
	mapping := make(map[string]string, len(views))
	for _, view := range views {
		mapping[view.StructName] = view.Name
	}
	return mapping
}

// appendRoleAndFunctionStatements appends every declared role and function, for
// every target. A target that cannot host one says so through its renderer.
func appendRoleAndFunctionStatements(visit func(ast.Node) error, database schemamodel.Database) error {
	for _, role := range database.Roles {
		if err := visit(FromRole(role)); err != nil {
			return err
		}
	}
	for _, function := range database.Functions {
		if err := visit(FromFunction(function)); err != nil {
			return err
		}
	}
	return nil
}

// appendPostTableObjectStatements appends every declared object whose statement
// names a table or a column, for every target: a sequence's OWNED BY, views and
// materialized views, row-level security and its policies, grants, and triggers.
//
// Views and materialized views share one dependency ordering because a view may
// select from another; emitting the two kinds one after the other gets that
// wrong whichever kind goes first.
func appendPostTableObjectStatements(
	visit func(ast.Node) error,
	database schemamodel.Database,
	targetPlatform string,
) error {
	// Associate standalone sequences with their owning table.column now that the
	// tables exist. CREATE SEQUENCE ran earlier (before tables) without OWNED BY.
	for _, sequence := range database.Sequences {
		if ownershipNode := sequenceOwnershipNode(sequence); ownershipNode != nil {
			if err := visit(ownershipNode); err != nil {
				return err
			}
		}
	}
	if err := appendOrderedViewLikeStatements(visit, database, targetPlatform); err != nil {
		return err
	}
	for _, rlsEnabled := range database.RLSEnabledTables {
		if err := visit(FromRLSEnabledTable(rlsEnabled)); err != nil {
			return err
		}
	}
	for _, rlsPolicy := range database.RLSPolicies {
		if err := visit(FromRLSPolicy(schemaprep.QualifyRLSPolicyForTarget(
			rlsPolicy, declaredTableSchema(database, rlsPolicy.Table), targetPlatform))); err != nil {
			return err
		}
	}
	for _, grant := range database.Grants {
		if err := visit(FromGrant(grant)); err != nil {
			return err
		}
	}
	for _, trigger := range database.Triggers {
		if err := visit(FromTrigger(trigger)); err != nil {
			return err
		}
	}
	return nil
}

// declaredTableSchema is the schema a declared table is written under, or empty
// when nothing declares it.
//
// It exists here because this path renders a whole database rather than a diff,
// so the tables are in hand and there is nothing to carry the answer. A plan
// built from a diff gets it from the entry instead (stokaro/ptah#2315).
//
// The name is matched as written, which is what the search inside
// QualifyRLSPolicyForTarget did with the same two values before it was narrowed.
func declaredTableSchema(database schemamodel.Database, tableName string) string {
	for _, declared := range database.Tables {
		if declared.Name == tableName {
			return declared.Schema
		}
	}
	return ""
}

func appendOrderedViewLikeStatements(
	visit func(ast.Node) error,
	database schemamodel.Database,
	targetPlatform string,
) error {
	objects := make([]deporder.ViewLike, 0, len(database.Views)+len(database.MaterializedViews))
	viewsByName := make(map[string]schemamodel.View, len(database.Views))
	materializedViewsByName := make(map[string]schemamodel.MaterializedView, len(database.MaterializedViews))
	for _, view := range database.Views {
		objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body})
		viewsByName[view.Name] = view
	}
	for _, view := range database.MaterializedViews {
		objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
		materializedViewsByName[view.Name] = view
	}

	for _, object := range deporder.ViewLikesForCreateForDialect(objects, targetPlatform) {
		if object.Materialized {
			if err := visit(FromMaterializedView(materializedViewsByName[object.Name])); err != nil {
				return err
			}
			continue
		}
		if err := visit(FromView(viewsByName[object.Name])); err != nil {
			return err
		}
	}
	return nil
}

func isSQLiteTarget(targetPlatform string) bool {
	return platform.NormalizeDialect(targetPlatform) == platform.SQLite
}

func isMySQLFamilyTarget(targetPlatform string) bool {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func appendSchemaStatements(visit func(ast.Node) error, schemas []schemamodel.Schema) error {
	for _, schema := range schemas {
		if err := visit(&ast.CreateSchemaNode{
			Name:        schema.Name,
			IfNotExists: true,
			Comment:     schema.Comment,
			Charset:     schema.Charset,
			Collate:     schema.Collate,
		}); err != nil {
			return err
		}
	}
	return nil
}

func schemasForRender(database schemamodel.Database, targetPlatform string) []schemamodel.Schema {
	schemas := slices.Clone(database.Schemas)
	if !supportsExtensionInstallationSchema(targetPlatform) {
		return schemas
	}

	seen := make(map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		seen[schema.Name] = struct{}{}
	}
	for _, extension := range database.Extensions {
		name := extension.Schema
		if name == "" || systemschema.IsPostgresSystemSchema(name) {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		schemas = append(schemas, schemamodel.Schema{Name: name})
	}
	return schemas
}

// createStructToTableMap creates a mapping from struct names to table names.
// This is used to resolve the correct table names for indexes.
func createStructToTableMap(tables []schemamodel.Table) map[string]string {
	structToTableMap := make(map[string]string)
	for _, table := range tables {
		structToTableMap[table.StructName] = table.QualifiedName()
	}
	return structToTableMap
}

// FromIndexWithTableMapping converts a schemamodel.Index to an ast.IndexNode with proper table name resolution.
// This function is similar to FromIndex but uses a struct-to-table mapping to resolve the correct table names.
func FromIndexWithTableMapping(index schemamodel.Index, structToTableMap map[string]string) *ast.IndexNode {
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
	indexNode.IncludeColumns = index.IncludeColumns
	indexNode.NullsDistinct = cloneBoolPtr(index.NullsDistinct)
	indexNode.StorageParams = maps.Clone(index.StorageParams)

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

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func toASTIndexParts(parts []schemamodel.IndexPart) []ast.IndexPart {
	astParts := make([]ast.IndexPart, 0, len(parts))
	for _, part := range parts {
		astParts = append(astParts, ast.IndexPart{
			Name:       part.Name,
			Expr:       part.Expr,
			Operator:   part.Operator,
			Prefix:     part.Prefix,
			Desc:       part.Desc,
			NullsOrder: part.NullsOrder,
		})
	}
	return astParts
}

func indexFields(index schemamodel.Index) []string {
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

func foreignKeyReference(foreign string) *ast.ForeignKeyRef {
	reference := schemaprep.ParseForeignKeyReference(foreign)
	if reference == nil {
		return nil
	}
	return &ast.ForeignKeyRef{
		Table:   reference.Table,
		Column:  reference.Column,
		Columns: slices.Clone(reference.Columns),
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
func validateEnumField(field schemamodel.Field, enums []schemamodel.Enum) {
	// Enum identity is the declaration, not an "enum_" name prefix; see
	// declaredEnum.
	globalEnum := declaredEnum(field.Type, enums)

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

// keptNotNullConstraintName is the NOT NULL constraint name this column carries
// into the AST, which is the declared one unless the column is a primary key.
//
// The renderer refuses a named NOT NULL on a primary-key column, and it is right
// to: the NOT NULL there is synthesized for comparison rather than declared, the
// key is the constraint the column actually has, and its name is the addressable
// one. A reader supplies such a name on every PostgreSQL 18 primary key, because
// that server names every NOT NULL, so carrying it through would turn reading a
// database back into a refusal.
//
// Dropping it here is therefore the deliberate half of the same rule, and it is
// the only place where dropping a name is correct: on any other column the name
// travels, and a target that cannot keep it refuses rather than losing it
// silently (stokaro/ptah#2590, stokaro/ptah#2161).
func keptNotNullConstraintName(field schemamodel.Field) string {
	if field.Primary {
		return ""
	}
	return field.NotNullConstraintName
}

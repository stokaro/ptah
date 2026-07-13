package postgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

const (
	// DialectName is the PostgreSQL dialect identifier
	DialectName = "postgres"
)

// Planner implements PostgreSQL-specific migration planning functionality.
//
// The Planner is responsible for converting schema differences into PostgreSQL-compatible
// AST nodes that can be rendered into executable SQL statements. It handles PostgreSQL-specific
// features like ENUM types, SERIAL columns, and proper dependency ordering.
//
// # Usage Example
//
//	planner := &postgres.Planner{}
//
//	// Schema differences from comparison
//	diff := &differtypes.SchemaDiff{
//		EnumsAdded:  []string{"user_status"},
//		TablesAdded: []string{"users"},
//	}
//
//	// Target schema from Go struct parsing
//	generated := &goschema.Database{
//		Enums: []goschema.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//		},
//	}
//
//	// Generate migration AST nodes
//	nodes := planner.GenerateMigrationAST(diff, generated)
//
// # Thread Safety
//
// The Planner carries only immutable configuration (a capability set and
// emission policy flags) and is safe for concurrent use across multiple
// goroutines. Each call to GenerateMigrationSQL operates independently
// without shared state.
type Planner struct {
	// caps describes what the concrete target accepts (issue #225/#226); the
	// nil zero value defaults to the current PostgreSQL line preset
	// (capability.Postgres16) via the capabilities accessor, so a bare
	// &Planner{} behaves exactly like New(). Version presets live in the
	// capability package — capability.Postgres16 for PostgreSQL 14+,
	// capability.Postgres13 for 12–13.
	caps capability.Capabilities
	// concurrentIndexes requests CREATE INDEX CONCURRENTLY for new indexes.
	// It is a POLICY choice (concurrent builds cannot run inside a
	// transaction, so callers must arrange no-transaction execution — issue
	// #152), and it only takes effect when the target also has the
	// capability.CreateIndexConcurrently capability — a future
	// postgres-compatible preset without it (CockroachDB, issue #171) keeps
	// plain CREATE INDEX no matter the policy.
	concurrentIndexes bool
}

// New returns a planner configured with the current PostgreSQL line preset
// (capability.Postgres16: PostgreSQL 14+).
func New() *Planner {
	return NewWithCapabilities(capability.Postgres16())
}

// NewWithCapabilities returns a planner for a specific capability set — e.g.
// capability.Postgres13() for a PostgreSQL 12/13 target, or a set resolved
// from a live server via capability.ForServerVersion. The set is expected to
// be valid (capability.Capabilities.Validate); presets always are. The set is
// cloned, so later mutations by the caller cannot affect the planner. A nil
// set defaults to the capability.Postgres16 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
}

// capabilities returns the planner's capability set, defaulting the nil zero
// value to the current PostgreSQL line preset so a bare &Planner{} behaves
// exactly like New(). Restriction must be an explicit choice, never a
// zero-value surprise.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps == nil {
		return capability.Postgres16()
	}
	return p.caps
}

// WithConcurrentIndexes returns a copy of the planner that emits
// CREATE [UNIQUE] INDEX CONCURRENTLY for newly added indexes, provided the
// target capability set includes capability.CreateIndexConcurrently. The
// receiver is not modified. Concurrent index builds cannot run inside a
// transaction block; the caller owns arranging no-transaction execution for
// such statements (issue #152 tracks first-class support in the migrator).
func (p *Planner) WithConcurrentIndexes() *Planner {
	cp := *p
	cp.concurrentIndexes = true
	return &cp
}

func (p *Planner) addNewEnums(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, enumName := range diff.EnumsAdded {
		for _, enum := range generated.Enums {
			if enum.Name == enumName {
				values := make([]string, len(enum.Values))
				for i, v := range enum.Values {
					values[i] = "'" + v + "'"
				}

				enumNode := ast.NewEnum(enum.Name, enum.Values...)
				result = append(result, enumNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingEnums(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, enumDiff := range diff.EnumsModified {
		astNode := ast.NewAlterType(enumDiff.EnumName)
		for _, value := range enumDiff.ValuesAdded {
			addEnumAst := ast.NewAddEnumValueOperation(value)
			astNode.AddOperation(addEnumAst)
		}
		result = append(result, astNode)

		// Note: PostgreSQL doesn't support removing enum values without recreating the enum
		if len(enumDiff.ValuesRemoved) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: Cannot remove enum values %v from %s without recreating the enum", enumDiff.ValuesRemoved, enumDiff.EnumName))
			result = append(result, astCommentNode)
		}
	}
	return result
}

func (p *Planner) addNewTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	tablesToAdd := createTableLookupMap(diff.TablesAdded)

	result = p.addSchemaPreconditions(result, generated, tablesToAdd)

	// Phase 1: Create tables without foreign key constraints
	result = p.createTablesWithoutForeignKeys(result, generated, tablesToAdd)

	// Phase 2: Add foreign key constraints via ALTER TABLE statements
	result = p.addForeignKeyConstraints(result, generated, tablesToAdd)

	return result
}

func (p *Planner) addSchemaPreconditions(result []ast.Node, generated *goschema.Database, tablesToAdd map[string]bool) []ast.Node {
	seen := make(map[string]struct{})
	for _, table := range generated.Tables {
		schema := strings.TrimSpace(table.Schema)
		if schema == "" || !tablesToAdd[table.QualifiedName()] {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		result = append(result, ast.NewRawSQL("CREATE SCHEMA IF NOT EXISTS "+schema))
	}
	return result
}

// createTableLookupMap creates a map for quick table lookup
func createTableLookupMap(tableNames []string) map[string]bool {
	tablesToAdd := make(map[string]bool)
	for _, tableName := range tableNames {
		tablesToAdd[tableName] = true
	}
	return tablesToAdd
}

// createTablesWithoutForeignKeys creates all tables without foreign key constraints
func (p *Planner) createTablesWithoutForeignKeys(result []ast.Node, generated *goschema.Database, tablesToAdd map[string]bool) []ast.Node {
	allFields := generated.Fields

	for _, table := range generated.Tables {
		if !tablesToAdd[table.QualifiedName()] {
			continue
		}

		astNode := ast.NewCreateTable(table.QualifiedName())
		for _, field := range allFields {
			if field.StructName == table.StructName {
				columnNode := fromschema.FromFieldWithoutForeignKeys(field, generated.Enums, DialectName)
				astNode.AddColumn(columnNode)
			}
		}
		result = append(result, astNode)
	}

	return result
}

// addForeignKeyConstraints adds foreign key constraints via ALTER TABLE statements
func (p *Planner) addForeignKeyConstraints(result []ast.Node, generated *goschema.Database, tablesToAdd map[string]bool) []ast.Node {
	for _, table := range generated.Tables {
		if !tablesToAdd[table.QualifiedName()] {
			continue
		}

		result = p.addRegularForeignKeys(result, generated, table)
		result = p.addSelfReferencingForeignKeys(result, generated, table)
	}

	return result
}

// addRegularForeignKeys adds regular (non-self-referencing) foreign key constraints
func (p *Planner) addRegularForeignKeys(result []ast.Node, generated *goschema.Database, table goschema.Table) []ast.Node {
	for _, field := range generated.Fields {
		if !isRegularForeignKeyField(field, table) {
			continue
		}

		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef != nil && !foreignKeyTargetsTable(fkRef, table) {
			qualifyForeignKeyRef(generated, table, fkRef)
			fkRef.OnDelete = field.OnDelete
			fkRef.OnUpdate = field.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), foreignKeyName(table.Name, field), []string{field.Name}, fkRef))
		}
	}
	return result
}

func foreignKeyTargetsTable(fkRef *ast.ForeignKeyRef, table goschema.Table) bool {
	return fkRef.Table == table.Name || fkRef.Table == table.QualifiedName()
}

func qualifyForeignKeyRef(generated *goschema.Database, current goschema.Table, fkRef *ast.ForeignKeyRef) {
	if strings.Contains(fkRef.Table, ".") {
		return
	}
	currentSchema := strings.TrimSpace(current.Schema)
	for _, table := range generated.Tables {
		if strings.TrimSpace(table.Schema) == currentSchema && table.Name == fkRef.Table {
			fkRef.Table = table.QualifiedName()
			return
		}
	}
	matchedName := ""
	for _, table := range generated.Tables {
		if table.Name != fkRef.Table {
			continue
		}
		if matchedName != "" {
			return
		}
		matchedName = table.QualifiedName()
	}
	if matchedName != "" {
		fkRef.Table = matchedName
	}
}

// addSelfReferencingForeignKeys adds self-referencing foreign key constraints
func (p *Planner) addSelfReferencingForeignKeys(result []ast.Node, generated *goschema.Database, table goschema.Table) []ast.Node {
	selfRefFKs, exists := generated.SelfReferencingForeignKeys[table.QualifiedName()]
	if !exists {
		return result
	}

	for _, selfRefFK := range selfRefFKs {
		fkRef := fromschema.ParseForeignKeyReference(selfRefFK.Foreign)
		if fkRef != nil {
			qualifyForeignKeyRef(generated, table, fkRef)
			fkRef.OnDelete = selfRefFK.OnDelete
			fkRef.OnUpdate = selfRefFK.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), selfReferencingForeignKeyName(table.Name, selfRefFK), []string{selfRefFK.FieldName}, fkRef))
		}
	}

	return result
}

// selfReferencingForeignKeyName returns the constraint name for a
// self-referencing field-level foreign key, deriving the conventional
// fk_<table>_<field> name when foreign_key_name= was omitted (same rule as the
// regular field path in foreignKeyName).
func selfReferencingForeignKeyName(tableName string, fk goschema.SelfReferencingFK) string {
	if fk.ForeignKeyName != "" {
		return fk.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, fk.FieldName)
}

// isRegularForeignKeyField checks if a field is a regular foreign key field for the given table.
//
// A field-level foreign= annotation is a foreign key regardless of whether the
// author supplied an explicit foreign_key_name=. When the name is omitted the
// planner derives the conventional fk_<table>_<column> name (see
// foreignKeyName) so the constraint is actually created in the database with a
// stable, named identity. Without this an anonymous field-level FK on a newly
// created table was silently dropped from the migration, which made the
// schemadiff comparator (which synthesizes the FK under the conventional name)
// re-report it as missing forever (issue #189 round-trip failure).
func isRegularForeignKeyField(field goschema.Field, table goschema.Table) bool {
	return field.StructName == table.StructName && field.Foreign != ""
}

// foreignKeyName returns the constraint name to use for a field-level foreign
// key: the explicit foreign_key_name= when set, otherwise the conventional
// fk_<table>_<column> name shared with the schemadiff comparator and the down
// path via fromschema.GenerateForeignKeyName.
func foreignKeyName(tableName string, field goschema.Field) string {
	if field.ForeignKeyName != "" {
		return field.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, field.Name)
}

// createForeignKeyAlterStatement creates an ALTER TABLE statement for adding a foreign key constraint
func (p *Planner) createForeignKeyAlterStatement(tableName, constraintName string, columns []string, fkRef *ast.ForeignKeyRef) *ast.AlterTableNode {
	fkRef.Name = constraintName
	fkConstraint := ast.NewForeignKeyConstraint(constraintName, columns, fkRef)

	return &ast.AlterTableNode{
		Name:       tableName,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: fkConstraint}},
	}
}

func (p *Planner) addNewTableColumns(result []ast.Node, tableDiff types.TableDiff, generated *goschema.Database) []ast.Node {
	for _, colName := range tableDiff.ColumnsAdded {
		// Find the field definition for this column
		// We need to find the struct name that corresponds to this table name
		var targetField *goschema.Field
		var targetStructName string

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName); table != nil {
			targetStructName = table.StructName
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		if targetField != nil {
			columnNode := fromschema.FromFieldWithoutForeignKeys(*targetField, generated.Enums, "postgres")

			// Only add the column - foreign key constraints will be added separately
			// to ensure proper dependency ordering (columns must exist before FK constraints)
			operations := []ast.AlterOperation{&ast.AddColumnOperation{Column: columnNode}}

			// Generate ALTER TABLE statement with only the ADD COLUMN operation
			alterNode := &ast.AlterTableNode{
				Name:       tableDiff.TableName,
				Operations: operations,
			}
			result = append(result, alterNode)
		}
	}
	return result
}

// addForeignKeyConstraintsForNewColumns adds foreign key constraints for newly added columns.
// This method is called after all columns have been added to ensure that referenced columns exist.
func (p *Planner) addForeignKeyConstraintsForNewColumns(result []ast.Node, tableDiff types.TableDiff, generated *goschema.Database) []ast.Node {
	for _, colName := range tableDiff.ColumnsAdded {
		// Find the field definition for this column
		var targetField *goschema.Field
		var targetStructName string
		var targetTableName string
		var targetTable *goschema.Table

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName); table != nil {
			targetTable = table
			targetStructName = table.StructName
			targetTableName = table.Name
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		// Only process fields that have foreign key constraints
		if targetField != nil && targetField.Foreign != "" {
			// Parse the foreign key reference
			fkRef := fromschema.ParseForeignKeyReference(targetField.Foreign)
			if fkRef != nil {
				if targetTable != nil {
					qualifyForeignKeyRef(generated, *targetTable, fkRef)
				}
				fkName := foreignKeyName(targetTableName, *targetField)
				fkRef.Name = fkName
				fkRef.OnDelete = targetField.OnDelete
				fkRef.OnUpdate = targetField.OnUpdate

				// Create foreign key constraint
				fkConstraint := ast.NewForeignKeyConstraint(
					fkName,
					[]string{targetField.Name},
					fkRef,
				)

				// Create ALTER TABLE statement with only the ADD CONSTRAINT operation
				alterNode := &ast.AlterTableNode{
					Name:       tableDiff.TableName,
					Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: fkConstraint}},
				}
				result = append(result, alterNode)
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingTableColumns(result []ast.Node, tableDiff types.TableDiff, generated *goschema.Database) []ast.Node {
	for _, colDiff := range tableDiff.ColumnsModified {
		// Find the target field definition for this column
		// We need to find the struct name that corresponds to this table name
		var targetField *goschema.Field
		var targetStructName string

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName); table != nil {
			targetStructName = table.StructName
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colDiff.ColumnName {
				targetField = &field
				break
			}
		}

		if targetField == nil {
			astCommentNode := ast.NewComment(fmt.Sprintf("ERROR: Could not find field definition for %s.%s (struct: %s)", tableDiff.TableName, colDiff.ColumnName, targetStructName))
			result = append(result, astCommentNode)
			continue
		}

		// Create a column definition with the target field properties
		columnNode := fromschema.FromField(*targetField, generated.Enums, "postgres")

		// Generate ALTER COLUMN statements using AST
		alterNode := &ast.AlterTableNode{
			Name: tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
				Column:              columnNode,
				PreviousType:        previousColumnType(colDiff.Changes["type"]),
				PreviousNullable:    previousColumnNullable(colDiff.Changes["nullable"]),
				HasPreviousNullable: colDiff.Changes["nullable"] != "",
			}},
		}
		result = append(result, alterNode)

		// Add a comment showing what changes are being made. Iterate the
		// changes in sorted key order so migration output is deterministic
		// (issue #59).
		changesList := make([]string, 0, len(colDiff.Changes))
		for _, changeType := range slices.Sorted(maps.Keys(colDiff.Changes)) {
			changesList = append(changesList, fmt.Sprintf("%s: %s", changeType, colDiff.Changes[changeType]))
		}
		astCommentNode := ast.NewComment(fmt.Sprintf("Modify column %s.%s: %s", tableDiff.TableName, colDiff.ColumnName, strings.Join(changesList, ", ")))
		result = append(result, astCommentNode)
	}
	return result
}

func findGeneratedTableByDiffName(generated *goschema.Database, tableName string) *goschema.Table {
	for i := range generated.Tables {
		table := &generated.Tables[i]
		if table.Name == tableName || table.QualifiedName() == tableName {
			return table
		}
	}
	return nil
}

func previousColumnType(change string) string {
	before, _, ok := strings.Cut(change, " -> ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(before)
}

func previousColumnNullable(change string) bool {
	before, _, ok := strings.Cut(change, " -> ")
	return ok && strings.TrimSpace(before) == "true"
}

func (p *Planner) removeTableColumnsFromDiff(result []ast.Node, tableDiff types.TableDiff) []ast.Node {
	for _, colName := range tableDiff.ColumnsRemoved {
		// Generate DROP COLUMN statement using AST with CASCADE to handle dependencies
		dropOp := &ast.DropColumnOperation{
			ColumnName: colName,
			Cascade:    true, // Use CASCADE to automatically drop dependent RLS policies
		}
		alterNode := &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{dropOp},
		}
		result = append(result, alterNode)
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: Dropping column %s.%s with CASCADE - This will delete data and dependent objects!", tableDiff.TableName, colName))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) addAndModifyTableColumns(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsModified) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add new columns
			result = p.addNewTableColumns(result, tableDiff, generated)

			// Modify existing columns
			result = p.modifyExistingTableColumns(result, tableDiff, generated)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewComment(fmt.Sprintf("Add/modify columns for table: %s", tableDiff.TableName))
				// Insert the comment before the operations we just added
				newResult := make([]ast.Node, 0, len(result)+1)
				newResult = append(newResult, result[:initialLength]...)
				newResult = append(newResult, astCommentNode)
				newResult = append(newResult, result[initialLength:]...)
				result = newResult
			}
		}
	}
	return result
}

// addForeignKeyConstraintsForModifiedTables adds foreign key constraints for all newly added columns
// across all modified tables. This ensures that all columns exist before any foreign key constraints
// are created, preventing dependency ordering issues.
func (p *Planner) addForeignKeyConstraintsForModifiedTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add foreign key constraints for new columns
			result = p.addForeignKeyConstraintsForNewColumns(result, tableDiff, generated)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewComment(fmt.Sprintf("Add foreign key constraints for table: %s", tableDiff.TableName))
				// Insert the comment before the operations we just added
				newResult := make([]ast.Node, 0, len(result)+1)
				newResult = append(newResult, result[:initialLength]...)
				newResult = append(newResult, astCommentNode)
				newResult = append(newResult, result[initialLength:]...)
				result = newResult
			}
		}
	}
	return result
}

func (p *Planner) removeTableColumns(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsRemoved) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("Remove columns from table: %s", tableDiff.TableName))
			result = append(result, astCommentNode)

			// Remove columns (dangerous!)
			result = p.removeTableColumnsFromDiff(result, tableDiff)
		}
	}
	return result
}

func (p *Planner) addNewIndexes(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Create a mapping from struct names to table names for proper index table resolution
	structToTableMap := make(map[string]string)
	for _, table := range generated.Tables {
		structToTableMap[table.StructName] = table.QualifiedName()
	}

	for _, indexName := range diff.IndexesAdded {
		// Find the index definition
		for _, idx := range generated.Indexes {
			if idx.Name == indexName {
				// Use enhanced index creation with PostgreSQL features
				indexNode := fromschema.FromIndexWithTableMapping(idx, structToTableMap)
				// CONCURRENTLY is opt-in policy AND capability-gated: the
				// planner never emits it for a target that rejects it
				// (issue #226; CockroachDB-style presets keep plain
				// CREATE INDEX even when the policy is on).
				if p.concurrentIndexes && p.capabilities().Has(capability.CreateIndexConcurrently) {
					indexNode.Concurrently = true
				}
				result = append(result, indexNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) removeIndexes(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// IF EXISTS on DROP INDEX is capability-gated intent, mirroring the MySQL
	// planner (issue #226). Every supported PostgreSQL line has the guard, so
	// the default preset keeps today's output; a preset without it (or a
	// composed set) actually changes the plan.
	guarded := p.capabilities().Has(capability.DropIndexIfExists)
	for _, indexName := range diff.IndexesRemoved {
		dropIndexNode := ast.NewDropIndex(indexName)
		if guarded {
			dropIndexNode.SetIfExists()
		}
		result = append(result, dropIndexNode)
	}
	return result
}

func (p *Planner) removeTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, tableName := range diff.TablesRemoved {
		dropTableNode := ast.NewDropTable(tableName).
			SetIfExists().
			SetCascade().
			SetComment("WARNING: This will delete all data!")

		result = append(result, dropTableNode)
	}
	return result
}

func (p *Planner) removeEnums(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, enumName := range diff.EnumsRemoved {
		dropTypeNode := ast.NewDropType(enumName).
			SetIfExists().
			SetCascade().
			SetComment("WARNING: Make sure no tables use this enum!")

		result = append(result, dropTypeNode)
	}
	return result
}

// GenerateMigrationAST generates PostgreSQL-specific migration AST statements from schema differences.
//
// This method transforms the schema differences captured in the SchemaDiff into executable
// PostgreSQL AST statements that can be applied to bring the database schema in line with the target
// schema. The generated AST follows PostgreSQL-specific syntax and best practices.
//
// # Migration Order
//
// The SQL statements are generated in a specific order to avoid dependency conflicts:
//  1. Create new enum types (required before tables that use them)
//  2. Modify existing enum types (add new values)
//  3. Create new tables
//  4. Modify existing tables (add/modify/remove columns)
//  5. Add new indexes
//  6. Remove indexes (safe operations)
//  7. Remove tables (dangerous - commented out by default)
//  8. Remove enum types (dangerous - commented out by default)
//
// # PostgreSQL-Specific Features
//
//   - Native ENUM types with CREATE TYPE and ALTER TYPE statements
//   - SERIAL columns for auto-increment functionality
//   - Proper handling of enum value limitations (cannot remove values easily)
//   - PostgreSQL-specific syntax for ALTER statements
//
// # Parameters
//
//   - diff: The schema differences to be applied
//   - generated: The target schema parsed from Go struct annotations
//
// # Examples
//
// Basic enum and table creation:
//
//	diff := &differtypes.SchemaDiff{
//		EnumsAdded:  []string{"user_status"},
//		TablesAdded: []string{"users"},
//	}
//
//	generated := &goschema.Database{
//		Enums: []goschema.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//			{Name: "status", Type: "user_status", StructName: "User"},
//		},
//	}
//
//	nodes := planner.GenerateMigrationAST(diff, generated)
//	// Results in:
//	// 1. CREATE TYPE user_status AS ENUM ('active', 'inactive');
//	// 2. CREATE TABLE users (id SERIAL PRIMARY KEY, status user_status);
//
// Table modification with column changes:
//
//	diff := &differtypes.SchemaDiff{
//		TablesModified: []differtypes.TableDiff{
//			{
//				TableName:    "users",
//				ColumnsAdded: []string{"email"},
//				ColumnsModified: []differtypes.ColumnDiff{
//					{ColumnName: "name", Changes: map[string]string{"type": "VARCHAR(255)"}},
//				},
//			},
//		},
//	}
//	// Results in ALTER TABLE statements for adding and modifying columns
//
// # Return Value
//
// Returns a slice of AST nodes representing SQL statements. Each node can be rendered
// to SQL using a PostgreSQL-specific visitor. Comments and warnings are included
// as CommentNode instances for documentation and safety.
func (p *Planner) GenerateMigrationAST(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node

	// 0. Add new extensions first (PostgreSQL extensions should be created before other objects)
	result = p.addNewExtensions(result, diff, generated)

	// 1. Add new roles (roles may be referenced by RLS policies and functions)
	if p.capabilities().Has(capability.RoleManagement) {
		result = p.addNewRoles(result, diff, generated)
	}

	// 2. Add new functions (functions may be used by RLS policies)
	result = p.addNewFunctions(result, diff, generated)

	// 2b. Modify existing function definitions (body, volatility, security, language).
	// PostgreSQL CREATE OR REPLACE FUNCTION updates the live definition in place
	// without affecting policies or triggers that reference the function.
	result = p.modifyExistingFunctions(result, diff, generated)

	// 3. Add new enums (PostgreSQL requires enum types to exist before tables use them)
	result = p.addNewEnums(result, diff, generated)

	// 4. Modify existing enums (add values only - PostgreSQL doesn't support removing enum values easily)
	result = p.modifyExistingEnums(result, diff)

	// 5. Add new tables
	result = p.addNewTables(result, diff, generated)

	// 6. Add and modify table columns (must be done before creating RLS policies that depend on columns)
	result = p.addAndModifyTableColumns(result, diff, generated)

	// 6.5. Add foreign key constraints for newly added columns (must be done after all columns exist)
	result = p.addForeignKeyConstraintsForModifiedTables(result, diff, generated)

	// 6.6. Add and modify views, materialized views, and triggers after their tables/functions exist.
	result = p.addNewViews(result, diff, generated)
	result = p.modifyExistingViews(result, diff, generated)
	result = p.addNewMaterializedViews(result, diff, generated)
	result = p.modifyExistingMaterializedViews(result, diff, generated)
	result = p.addNewTriggers(result, diff, generated)
	result = p.modifyExistingTriggers(result, diff, generated)

	// 7. Modify existing roles (must be done before RLS policies that reference them)
	if p.capabilities().Has(capability.RoleManagement) {
		result = p.modifyExistingRoles(result, diff, generated)
	}

	// 7.5. Revoke removed grants before adding replacement grants.
	if p.capabilities().Has(capability.RoleManagement) {
		result = p.removeGrants(result, diff)
		result = p.revokeGrantOptions(result, diff)
	}

	// 8. Enable RLS on tables (must be done after table creation and modification)
	if p.capabilities().Has(capability.RowLevelSecurity) {
		result = p.enableRLSOnTables(result, diff, generated)
	}

	// 9. Add RLS policies (must be done after RLS is enabled and columns exist)
	if p.capabilities().Has(capability.RowLevelSecurity) {
		result = p.addNewRLSPolicies(result, diff, generated)
	}

	// 9.5. Add role privilege grants after roles and target objects exist.
	if p.capabilities().Has(capability.RoleManagement) {
		result = p.addNewGrants(result, diff)
	}

	// 10. Add new indexes
	result = p.addNewIndexes(result, diff, generated)

	// 10.5. Add new constraints (must be done after tables and columns exist)
	result = p.addNewConstraints(result, diff, generated)

	// 11. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 12. Remove RLS policies (must be done before disabling RLS and before dropping columns)
	if p.capabilities().Has(capability.RowLevelSecurity) {
		result = p.removeRLSPolicies(result, diff)
	}

	// 11. Disable RLS on tables (must be done after removing policies)
	if p.capabilities().Has(capability.RowLevelSecurity) {
		result = p.disableRLSOnTables(result, diff)
	}

	// 12. Remove table columns (must be done after removing RLS policies that depend on columns)
	result = p.removeTableColumns(result, diff)

	// 12.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff)

	// 12.6. Remove triggers and view-like objects before dropping tables/functions they depend on.
	result = p.removeTriggers(result, diff)
	result = p.removeMaterializedViews(result, diff)
	result = p.removeViews(result, diff)

	// 13. Remove tables (dangerous!)
	result = p.removeTables(result, diff)

	// 13. Remove functions (must be done after removing policies that might use them)
	result = p.removeFunctions(result, diff)

	// 14. Remove roles (must be done after removing functions and policies that depend on them)
	if p.capabilities().Has(capability.RoleManagement) {
		result = p.removeRoles(result, diff)
	}

	// 15. Remove enums (dangerous!)
	result = p.removeEnums(result, diff)

	// 16. Remove extensions (dangerous!)
	result = p.removeExtensions(result, diff)

	return result
}

func (p *Planner) addNewRoles(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, roleName := range diff.RolesAdded {
		// Find the role definition
		for _, role := range generated.Roles {
			if role.Name == roleName {
				roleNode := fromschema.FromRole(role)
				result = append(result, roleNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingRoles(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, roleDiff := range diff.RolesModified {
		targetRole := p.findTargetRole(roleDiff.RoleName, generated)
		if targetRole == nil {
			continue // Skip if role not found in target schema
		}

		alterRoleNode := p.buildAlterRoleNode(roleDiff, targetRole)
		if len(alterRoleNode.Operations) > 0 {
			alterRoleNode.SetComment(fmt.Sprintf("Modify role %s attributes", roleDiff.RoleName))
			result = append(result, alterRoleNode)
		}
	}
	return result
}

// findTargetRole finds a role by name in the generated database schema
func (p *Planner) findTargetRole(roleName string, generated *goschema.Database) *goschema.Role {
	for _, role := range generated.Roles {
		if role.Name == roleName {
			return &role
		}
	}
	return nil
}

// buildAlterRoleNode creates an ALTER ROLE node with operations based on role changes
func (p *Planner) buildAlterRoleNode(roleDiff types.RoleDiff, targetRole *goschema.Role) *ast.AlterRoleNode {
	alterRoleNode := ast.NewAlterRole(roleDiff.RoleName)

	// Sorted key order keeps the ALTER ROLE operation order deterministic
	// across runs (issue #59).
	for _, changeType := range slices.Sorted(maps.Keys(roleDiff.Changes)) {
		p.addRoleOperation(alterRoleNode, changeType, roleDiff.Changes[changeType], targetRole)
	}

	return alterRoleNode
}

// addRoleOperation adds the appropriate operation to the ALTER ROLE node based on change type and value
func (p *Planner) addRoleOperation(alterRoleNode *ast.AlterRoleNode, changeType, changeValue string, targetRole *goschema.Role) {
	switch changeType {
	case "login":
		p.addLoginOperation(alterRoleNode, changeValue)
	case "password":
		p.addPasswordOperation(alterRoleNode, changeValue, targetRole)
	case "superuser":
		p.addSuperuserOperation(alterRoleNode, changeValue)
	case "createdb", "create_db":
		p.addCreateDBOperation(alterRoleNode, changeValue)
	case "createrole", "create_role":
		p.addCreateRoleOperation(alterRoleNode, changeValue)
	case "inherit":
		p.addInheritOperation(alterRoleNode, changeValue)
	case "replication":
		p.addReplicationOperation(alterRoleNode, changeValue)
	}
}

// addLoginOperation adds a login operation to the ALTER ROLE node
func (p *Planner) addLoginOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetLoginOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetLoginOperation(false))
	}
}

// addSuperuserOperation adds a superuser operation to the ALTER ROLE node
func (p *Planner) addSuperuserOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(false))
	}
}

// addCreateDBOperation adds a createdb operation to the ALTER ROLE node
func (p *Planner) addCreateDBOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(false))
	}
}

// addCreateRoleOperation adds a createrole operation to the ALTER ROLE node
func (p *Planner) addCreateRoleOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(false))
	}
}

// addInheritOperation adds an inherit operation to the ALTER ROLE node
func (p *Planner) addInheritOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetInheritOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetInheritOperation(false))
	}
}

// addReplicationOperation adds a replication operation to the ALTER ROLE node
func (p *Planner) addReplicationOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetReplicationOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetReplicationOperation(false))
	}
}

// addPasswordOperation adds a password operation to the ALTER ROLE node
func (p *Planner) addPasswordOperation(alterRoleNode *ast.AlterRoleNode, changeValue string, targetRole *goschema.Role) {
	if changeValue == "password_update_required" {
		// Use the target role to get the new password
		if targetRole != nil && targetRole.Password != "" {
			alterRoleNode.AddOperation(ast.NewSetPasswordOperation(targetRole.Password))
		}
	}
}

func (p *Planner) removeRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, roleName := range diff.RolesRemoved {
		dropRoleNode := ast.NewDropRole(roleName).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this role")
		result = append(result, dropRoleNode)
	}
	return result
}

func (p *Planner) addNewGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsAdded {
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(grant.WithOption)
		result = append(result, node)
	}
	for _, grant := range diff.GrantOptionsAdded {
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(true)
		result = append(result, node)
	}
	return result
}

func (p *Planner) removeGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsRemoved {
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege})
		result = append(result, node)
	}
	return result
}

func (p *Planner) revokeGrantOptions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantOptionsRevoked {
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetGrantOptionFor(true)
		result = append(result, node)
	}
	return result
}

func (p *Planner) addNewExtensions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, extensionName := range diff.ExtensionsAdded {
		// Find the extension definition
		for _, ext := range generated.Extensions {
			if ext.Name == extensionName {
				extensionNode := fromschema.FromExtension(ext)
				result = append(result, extensionNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) removeExtensions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// Generate DROP EXTENSION statements with comprehensive safety warnings
	// Extension removal is potentially dangerous and requires careful consideration
	for i, extensionName := range diff.ExtensionsRemoved {
		// Add comprehensive warning comments before each DROP EXTENSION statement
		warningComment1 := ast.NewComment(fmt.Sprintf("WARNING: Removing extension '%s' may break existing functionality that depends on it", extensionName))
		warningComment2 := ast.NewComment("Consider reviewing all database objects that use this extension before proceeding")
		warningComment3 := ast.NewComment("Extension removal may cascade to dependent objects - review carefully")

		result = append(result, warningComment1)
		result = append(result, warningComment2)
		result = append(result, warningComment3)

		// Create DROP EXTENSION statement with IF EXISTS for safety
		dropExtension := ast.NewDropExtension(extensionName).
			SetIfExists().
			SetComment(fmt.Sprintf("Remove extension '%s' as it's no longer required by the schema", extensionName))

		result = append(result, dropExtension)

		// Add blank line for readability between extension removals (not after the last one)
		if i < len(diff.ExtensionsRemoved)-1 {
			blankLine := ast.NewComment("")
			result = append(result, blankLine)
		}
	}
	return result
}

func (p *Planner) addNewFunctions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, functionName := range diff.FunctionsAdded {
		// Find the function definition
		for _, fn := range generated.Functions {
			if fn.Name == functionName {
				functionNode := fromschema.FromFunction(fn)
				result = append(result, functionNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingFunctions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, fnDiff := range diff.FunctionsModified {
		// Find the target function definition. Without it we can't emit a
		// faithful CREATE OR REPLACE, so skip silently (the diff alone would
		// not tell us the new body/attributes).
		var target *goschema.Function
		for i := range generated.Functions {
			if generated.Functions[i].Name == fnDiff.FunctionName {
				target = &generated.Functions[i]
				break
			}
		}
		if target == nil {
			continue
		}

		functionNode := fromschema.FromFunction(*target)
		functionNode.SetComment(fmt.Sprintf("Modify function %s: %s", target.Name, summarizeFunctionChanges(fnDiff)))
		result = append(result, functionNode)
	}
	return result
}

// summarizeFunctionChanges produces a deterministic one-line summary of the
// changed attributes for use as a SQL comment.
func summarizeFunctionChanges(fnDiff types.FunctionDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(fnDiff.Changes)), ", ")
}

func (p *Planner) removeFunctions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, functionName := range diff.FunctionsRemoved {
		dropFunctionNode := ast.NewDropFunction(functionName).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this function")
		result = append(result, dropFunctionNode)
	}
	return result
}

func (p *Planner) addNewViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, viewName := range diff.ViewsAdded {
		if view := findView(generated.Views, viewName); view != nil {
			result = append(result, fromschema.FromView(*view))
		}
	}
	return result
}

func (p *Planner) modifyExistingViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, viewDiff := range diff.ViewsModified {
		if view := findView(generated.Views, viewDiff.ViewName); view != nil {
			result = append(result, fromschema.FromView(*view).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, viewName := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(viewName).SetIfExists().SetCascade())
	}
	return result
}

func (p *Planner) addNewMaterializedViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, viewName := range diff.MaterializedViewsAdded {
		if view := findMaterializedView(generated.MaterializedViews, viewName); view != nil {
			result = append(result, fromschema.FromMaterializedView(*view))
		}
	}
	return result
}

func (p *Planner) modifyExistingMaterializedViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, viewDiff := range diff.MaterializedViewsModified {
		if view := findMaterializedView(generated.MaterializedViews, viewDiff.ViewName); view != nil {
			result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists().SetCascade())
			result = append(result, fromschema.FromMaterializedView(*view))
		}
	}
	return result
}

func (p *Planner) removeMaterializedViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, viewName := range diff.MaterializedViewsRemoved {
		result = append(result, ast.NewDropMaterializedView(viewName).SetIfExists().SetCascade())
	}
	return result
}

func (p *Planner) addNewTriggers(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, triggerRef := range diff.TriggersAdded {
		if trigger := findTrigger(generated.Triggers, triggerRef.TableName, triggerRef.TriggerName); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger))
		}
	}
	return result
}

func (p *Planner) modifyExistingTriggers(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, triggerDiff := range diff.TriggersModified {
		if trigger := findTrigger(generated.Triggers, triggerDiff.TableName, triggerDiff.TriggerName); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeTriggers(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, triggerRef := range diff.TriggersRemoved {
		functionName := goschema.Trigger{Name: triggerRef.TriggerName, Table: triggerRef.TableName}.FunctionName()
		result = append(result, ast.NewDropTrigger(triggerRef.TriggerName, triggerRef.TableName).
			SetIfExists().
			SetCascade().
			SetFunctionName(functionName))
	}
	return result
}

func findView(views []goschema.View, name string) *goschema.View {
	for i := range views {
		if views[i].Name == name {
			return &views[i]
		}
	}
	return nil
}

func findMaterializedView(views []goschema.MaterializedView, name string) *goschema.MaterializedView {
	for i := range views {
		if views[i].Name == name {
			return &views[i]
		}
	}
	return nil
}

func findTrigger(triggers []goschema.Trigger, tableName, triggerName string) *goschema.Trigger {
	for i := range triggers {
		if triggers[i].Table == tableName && triggers[i].Name == triggerName {
			return &triggers[i]
		}
	}
	return nil
}

func (p *Planner) enableRLSOnTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Create a set of tables that need RLS enabled
	tablesNeedingRLS := make(map[string]bool)
	for _, policy := range generated.RLSPolicies {
		tablesNeedingRLS[policy.Table] = true
	}

	// Enable RLS on tables that have policies but don't have RLS enabled yet.
	// Iterate in sorted order so migration output is deterministic (issue #59).
	for _, tableName := range slices.Sorted(maps.Keys(tablesNeedingRLS)) {
		// Check if this table is being added or if RLS is being enabled
		tableIsNew := slices.Contains(diff.TablesAdded, tableName)

		// For new tables with RLS policies, enable RLS
		if tableIsNew {
			enableRLSNode := ast.NewAlterTableEnableRLS(tableName).
				SetComment(fmt.Sprintf("Enable RLS for %s table", tableName))
			result = append(result, enableRLSNode)
		}
	}
	return result
}

func (p *Planner) disableRLSOnTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// Track which tables had policies removed to potentially disable RLS
	tablesWithRemovedPolicies := make(map[string]bool)
	for _, policyRef := range diff.RLSPoliciesRemoved {
		tablesWithRemovedPolicies[policyRef.TableName] = true
	}

	// For each table that had policies removed, add a comment about potentially disabling RLS
	// Note: We don't automatically disable RLS because there might be other policies on the table
	for _, tableName := range slices.Sorted(maps.Keys(tablesWithRemovedPolicies)) {
		warningComment := ast.NewComment(fmt.Sprintf("NOTE: RLS policies were removed from table %s - verify if RLS should be disabled", tableName))
		result = append(result, warningComment)
	}
	return result
}

func (p *Planner) addNewRLSPolicies(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, policyName := range diff.RLSPoliciesAdded {
		// Find the policy definition
		for _, policy := range generated.RLSPolicies {
			if policy.Name == policyName {
				policyNode := fromschema.FromRLSPolicy(policy)
				// Set Replace flag to handle conflicts gracefully during migrations
				policyNode.Replace = true
				result = append(result, policyNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) removeRLSPolicies(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, policyRef := range diff.RLSPoliciesRemoved {
		// Now we have both policy name and table name, so we can generate proper DROP POLICY statements
		dropPolicyNode := ast.NewDropPolicy(policyRef.PolicyName, policyRef.TableName).
			SetIfExists().
			SetComment(fmt.Sprintf("Drop RLS policy %s from table %s", policyRef.PolicyName, policyRef.TableName))
		result = append(result, dropPolicyNode)
	}
	return result
}

// addNewConstraints adds new table-level constraints via ALTER TABLE statements.
//
// This method processes constraints defined through Go struct annotations and creates
// appropriate ALTER TABLE ADD CONSTRAINT statements. It handles different constraint
// types including EXCLUDE, CHECK, UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints.
//
// # Constraint Processing Order
//
// Constraints are processed in the order they appear in the generated schema.
// This method assumes that all referenced tables and columns already exist.
//
// # Supported Constraint Types
//
//   - EXCLUDE: PostgreSQL EXCLUDE constraints for preventing conflicts
//   - CHECK: Table-level CHECK constraints for data validation
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
//
// # Example Generated SQL
//
//	ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings
//	  EXCLUDE USING gist (room_id WITH =, during WITH &&);
//
//	ALTER TABLE products ADD CONSTRAINT positive_price
//	  CHECK (price > 0);
func (p *Planner) addNewConstraints(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Resolve struct → table name once for the field-level synthesis fallbacks.
	structToTable := make(map[string]string, len(generated.Tables))
	for _, t := range generated.Tables {
		structToTable[t.StructName] = t.QualifiedName()
	}

	// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved
	// is a modification (the comparator expresses a changed constraint as
	// remove + add of the same name — e.g. an on_delete change on a field-level
	// FK, issue #189). For those we must DROP the old definition before adding
	// the new one, otherwise the ADD CONSTRAINT collides with the still-present
	// constraint of the same name. removeConstraints runs later in the pipeline
	// and deliberately skips these names, so the drop+add is owned here and
	// ordered correctly.
	removedNames := make(map[string]struct{}, len(diff.ConstraintsRemoved))
	for _, name := range diff.ConstraintsRemoved {
		removedNames[name] = struct{}{}
	}

	// Prefer the table-qualified additions when the comparator supplied them.
	// A field-level FK contributed by an embedded inline-relation mixin shares
	// one constraint name across every host table, so the bare ConstraintsAdded
	// name list (and a field scan keyed on the Go struct name) cannot target the
	// right table — it would emit ALTER TABLE <MixinStruct> once per host
	// (issue #197). ConstraintsAddedWithTables carries the concrete table and
	// the full FK definition, so each host gets its own correct ALTER. Names
	// handled here are recorded so the legacy name loop below skips them.
	//
	// A modified FK (dropped + re-added) must be dropped before its re-add. The
	// authoritative "is this host a modification" signal is whether the exact
	// (table, name) pair appears in the removal set — NOT whether the name alone
	// was removed somewhere. In the MIXED case a shared FK name can be a modify
	// on host A and a pure ADD on host B (B has no removal entry); keying the
	// modify decision on the name alone would emit a phantom
	// `ALTER TABLE B DROP CONSTRAINT IF EXISTS <name>` for the pure-add host.
	// Keying the drop decision on (table, name) — mirroring MySQL's
	// removalByTableName — gives the pure-add host no drop.
	removalByTableName := make(map[string]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalByTableName[info.TableName+"."+info.Name] = info
	}

	// Index removals by bare name as well, so the legacy ConstraintsAdded loop
	// below can scope a modified non-FK constraint's DROP to its concrete host
	// table(s). The comparator records every removal in
	// ConstraintsRemovedWithTables in lockstep with the bare ConstraintsRemoved
	// list, so a modified constraint's host is normally known here even though
	// the bare loop iterates names alone.
	removalsByName := make(map[string][]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalsByName[info.Name] = append(removalsByName[info.Name], info)
	}

	// Index the hosts that are actually being re-ADDED under each name. A modified
	// constraint's pre-drop must hit only the hosts whose constraint is being
	// re-added — NOT every host that merely has a removal entry for the name. In
	// the MIXED case (issue #206) a shared name is a modify on host A (re-added)
	// and a PURE removal on host B (not re-added): B's drop is owned by
	// removeConstraints, so the add-side modify-drop must leave B alone or it
	// would be dropped twice.
	addedHostsByName := make(map[string]map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// An addition entry with no recorded host is hostless: a "" host
			// would match no removal entry, so keeping it here would make
			// emitModifyDropForName filter out every REAL removal host and
			// skip a required pre-drop — the re-ADD then collides with the
			// still-present constraint (42710; IF EXISTS on the drop cannot
			// help because the drop was never emitted). Treat the name as if
			// it had no recorded addition hosts at all (issue #229, mirroring
			// the MySQL planner).
			continue
		}
		hosts := addedHostsByName[add.Name]
		if hosts == nil {
			hosts = make(map[string]struct{})
			addedHostsByName[add.Name] = hosts
		}
		hosts[add.TableName] = struct{}{}
	}

	handled := make(map[string]struct{})
	droppedForModify := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// Only emit the DROP-before-ADD when this exact host's FK is being
		// modified (its (table, name) is in the removal set). A pure-add host
		// gets no phantom drop.
		if _, modified := removalByTableName[add.TableName+"."+add.Name]; modified {
			result = p.emitModifyDrop(result, add, droppedForModify)
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		handled[add.Name] = struct{}{}
	}

	for _, constraintName := range diff.ConstraintsAdded {
		// Already emitted via the table-qualified FK path above.
		if _, done := handled[constraintName]; done {
			continue
		}

		// For a modification, emit the DROP first so it precedes the re-add,
		// scoped to the constraint's concrete host table when the comparator
		// recorded it (issue #199) — never a name-only resolution that could drop
		// a same-named constraint on the wrong table.
		if _, modified := removedNames[constraintName]; modified {
			result = p.emitModifyDropForName(result, constraintName, removalsByName, addedHostsByName[constraintName], droppedForModify)
		}

		// Resolve the ADD CONSTRAINT node, in precedence order:
		//  1. explicit table-level //migrator:schema:constraint
		//  2. synthesized field-level check= (issue #112 / PR #123)
		//  3. synthesized field-level foreign= action drift (issue #189)
		// The two field-level fallbacks exist because the comparator
		// synthesizes those constraints into diff.ConstraintsAdded by name only
		// — they never reach generated.Constraints, so without the fallbacks an
		// ADD CONSTRAINT for an existing column would be silently dropped.
		if node, ok := p.addConstraintNodeFor(constraintName, generated, structToTable); ok {
			if node != nil {
				result = append(result, node)
			}
			continue
		}
	}
	return result
}

// emitModifyDrop appends the DROP that must precede the re-ADD of a modified
// field-level FK (a constraint whose (table, name) is in both the additions and
// the removals). It always emits a table-qualified
// ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>, deduped per (host, name),
// because the concrete host is carried on ConstraintAdditionInfo.TableName and
// is therefore always known at emit time.
//
// This is unconditional regardless of how many hosts share the FK name:
//   - When the name lands on >=2 host tables (an inline-relation mixin embedded
//     into many tables, issue #197), each modify host's old constraint must be
//     dropped before its own ADD; a name-only resolution would only reach one.
//   - When the name lands on a single host (the #189 on_delete/on_update action
//     drift case), the table is equally known, so scoping the drop directly is
//     both simpler and safe. The earlier single-host branch fell back to the
//     name-only information_schema DO block (p.dropConstraintNode), which resolves
//     the owning table with LIMIT 1 and no table_name filter. PostgreSQL
//     constraint names are unique per table, not per schema, so that lookup could
//     drop a same-named constraint on the WRONG table (issue #199). Emitting the
//     direct table-qualified drop eliminates the ambiguity.
//
// The name-only DO block (dropConstraintNode) is no longer used for a modify
// whose host the comparator recorded: the legacy ConstraintsAdded modify path
// scopes its DROP via emitModifyDropForName too, and removeConstraints scopes
// pure removals table-qualified as well. It remains in use only as a defensive
// fallback for a synthetic diff that carries no ConstraintsRemovedWithTables
// entry.
func (p *Planner) emitModifyDrop(
	result []ast.Node,
	add types.ConstraintAdditionInfo,
	droppedForModify map[string]struct{},
) []ast.Node {
	return p.appendScopedDrop(result, add.TableName, add.Name, droppedForModify)
}

// emitModifyDropForName appends the DROP(s) that must precede the re-ADD of a
// modified constraint reached via the bare ConstraintsAdded name list (the
// non-FK and field-level synthesis paths; FK modifies are handled per-host in
// the ConstraintsAddedWithTables loop). The comparator records every removal in
// ConstraintsRemovedWithTables in lockstep with the bare list, so the owning
// table is normally known: the modified host gets a direct, table-qualified
// ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>, deduped per (host, name).
// This scopes the drop to the exact host instead of the name-only
// information_schema LIMIT 1 lookup, which — because constraint names are unique
// per table, not per schema — could drop a same-named constraint on the wrong
// table (issue #199).
//
// The drop is restricted to addedHosts: the hosts whose constraint is actually
// being re-added under this name (ConstraintsAddedWithTables). In the MIXED case
// (issue #206) a shared name is a modify on host A (re-added) and a PURE removal
// on host B (not re-added); B's drop is owned by removeConstraints, so dropping
// it here too would emit the drop twice. Restricting to addedHosts leaves B to
// removeConstraints. A removal host absent from addedHosts is therefore skipped.
//
// When addedHosts is empty the re-added hosts are unknown — e.g. a down/reverse
// diff fills ConstraintsRemovedWithTables but not ConstraintsAddedWithTables
// (reverseConstraintAdditions restores only FOREIGN KEYs, and nothing at all
// when the schema context is absent). In that case the drop is still scoped to
// every recorded removal host (the pre-#206 behavior), NOT the name-only DO
// block — otherwise the reverse direction would regress a known-host drop back
// to the information_schema LIMIT 1 lookup. Only a name with no recorded removal
// host at all falls back to the DO block.
func (p *Planner) emitModifyDropForName(
	result []ast.Node,
	name string,
	removalsByName map[string][]types.ConstraintRemovalInfo,
	addedHosts map[string]struct{},
	droppedForModify map[string]struct{},
) []ast.Node {
	if len(addedHosts) > 0 {
		// Re-added hosts are known: drop ONLY those. A removal host that is not
		// being re-added is a pure removal owned by removeConstraints, so dropping
		// it here too would emit the drop twice (issue #206).
		for _, info := range removalsByName[name] {
			if info.TableName == "" {
				continue
			}
			if _, reAdded := addedHosts[info.TableName]; !reAdded {
				continue
			}
			result = p.appendScopedDrop(result, info.TableName, info.Name, droppedForModify)
		}
		return result
	}
	// addedHosts unknown: scope by every recorded removal host before resorting to
	// the name-only DO block, so the reverse/down direction keeps the table-scoped
	// drop it had before issue #206.
	scoped := false
	for _, info := range removalsByName[name] {
		if info.TableName == "" {
			continue
		}
		result = p.appendScopedDrop(result, info.TableName, info.Name, droppedForModify)
		scoped = true
	}
	if scoped {
		return result
	}
	// No host recorded for this name — fall back to the runtime DO block.
	if _, done := droppedForModify[name]; done {
		return result
	}
	droppedForModify[name] = struct{}{}
	return append(result, p.dropConstraintNode(name))
}

// appendScopedDrop appends a single direct, table-qualified
// ALTER TABLE <table> DROP CONSTRAINT IF EXISTS <name>, deduped per (table, name)
// via droppedForModify so a constraint name shared across host tables is dropped
// once per host and never twice for the same host.
func (p *Planner) appendScopedDrop(result []ast.Node, table, name string, droppedForModify map[string]struct{}) []ast.Node {
	dedupKey := table + "." + name
	if _, done := droppedForModify[dedupKey]; done {
		return result
	}
	droppedForModify[dedupKey] = struct{}{}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: name,
			IfExists:       true,
		}},
	})
}

// foreignKeyAdditionNode builds the ALTER TABLE ADD CONSTRAINT node for a
// table-qualified field-level FK addition (ConstraintsAddedWithTables). The
// table comes straight from the comparator's synthesized constraint, so this
// path is correct for FK names that repeat across the many tables embedding an
// inline-relation mixin (issue #197), unlike the legacy field-scan fallback
// that re-derived the table from a Go struct name.
func (p *Planner) foreignKeyAdditionNode(add types.ConstraintAdditionInfo) *ast.AlterTableNode {
	fkRef := &ast.ForeignKeyRef{
		Table:    add.ForeignTable,
		Column:   add.ForeignColumn,
		OnDelete: add.OnDelete,
		OnUpdate: add.OnUpdate,
	}
	return p.createForeignKeyAlterStatement(add.TableName, add.Name, add.Columns, fkRef)
}

// addConstraintNodeFor resolves the ADD CONSTRAINT node for a constraint known
// only by name, trying the explicit table-level constraints first and then the
// synthesized field-level check= / foreign= fallbacks (see addNewConstraints).
// The returned bool reports whether a matching definition was found; the node
// may still be nil when a match exists but produces no valid AST (e.g. an
// EXCLUDE constraint, which convertConstraintToAST cannot represent).
func (p *Planner) addConstraintNodeFor(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, constraint := range generated.Constraints {
		if constraint.Name != constraintName {
			continue
		}
		if astConstraint := p.convertConstraintToAST(constraint); astConstraint != nil {
			return &ast.AlterTableNode{
				Name:       constraint.Table,
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: astConstraint}},
			}, true
		}
		return nil, true
	}

	if node, ok := p.fieldLevelCheckConstraintNode(constraintName, generated, structToTable); ok {
		return node, true
	}

	return p.fieldLevelForeignKeyConstraintNode(constraintName, generated, structToTable)
}

// fieldLevelCheckConstraintNode builds the ADD CONSTRAINT node for a synthesized
// field-level check= constraint (issue #112 / PR #123). New columns are handled
// by the inline CHECK in ALTER TABLE ADD COLUMN, and the comparator deliberately
// skips synthesizing those, so only existing-column field-level CHECKs reach
// here.
func (p *Planner) fieldLevelCheckConstraintNode(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range generated.Fields {
		if f.Check == "" {
			continue
		}
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}
		name := f.CheckName
		if name == "" {
			name = unqualifiedTableName(tableName) + "_" + f.Name + "_check"
		}
		if name != constraintName {
			continue
		}
		return &ast.AlterTableNode{
			Name: tableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type:       ast.CheckConstraint,
				Name:       name,
				Expression: f.Check,
			}}},
		}, true
	}
	return nil, false
}

// fieldLevelForeignKeyConstraintNode builds the ADD CONSTRAINT node for a
// synthesized field-level foreign= constraint whose on_delete / on_update action
// changed (issue #189). Without this the FK would be dropped (via
// removeConstraints) but never re-added with the new action — a destructive,
// silently-broken migration. New columns/tables are handled by the inline FK in
// CREATE TABLE / ALTER TABLE ADD COLUMN and the comparator deliberately skips
// synthesizing those, so only existing-column FK action changes reach here.
func (p *Planner) fieldLevelForeignKeyConstraintNode(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range generated.Fields {
		if f.Foreign == "" {
			continue
		}
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}
		name := foreignKeyName(unqualifiedTableName(tableName), f)
		if name != constraintName {
			continue
		}
		fkRef := fromschema.ParseForeignKeyReference(f.Foreign)
		if fkRef == nil {
			continue
		}
		if table := findGeneratedTableByDiffName(generated, tableName); table != nil {
			qualifyForeignKeyRef(generated, *table, fkRef)
		}
		fkRef.OnDelete = f.OnDelete
		fkRef.OnUpdate = f.OnUpdate
		return p.createForeignKeyAlterStatement(tableName, name, []string{f.Name}, fkRef), true
	}
	return nil, false
}

func unqualifiedTableName(tableName string) string {
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		return tableName[idx+1:]
	}
	return tableName
}

// removeConstraints removes table-level constraints via ALTER TABLE statements.
//
// This method generates ALTER TABLE DROP CONSTRAINT statements for constraints
// that exist in the database but not in the generated schema.
//
// # Safety Considerations
//
// Dropping constraints can affect data integrity and application behavior:
//   - Removing CHECK constraints may allow invalid data
//   - Removing UNIQUE constraints may allow duplicate data
//   - Removing FOREIGN KEY constraints may allow orphaned records
//   - Removing EXCLUDE constraints may allow conflicting data
//
// # Example Generated SQL
//
//	ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlapping_bookings;
//	ALTER TABLE products DROP CONSTRAINT IF EXISTS positive_price;
func (p *Planner) removeConstraints(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// A removed constraint is dropped from its exact owning table with a direct,
	// table-qualified ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>. The
	// comparator records that host in ConstraintsRemovedWithTables in lockstep
	// with the bare ConstraintsRemoved name list, so real diff output always
	// carries it.
	//
	// The name-only information_schema DO block (dropConstraintNode) is used ONLY
	// as a defensive fallback for a synthetic, hand-built diff that lists a
	// removed constraint by name with no ConstraintsRemovedWithTables host — it
	// resolves the owning table at execution time via information_schema LIMIT 1.
	// That LIMIT 1 lookup is unsafe for real removals because PostgreSQL
	// constraint names are unique per table, not per schema, so a same-named
	// constraint could be dropped from the WRONG table (issue #199), and a name
	// that lands on multiple host tables would be dropped from only one of them
	// (issue #197). The table-qualified drop avoids both.
	//
	// A constraint that appears in BOTH the additions and the removals for the
	// SAME (table, name) is a modification (the comparator expresses a changed
	// constraint as remove + add of the same name). Those are emitted as
	// DROP-then-ADD by addNewConstraints, which runs earlier in the pipeline so
	// the drop precedes the re-add; dropping them again here would remove the
	// freshly added constraint, so they are skipped.
	//
	// The skip MUST be keyed on (table, name), not the bare name. A shared
	// constraint name can be a modify on host A (its name lands in
	// ConstraintsAdded) and a PURE removal on host B (B has no addition). Keying
	// the skip on the name alone would treat B's removal as a modify owned by
	// addNewConstraints and skip it, leaving the stale constraint on B forever
	// (issue #206). The comparator records every addition's host in
	// ConstraintsAddedWithTables in lockstep with the bare list, so the modify
	// owner is always known per host.
	modifySet := make(map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	addedHostCounts := make(map[string]int, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// Hostless addition entries do not count as recorded hosts —
			// mirroring addedHostsByName in addNewConstraints — so the
			// hostless-re-add rule below still engages (issue #229).
			continue
		}
		modifySet[add.TableName+"."+add.Name] = struct{}{}
		addedHostCounts[add.Name]++
	}

	// Bare added names, for re-adds whose hosts were NOT recorded
	// (ConstraintsAdded carries the name but ConstraintsAddedWithTables has no
	// entry for it — reverse/down diffs of non-FK modifies, legacy callers
	// without an introspected schema, and hand-built diffs). For those,
	// emitModifyDropForName cannot restrict its pre-drop to the re-added
	// hosts, so it drops EVERY recorded removal host BEFORE the re-add;
	// dropping any of them again here would land AFTER the re-add and delete
	// the freshly restored constraint — IF EXISTS is no protection against
	// dropping a constraint that now exists again. This silently destroyed
	// the constraint on every non-FK down migration (issue #229).
	addedBareNamesHosted := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedBareNamesHosted[name] = struct{}{}
	}

	// When the comparator supplied the owning table (ConstraintsRemovedWithTables),
	// drop the constraint from that exact table with a direct ALTER TABLE … DROP
	// CONSTRAINT IF EXISTS, deduped per (table, name) via appendScopedDrop. This
	// is required for a field-level FK whose name repeats across the many tables
	// embedding an inline-relation mixin (issue #197): the name-only DO block
	// below resolves a single table via information_schema LIMIT 1, so it would
	// drop the constraint from only one of the host tables and silently leave the
	// rest. Names that carried at least one host are recorded so the bare
	// fallback below — which exists only for synthetic diffs — does not re-emit
	// the name-only DO block for them.
	dropped := make(map[string]struct{})
	namesWithHost := make(map[string]struct{})
	for _, info := range diff.ConstraintsRemovedWithTables {
		if info.TableName == "" {
			// No host recorded for this entry; defer to the bare fallback.
			continue
		}
		namesWithHost[info.Name] = struct{}{}
		if _, modified := modifySet[info.TableName+"."+info.Name]; modified {
			// addNewConstraints owns this host's DROP-then-ADD; do not re-drop.
			continue
		}
		if _, added := addedBareNamesHosted[info.Name]; added && addedHostCounts[info.Name] == 0 {
			// Hostless re-add: addNewConstraints already dropped every
			// recorded removal host for this name before the re-add
			// (emitModifyDropForName with unknown addedHosts). A second drop
			// here would follow the re-add and delete the fresh constraint
			// (issue #229).
			continue
		}
		result = p.appendScopedDrop(result, info.TableName, info.Name, dropped)
	}

	// Bare fallback for synthetic diffs only: a hand-built diff may list a
	// removed constraint by name with no ConstraintsRemovedWithTables host. Such
	// names have genuinely no table to scope by, so the runtime information_schema
	// DO block (dropConstraintNode) remains the only option. Real comparator
	// output always carries the host, so it is fully handled above and skipped
	// here. A bare modify (name in ConstraintsAdded with no recorded host) is
	// owned by addNewConstraints and skipped.
	addedBareNames := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedBareNames[name] = struct{}{}
	}
	for _, constraintName := range diff.ConstraintsRemoved {
		if _, hadHost := namesWithHost[constraintName]; hadHost {
			continue
		}
		if _, modified := addedBareNames[constraintName]; modified {
			continue
		}
		result = append(result, p.dropConstraintNode(constraintName))
	}
	return result
}

// dropConstraintNode builds a self-contained DROP CONSTRAINT statement for a
// constraint known only by name. The diff layer discards the table name for
// removed constraints (field-level CHECK / FK constraints are synthesized and
// presented by name alone), so the table is resolved at execution time from
// information_schema via a DO block.
//
// Constraint-name safety. Postgres constraint names should be plain ASCII
// alnum + underscore; we reject only the chars that would actually break our
// specific DO-block template:
//   - `$` would collide with the `$ptah$` dollar-quote tag and terminate the
//     body early.
//   - newline / carriage return would terminate the leading `--` comment line
//     and dump whatever follows as bare SQL.
//
// Anything else (apostrophe) is handled by SQL-literal escaping. Unsafe names
// produce a DO block whose only action is RAISE EXCEPTION, so the migration
// fails loudly rather than silently looping forever on subsequent runs.
func (p *Planner) dropConstraintNode(constraintName string) ast.Node {
	escaped := strings.ReplaceAll(constraintName, "'", "''")
	if strings.ContainsAny(constraintName, "$\n\r") {
		// Build a printable, single-quoted SQL string literal of the
		// rejected name so the operator's error output shows what was
		// rejected. `$` is rendered as `\$` so the surrounding `$ptah$`
		// dollar quoting can't be prematurely terminated; `\n` / `\r` /
		// `\t` are rendered as their printable escapes; apostrophes are
		// SQL-escaped via `''`. The result is plain ASCII inside `'…'`.
		visible := strings.NewReplacer(
			"\n", `\n`,
			"\r", `\r`,
			"\t", `\t`,
			"$", `\$`,
		).Replace(constraintName)
		visible = strings.ReplaceAll(visible, "'", "''")

		failBlock := fmt.Sprintf(`-- Unsafe constraint name rejected by the migration generator; the
-- following DO block raises an exception so the migration fails loudly.
DO $ptah$
BEGIN
    RAISE EXCEPTION 'refusing to drop constraint with unsafe name ''%s''; rename the constraint and regenerate the migration';
END
$ptah$`, visible)
		return ast.NewRawSQL(failBlock)
	}
	doBlock := fmt.Sprintf(`-- Drop constraint %s (table resolved at runtime from information_schema)
DO $ptah$
DECLARE
    target_table TEXT;
BEGIN
    SELECT table_name INTO target_table
    FROM information_schema.table_constraints
    WHERE constraint_name = '%s'
      AND table_schema = current_schema()
    LIMIT 1;

    IF target_table IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %%I DROP CONSTRAINT IF EXISTS %%I', target_table, '%s');
        RAISE NOTICE 'Dropped constraint %s from table %%', target_table;
    ELSE
        RAISE NOTICE 'Constraint %s not found in current schema';
    END IF;
END
$ptah$`, constraintName, escaped, escaped, escaped, escaped)

	return ast.NewRawSQL(doBlock)
}

// convertConstraintToAST converts a goschema.Constraint to an ast.ConstraintNode.
//
// This helper method handles the conversion between the schema annotation representation
// and the AST representation used for SQL generation.
func (p *Planner) convertConstraintToAST(constraint goschema.Constraint) *ast.ConstraintNode {
	switch constraint.Type {
	case "EXCLUDE":
		if constraint.UsingMethod == "" || constraint.ExcludeElements == "" {
			return nil // Invalid EXCLUDE constraint
		}
		astConstraint := ast.NewExcludeConstraint(constraint.Name, constraint.UsingMethod, constraint.ExcludeElements)
		if constraint.WhereCondition != "" {
			astConstraint.SetWhereCondition(constraint.WhereCondition)
		}
		return astConstraint

	case "CHECK":
		if constraint.CheckExpression == "" {
			return nil // Invalid CHECK constraint
		}
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       constraint.Name,
			Expression: constraint.CheckExpression,
		}

	case "UNIQUE":
		if len(constraint.Columns) == 0 {
			return nil // Invalid UNIQUE constraint
		}
		return ast.NewUniqueConstraint(constraint.Name, constraint.Columns...)

	case "PRIMARY KEY":
		if len(constraint.Columns) == 0 {
			return nil // Invalid PRIMARY KEY constraint
		}
		return ast.NewPrimaryKeyConstraint(constraint.Columns...)

	case "FOREIGN KEY":
		if len(constraint.Columns) == 0 || constraint.ForeignTable == "" || constraint.ForeignColumn == "" {
			return nil // Invalid FOREIGN KEY constraint
		}
		ref := &ast.ForeignKeyRef{
			Table:    constraint.ForeignTable,
			Column:   constraint.ForeignColumn,
			OnDelete: constraint.OnDelete,
			OnUpdate: constraint.OnUpdate,
			Name:     constraint.Name,
		}
		return ast.NewForeignKeyConstraint(constraint.Name, constraint.Columns, ref)

	default:
		return nil // Unsupported constraint type
	}
}

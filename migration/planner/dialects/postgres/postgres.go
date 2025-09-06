package postgres

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
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
// The Planner is stateless and safe for concurrent use across multiple goroutines.
// Each call to GenerateMigrationSQL operates independently without shared state.
type Planner struct {
}

func New() *Planner {
	return &Planner{}
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

	// Phase 1: Create tables without foreign key constraints
	result = p.createTablesWithoutForeignKeys(result, generated, tablesToAdd)

	// Phase 2: Add foreign key constraints via ALTER TABLE statements
	result = p.addForeignKeyConstraints(result, generated, tablesToAdd)

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
		if !tablesToAdd[table.Name] {
			continue
		}

		astNode := ast.NewCreateTable(table.Name)
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
		if !tablesToAdd[table.Name] {
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
		if fkRef != nil && fkRef.Table != table.Name {
			result = append(result, p.createForeignKeyAlterStatement(table.Name, field.ForeignKeyName, []string{field.Name}, fkRef))
		}
	}
	return result
}

// addSelfReferencingForeignKeys adds self-referencing foreign key constraints
func (p *Planner) addSelfReferencingForeignKeys(result []ast.Node, generated *goschema.Database, table goschema.Table) []ast.Node {
	selfRefFKs, exists := generated.SelfReferencingForeignKeys[table.Name]
	if !exists {
		return result
	}

	for _, selfRefFK := range selfRefFKs {
		fkRef := fromschema.ParseForeignKeyReference(selfRefFK.Foreign)
		if fkRef != nil {
			result = append(result, p.createForeignKeyAlterStatement(table.Name, selfRefFK.ForeignKeyName, []string{selfRefFK.FieldName}, fkRef))
		}
	}

	return result
}

// isRegularForeignKeyField checks if a field is a regular foreign key field for the given table
func isRegularForeignKeyField(field goschema.Field, table goschema.Table) bool {
	return field.StructName == table.StructName && field.Foreign != "" && field.ForeignKeyName != ""
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
		for _, table := range generated.Tables {
			if table.Name == tableDiff.TableName {
				targetStructName = table.StructName
				break
			}
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		if targetField != nil {
			columnNode := fromschema.FromField(*targetField, generated.Enums, "postgres")

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

		// First, find the struct name for this table
		for _, table := range generated.Tables {
			if table.Name == tableDiff.TableName {
				targetStructName = table.StructName
				break
			}
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		// Only process fields that have foreign key constraints
		if targetField != nil && targetField.Foreign != "" && targetField.ForeignKeyName != "" {
			// Parse the foreign key reference
			fkRef := fromschema.ParseForeignKeyReference(targetField.Foreign)
			if fkRef != nil {
				fkRef.Name = targetField.ForeignKeyName

				// Create foreign key constraint
				fkConstraint := ast.NewForeignKeyConstraint(
					targetField.ForeignKeyName,
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
		for _, table := range generated.Tables {
			if table.Name == tableDiff.TableName {
				targetStructName = table.StructName
				break
			}
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
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: columnNode}},
		}
		result = append(result, alterNode)

		// Add a comment showing what changes are being made
		changesList := make([]string, 0, len(colDiff.Changes))
		for changeType, change := range colDiff.Changes {
			changesList = append(changesList, fmt.Sprintf("%s: %s", changeType, change))
		}
		astCommentNode := ast.NewComment(fmt.Sprintf("Modify column %s.%s: %s", tableDiff.TableName, colDiff.ColumnName, strings.Join(changesList, ", ")))
		result = append(result, astCommentNode)
	}
	return result
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
		structToTableMap[table.StructName] = table.Name
	}

	for _, indexName := range diff.IndexesAdded {
		// Find the index definition
		for _, idx := range generated.Indexes {
			if idx.Name == indexName {
				// Use enhanced index creation with PostgreSQL features
				indexNode := fromschema.FromIndexWithTableMapping(idx, structToTableMap)
				result = append(result, indexNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) removeIndexes(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, indexName := range diff.IndexesRemoved {
		dropIndexNode := ast.NewDropIndex(indexName).
			SetIfExists()
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
	result = p.addNewRoles(result, diff, generated)

	// 2. Add new functions (functions may be used by RLS policies)
	result = p.addNewFunctions(result, diff, generated)

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

	// 7. Modify existing roles (must be done before RLS policies that reference them)
	result = p.modifyExistingRoles(result, diff, generated)

	// 8. Enable RLS on tables (must be done after table creation and modification)
	result = p.enableRLSOnTables(result, diff, generated)

	// 9. Add RLS policies (must be done after RLS is enabled and columns exist)
	result = p.addNewRLSPolicies(result, diff, generated)

	// 10. Add new indexes
	result = p.addNewIndexes(result, diff, generated)

	// 10.5. Add new constraints (must be done after tables and columns exist)
	result = p.addNewConstraints(result, diff, generated)

	// 11. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 12. Remove RLS policies (must be done before disabling RLS and before dropping columns)
	result = p.removeRLSPolicies(result, diff)

	// 11. Disable RLS on tables (must be done after removing policies)
	result = p.disableRLSOnTables(result, diff)

	// 12. Remove table columns (must be done after removing RLS policies that depend on columns)
	result = p.removeTableColumns(result, diff)

	// 12.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff)

	// 13. Remove tables (dangerous!)
	result = p.removeTables(result, diff)

	// 13. Remove functions (must be done after removing policies that might use them)
	result = p.removeFunctions(result, diff)

	// 14. Remove roles (must be done after removing functions and policies that depend on them)
	result = p.removeRoles(result, diff)

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

	for changeType, changeValue := range roleDiff.Changes {
		p.addRoleOperation(alterRoleNode, changeType, changeValue, targetRole)
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

func (p *Planner) removeFunctions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, functionName := range diff.FunctionsRemoved {
		dropFunctionNode := ast.NewDropFunction(functionName).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this function")
		result = append(result, dropFunctionNode)
	}
	return result
}

func (p *Planner) enableRLSOnTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Create a set of tables that need RLS enabled
	tablesNeedingRLS := make(map[string]bool)
	for _, policy := range generated.RLSPolicies {
		tablesNeedingRLS[policy.Table] = true
	}

	// Enable RLS on tables that have policies but don't have RLS enabled yet
	for tableName := range tablesNeedingRLS {
		// Check if this table is being added or if RLS is being enabled
		tableIsNew := false
		for _, addedTable := range diff.TablesAdded {
			if addedTable == tableName {
				tableIsNew = true
				break
			}
		}

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
	for tableName := range tablesWithRemovedPolicies {
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
	for _, constraintName := range diff.ConstraintsAdded {
		// Find the constraint definition in the generated schema
		for _, constraint := range generated.Constraints {
			if constraint.Name == constraintName {
				// Convert goschema.Constraint to ast.ConstraintNode
				astConstraint := p.convertConstraintToAST(constraint)
				if astConstraint != nil {
					// Create ALTER TABLE statement with ADD CONSTRAINT operation
					alterNode := &ast.AlterTableNode{
						Name:       constraint.Table,
						Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: astConstraint}},
					}
					result = append(result, alterNode)
				}
				break
			}
		}
	}
	return result
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
	for _, constraintName := range diff.ConstraintsRemoved {
		// Create a temporary function that finds and drops the constraint dynamically
		functionName := fmt.Sprintf("ptah_drop_constraint_%s", constraintName)

		functionBody := fmt.Sprintf(`DECLARE
    target_table TEXT;
BEGIN
    -- Find the table that contains this constraint
    SELECT table_name INTO target_table
    FROM information_schema.table_constraints
    WHERE constraint_name = '%s'
      AND table_schema = current_schema()
    LIMIT 1;

    -- Drop the constraint if found
    IF target_table IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %%I DROP CONSTRAINT IF EXISTS %%I', target_table, '%s');
        RAISE NOTICE 'Dropped constraint %s from table %%', target_table;
    ELSE
        RAISE NOTICE 'Constraint %s not found in current schema';
    END IF;
END;`, constraintName, constraintName, constraintName, constraintName)

		// Step 1: Create the function
		createFunctionNode := ast.NewCreateFunction(functionName).
			SetReturns("VOID").
			SetLanguage("plpgsql").
			SetBody(functionBody).
			SetComment(fmt.Sprintf("Temporary function to drop constraint %s", constraintName))

		result = append(result, createFunctionNode)

		// Step 2: Execute the function using a simple SQL function call
		executeFunctionBody := fmt.Sprintf("SELECT %s();", functionName)
		executeFunctionNode := ast.NewCreateFunction(fmt.Sprintf("ptah_exec_%s", functionName)).
			SetReturns("VOID").
			SetLanguage("sql").
			SetBody(executeFunctionBody).
			SetComment(fmt.Sprintf("Execute constraint removal for %s", constraintName))

		result = append(result, executeFunctionNode)

		// Step 3: Drop the temporary functions
		dropMainFunctionNode := ast.NewDropFunction(functionName).
			SetIfExists().
			SetComment(fmt.Sprintf("Clean up temporary function for %s", constraintName))

		result = append(result, dropMainFunctionNode)

		dropExecFunctionNode := ast.NewDropFunction(fmt.Sprintf("ptah_exec_%s", functionName)).
			SetIfExists().
			SetComment(fmt.Sprintf("Clean up executor function for %s", constraintName))

		result = append(result, dropExecFunctionNode)
	}
	return result
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

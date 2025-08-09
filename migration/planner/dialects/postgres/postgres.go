package postgres

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
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
	// Use the already processed fields from walker.go (embedded fields are now processed earlier)
	allFields := generated.Fields

	// Create a set of tables to add for quick lookup
	tablesToAdd := make(map[string]bool)
	for _, tableName := range diff.TablesAdded {
		tablesToAdd[tableName] = true
	}

	// Iterate through tables in dependency order (generated.Tables is already sorted)
	// This ensures foreign key constraints are satisfied during table creation
	for _, table := range generated.Tables {
		if !tablesToAdd[table.Name] {
			continue // Skip tables that are not being added
		}

		astNode := ast.NewCreateTable(table.Name)
		for _, field := range allFields {
			if field.StructName == table.StructName {
				columnNode := fromschema.FromField(field, generated.Enums, "postgres")
				astNode.AddColumn(columnNode)
			}
		}
		result = append(result, astNode)
	}
	return result
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
			// Generate ADD COLUMN statement using AST
			alterNode := &ast.AlterTableNode{
				Name:       tableDiff.TableName,
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: columnNode}},
			}
			result = append(result, alterNode)
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
			astCommentNode := ast.NewComment(fmt.Sprintf("Add/modify columns for table: %s", tableDiff.TableName))
			result = append(result, astCommentNode)

			// Add new columns
			result = p.addNewTableColumns(result, tableDiff, generated)

			// Modify existing columns
			result = p.modifyExistingTableColumns(result, tableDiff, generated)
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

	// 7. Modify existing roles (must be done before RLS policies that reference them)
	result = p.modifyExistingRoles(result, diff, generated)

	// 8. Enable RLS on tables (must be done after table creation and modification)
	result = p.enableRLSOnTables(result, diff, generated)

	// 9. Add RLS policies (must be done after RLS is enabled and columns exist)
	result = p.addNewRLSPolicies(result, diff, generated)

	// 10. Add new indexes
	result = p.addNewIndexes(result, diff, generated)

	// 11. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 12. Remove RLS policies (must be done before disabling RLS and before dropping columns)
	result = p.removeRLSPolicies(result, diff)

	// 11. Disable RLS on tables (must be done after removing policies)
	result = p.disableRLSOnTables(result, diff)

	// 12. Remove table columns (must be done after removing RLS policies that depend on columns)
	result = p.removeTableColumns(result, diff)

	// 12. Remove tables (dangerous!)
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
		// Find the role definition to get the current state
		var targetRole *goschema.Role
		for _, role := range generated.Roles {
			if role.Name == roleDiff.RoleName {
				targetRole = &role
				break
			}
		}

		if targetRole == nil {
			continue // Skip if role not found in target schema
		}

		// Create ALTER ROLE node with operations based on changes
		alterRoleNode := ast.NewAlterRole(roleDiff.RoleName)

		// Process each change and add corresponding operations
		for changeType, changeValue := range roleDiff.Changes {
			switch changeType {
			case "login":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetLoginOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetLoginOperation(false))
				}
			case "password":
				// Extract new password from "old -> new" format
				parts := strings.Split(changeValue, " -> ")
				if len(parts) == 2 {
					newPassword := parts[1]
					alterRoleNode.AddOperation(ast.NewSetPasswordOperation(newPassword))
				}
			case "superuser":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(false))
				}
			case "createdb", "create_db":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(false))
				}
			case "createrole", "create_role":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(false))
				}
			case "inherit":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetInheritOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetInheritOperation(false))
				}
			case "replication":
				if strings.Contains(changeValue, "-> true") {
					alterRoleNode.AddOperation(ast.NewSetReplicationOperation(true))
				} else if strings.Contains(changeValue, "-> false") {
					alterRoleNode.AddOperation(ast.NewSetReplicationOperation(false))
				}
			}
		}

		// Only add the ALTER ROLE statement if there are operations to perform
		if len(alterRoleNode.Operations) > 0 {
			alterRoleNode.SetComment(fmt.Sprintf("Modify role %s attributes", roleDiff.RoleName))
			result = append(result, alterRoleNode)
		}
	}
	return result
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

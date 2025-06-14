package mysql

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// Planner implements MySQL-specific migration planning functionality.
//
// The Planner is responsible for converting schema differences into MySQL-compatible
// AST nodes that can be rendered into executable SQL statements. It handles MySQL-specific
// features like inline ENUM types, AUTO_INCREMENT columns, and proper dependency ordering.
//
// # Usage Example
//
//	planner := &mysql.Planner{}
//
//	// Schema differences from comparison
//	diff := &differtypes.SchemaDiff{
//		TablesAdded: []string{"users"},
//	}
//
//	// Target schema from Go struct parsing
//	generated := &goschema.Database{
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "AUTO_INCREMENT", StructName: "User", Primary: true},
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

func (p *Planner) addEnumChangeWarnings(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	if len(diff.EnumsAdded) > 0 {
		astCommentNode := ast.NewComment(fmt.Sprintf("NOTE: MySQL enums are inline in column definitions. New enums: %v", diff.EnumsAdded))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) handleEnumModifications(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, enumDiff := range diff.EnumsModified {
		if len(enumDiff.ValuesAdded) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: MySQL enum modifications require ALTER TABLE for each column using enum %s. Values added: %v", enumDiff.EnumName, enumDiff.ValuesAdded))
			result = append(result, astCommentNode)
		}
		if len(enumDiff.ValuesRemoved) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: MySQL cannot remove enum values from %s without recreating the table. Values removed: %v", enumDiff.EnumName, enumDiff.ValuesRemoved))
			result = append(result, astCommentNode)
		}
	}
	return result
}

func (p *Planner) addNewTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, tableName := range diff.TablesAdded {
		// Find the table in generated schema and create it
		for _, table := range generated.Tables {
			if table.Name == tableName {
				astNode := ast.NewCreateTable(tableName)
				for _, field := range generated.Fields {
					if field.StructName == table.StructName {
						columnNode := fromschema.FromField(field, generated.Enums, "mysql")
						astNode.AddColumn(columnNode)
					}
				}
				result = append(result, astNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) addNewTableColumns(result []ast.Node, tableDiff *types.TableDiff, generated *goschema.Database) []ast.Node {
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
			columnNode := fromschema.FromField(*targetField, generated.Enums, "mysql")
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

func (p *Planner) modifyExistingColumns(result []ast.Node, tableDiff *types.TableDiff, generated *goschema.Database) []ast.Node {
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
		columnNode := fromschema.FromField(*targetField, generated.Enums, "mysql")

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

func (p *Planner) removeColumns(result []ast.Node, tableDiff *types.TableDiff) []ast.Node {
	for _, colName := range tableDiff.ColumnsRemoved {
		// Generate DROP COLUMN statement using AST
		alterNode := &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: colName}},
		}
		result = append(result, alterNode)
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: Dropping column %s.%s - This will delete data!", tableDiff.TableName, colName))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) modifyExistingTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		astCommentNode := ast.NewComment(fmt.Sprintf("Modify table: %s", tableDiff.TableName))
		result = append(result, astCommentNode)

		// Add new columns
		result = p.addNewTableColumns(result, &tableDiff, generated)

		// Modify existing columns
		result = p.modifyExistingColumns(result, &tableDiff, generated)

		// Remove columns (dangerous!)
		result = p.removeColumns(result, &tableDiff)
	}
	return result
}

func (p *Planner) addNewIndexes(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, indexName := range diff.IndexesAdded {
		// Find the index definition
		for _, idx := range generated.Indexes {
			if idx.Name == indexName {
				indexNode := ast.NewIndex(idx.Name, idx.StructName, idx.Fields...)
				if idx.Unique {
					indexNode.Unique = true
				}
				if idx.Comment != "" {
					indexNode.Comment = idx.Comment
				}
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

func (p *Planner) handleEnumRemovals(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, enumName := range diff.EnumsRemoved {
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: MySQL enum %s removal requires updating all tables that use this enum type!", enumName))
		result = append(result, astCommentNode)
	}
	return result
}

// GenerateMigrationAST generates MySQL-specific migration AST statements from schema differences.
//
// This method transforms the schema differences captured in the SchemaDiff into executable
// MySQL AST statements that can be applied to bring the database schema in line with the target
// schema. The generated AST follows MySQL-specific syntax and best practices.
//
// # Migration Order
//
// The SQL statements are generated in a specific order to avoid dependency conflicts:
//  1. Create new tables (MySQL handles enums inline, no separate enum creation needed)
//  2. Modify existing tables (add/modify/remove columns)
//  3. Add new indexes
//  4. Remove indexes (safe operations)
//  5. Remove tables (dangerous - commented out by default)
//
// # MySQL-Specific Features
//
//   - Inline ENUM types in column definitions (no separate CREATE TYPE statements)
//   - AUTO_INCREMENT columns for auto-increment functionality
//   - MySQL-specific syntax for ALTER statements
//   - Engine specifications (InnoDB, MyISAM, etc.)
//
// # Parameters
//
//   - diff: The schema differences to be applied
//   - generated: The target schema parsed from Go struct annotations
//
// # Examples
//
// Basic table creation with inline enum:
//
//	diff := &differtypes.SchemaDiff{
//		TablesAdded: []string{"users"},
//	}
//
//	generated := &goschema.Database{
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "INT AUTO_INCREMENT", StructName: "User", Primary: true},
//			{Name: "status", Type: "ENUM('active','inactive')", StructName: "User"},
//		},
//	}
//
//	nodes := planner.GenerateMigrationAST(diff, generated)
//	// Results in:
//	// CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, status ENUM('active','inactive'));
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
// to SQL using a MySQL-specific visitor. Comments and warnings are included
// as CommentNode instances for documentation and safety.
func (p *Planner) GenerateMigrationAST(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node

	// Note: MySQL doesn't use separate enum types like PostgreSQL
	// Enums are handled inline in column definitions, so we skip enum creation steps

	// 1. Add enum change warnings (MySQL limitations)
	result = p.addEnumChangeWarnings(result, diff)

	// 2. Handle enum modifications (MySQL limitations)
	result = p.handleEnumModifications(result, diff)

	// 3. Add new tables
	result = p.addNewTables(result, diff, generated)

	// 4. Modify existing tables
	result = p.modifyExistingTables(result, diff, generated)

	// 5. Add new indexes
	result = p.addNewIndexes(result, diff, generated)

	// 6. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 7. Remove tables (dangerous!)
	result = p.removeTables(result, diff)

	// 8. Handle enum removals (MySQL-specific warnings)
	result = p.handleEnumRemovals(result, diff)

	return result
}

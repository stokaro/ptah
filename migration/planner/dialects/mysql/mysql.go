package mysql

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

const (
	// DialectName is the MySQL dialect identifier
	DialectName = "mysql"
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

			// Create operations list starting with ADD COLUMN
			operations := []ast.AlterOperation{&ast.AddColumnOperation{Column: columnNode}}

			// If the column has a foreign key, add a separate ADD CONSTRAINT operation
			if targetField.Foreign != "" && targetField.ForeignKeyName != "" {
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

					// Add the constraint operation
					operations = append(operations, &ast.AddConstraintOperation{Constraint: fkConstraint})
				}
			}

			// Generate ALTER TABLE statement with all operations
			alterNode := &ast.AlterTableNode{
				Name:       tableDiff.TableName,
				Operations: operations,
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
	// Use the detailed removal info if available (includes table names for MySQL/MariaDB)
	if len(diff.IndexesRemovedWithTables) > 0 {
		for _, indexInfo := range diff.IndexesRemovedWithTables {
			dropIndexNode := ast.NewDropIndex(indexInfo.Name).
				SetIfExists().
				SetTable(indexInfo.TableName)
			result = append(result, dropIndexNode)
		}
	} else {
		// Fallback to the basic removal list (for backward compatibility)
		for _, indexName := range diff.IndexesRemoved {
			dropIndexNode := ast.NewDropIndex(indexName).
				SetIfExists()
			result = append(result, dropIndexNode)
		}
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

	// 5.5. Add new constraints (must be done after tables and columns exist)
	result = p.addNewConstraints(result, diff, generated)

	// 6. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 6.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff)

	// 7. Remove tables (dangerous!)
	result = p.removeTables(result, diff)

	// 8. Handle enum removals (MySQL-specific warnings)
	result = p.handleEnumRemovals(result, diff)

	return result
}

// addNewConstraints adds new table-level constraints via ALTER TABLE statements.
//
// This method processes constraints defined through Go struct annotations and creates
// appropriate ALTER TABLE ADD CONSTRAINT statements. Note that MySQL has different
// constraint support compared to PostgreSQL:
//
// # MySQL Constraint Limitations
//
//   - EXCLUDE constraints are not supported (PostgreSQL-specific)
//   - CHECK constraints have limited support in older MySQL versions
//   - Some constraint features may behave differently
//
// # Supported Constraint Types
//
//   - CHECK: Table-level CHECK constraints (MySQL 8.0.16+)
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
//
// # Example Generated SQL
//
//	ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);
//	ALTER TABLE users ADD CONSTRAINT uk_users_email_name UNIQUE (email, name);
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
				} else if constraint.Type == "EXCLUDE" {
					// Add warning for unsupported EXCLUDE constraints
					commentNode := ast.NewComment(fmt.Sprintf("WARNING: EXCLUDE constraint %s not supported in MySQL (PostgreSQL-specific feature)", constraint.Name))
					result = append(result, commentNode)
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
// # MySQL Constraint Removal
//
// MySQL uses different syntax for dropping different constraint types:
//   - DROP CONSTRAINT for named constraints (MySQL 8.0.19+)
//   - DROP INDEX for unique constraints in older versions
//   - DROP FOREIGN KEY for foreign key constraints
//
// # Example Generated SQL
//
//	ALTER TABLE products DROP CONSTRAINT positive_price;
//	ALTER TABLE users DROP CONSTRAINT uk_users_email_name;
func (p *Planner) removeConstraints(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, constraintName := range diff.ConstraintsRemoved {
		// For constraint removal, we need to determine which table the constraint belongs to
		// This is a limitation of the current approach - we don't track table names for removed constraints
		// For now, we'll add a comment indicating manual intervention may be needed
		commentNode := ast.NewComment(fmt.Sprintf("TODO: Remove constraint %s (table name unknown - manual intervention required)", constraintName))
		result = append(result, commentNode)
	}
	return result
}

// convertConstraintToAST converts a goschema.Constraint to an ast.ConstraintNode for MySQL.
//
// This helper method handles the conversion between the schema annotation representation
// and the AST representation used for SQL generation, taking into account MySQL-specific
// limitations and syntax differences.
func (p *Planner) convertConstraintToAST(constraint goschema.Constraint) *ast.ConstraintNode {
	switch constraint.Type {
	case "EXCLUDE":
		// EXCLUDE constraints are not supported in MySQL
		return nil

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

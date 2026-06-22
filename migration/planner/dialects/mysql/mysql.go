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
			fkRef.OnDelete = field.OnDelete
			fkRef.OnUpdate = field.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.Name, foreignKeyName(table.Name, field), []string{field.Name}, fkRef))
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
			fkRef.OnDelete = selfRefFK.OnDelete
			fkRef.OnUpdate = selfRefFK.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.Name, selfReferencingForeignKeyName(table.Name, selfRefFK), []string{selfRefFK.FieldName}, fkRef))
		}
	}

	return result
}

// isRegularForeignKeyField checks if a field is a regular foreign key field for the given table.
//
// A field-level foreign= annotation is a foreign key whether or not an explicit
// foreign_key_name= was supplied; when omitted the planner derives the
// conventional fk_<table>_<column> name (see foreignKeyName) so the constraint
// is actually created with a stable, named identity. MySQL in particular needs
// a known name to later emit ALTER TABLE ... DROP FOREIGN KEY for action drift
// (issue #189).
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

// selfReferencingForeignKeyName returns the constraint name for a
// self-referencing field-level foreign key, deriving the conventional
// fk_<table>_<field> name when foreign_key_name= was omitted.
func selfReferencingForeignKeyName(tableName string, fk goschema.SelfReferencingFK) string {
	if fk.ForeignKeyName != "" {
		return fk.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, fk.FieldName)
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
			if targetField.Foreign != "" {
				// Parse the foreign key reference
				fkRef := fromschema.ParseForeignKeyReference(targetField.Foreign)
				if fkRef != nil {
					fkName := foreignKeyName(tableDiff.TableName, *targetField)
					fkRef.Name = fkName
					fkRef.OnDelete = targetField.OnDelete
					fkRef.OnUpdate = targetField.OnUpdate

					// Create foreign key constraint
					fkConstraint := ast.NewForeignKeyConstraint(
						fkName,
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
// # Field-Level Fallbacks
//
// The schemadiff comparator synthesizes field-level check= and foreign= drift
// into diff.ConstraintsAdded by name only — those constraints never reach
// generated.Constraints. addNewConstraints therefore falls back to resolving
// the constraint from the field annotations (mirroring the PostgreSQL planner)
// so an existing-column CHECK/FK drift is actually re-emitted instead of being
// silently dropped.
//
// # Modifications (DROP-before-ADD)
//
// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved is
// a modification (the comparator expresses a changed constraint as remove + add
// of the same name — e.g. an on_delete change on a field-level FK, issue #189).
// The DROP is emitted here, immediately before the re-ADD, so it precedes the
// re-add and removeConstraints (which runs later) skips it.
//
// # Example Generated SQL
//
//	ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);
//	ALTER TABLE users ADD CONSTRAINT uk_users_email_name UNIQUE (email, name);
//	ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id;
//	ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
func (p *Planner) addNewConstraints(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Resolve struct → table name once for the field-level synthesis fallbacks.
	structToTable := make(map[string]string, len(generated.Tables))
	for _, t := range generated.Tables {
		structToTable[t.StructName] = t.Name
	}

	// Removal info keyed by (table, name) so a same-name modification can be
	// dropped from each owning table with the correct FK-aware syntax before
	// being re-added. A mixin-shared FK name with on_delete/on_update drift on
	// >=2 host tables produces one ConstraintsRemovedWithTables entry per host
	// (same Name, distinct TableName); keying the map on the name alone would
	// collapse them to the last host, so only that host's old FK would be
	// dropped and the other hosts' ADD CONSTRAINT would collide with the
	// still-present same-named constraint ("Duplicate foreign key constraint
	// name", errno 1826). Keying on (table, name) keeps one removal per host so
	// each host gets its own DROP FOREIGN KEY. A single-host name still resolves
	// to exactly one entry, so #189 stays byte-identical (one DROP + one ADD).
	removalByTableName := make(map[string]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalByTableName[info.TableName+"."+info.Name] = info
	}

	// Prefer the table-qualified additions when present. A field-level FK from an
	// embedded inline-relation mixin shares one name across every host table, so
	// resolving the table from the field's Go struct name targets the mixin
	// struct rather than the real tables (issue #197). ConstraintsAddedWithTables
	// carries the concrete table + full FK definition. Names handled here are
	// recorded so the legacy name loop skips them.
	handled := make(map[string]struct{})
	droppedForModify := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// For a modification, emit the DROP FOREIGN KEY from this exact host
		// table before its re-add. Dedup on (table, name) so each host is
		// dropped once even if the comparator lists it more than once.
		dropKey := add.TableName + "." + add.Name
		if info, modified := removalByTableName[dropKey]; modified {
			if _, done := droppedForModify[dropKey]; !done {
				result = append(result, dropConstraintNode(info))
				droppedForModify[dropKey] = struct{}{}
			}
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		handled[add.Name] = struct{}{}
	}

	// Fallback for added constraints with no table-qualified entry above
	// (table-level CHECK/UNIQUE, or field-level synthesis resolved by name).
	// These names are unique per table, so a name-keyed removal lookup is
	// sufficient and there is no multi-host collapse hazard here.
	removalByName := make(map[string]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalByName[info.Name] = info
	}
	for _, constraintName := range diff.ConstraintsAdded {
		if _, done := handled[constraintName]; done {
			continue
		}

		// For a modification, emit the DROP first so it precedes the re-add.
		if info, modified := removalByName[constraintName]; modified {
			result = append(result, dropConstraintNode(info))
		}

		result = p.appendAddConstraint(result, constraintName, generated, structToTable)
	}
	return result
}

// foreignKeyAdditionNode builds the ALTER TABLE ADD CONSTRAINT node for a
// table-qualified field-level FK addition (ConstraintsAddedWithTables). The
// concrete table comes from the comparator, so this is correct for FK names
// that repeat across the many tables embedding an inline-relation mixin
// (issue #197), unlike the legacy field scan keyed on a Go struct name.
func (p *Planner) foreignKeyAdditionNode(add types.ConstraintAdditionInfo) *ast.AlterTableNode {
	fkRef := &ast.ForeignKeyRef{
		Table:    add.ForeignTable,
		Column:   add.ForeignColumn,
		OnDelete: add.OnDelete,
		OnUpdate: add.OnUpdate,
	}
	return p.createForeignKeyAlterStatement(add.TableName, add.Name, add.Columns, fkRef)
}

// appendAddConstraint resolves the ADD CONSTRAINT node for a constraint known
// only by name, trying the explicit table-level constraints first and then the
// synthesized field-level check= / foreign= fallbacks, mirroring the PostgreSQL
// planner.
func (p *Planner) appendAddConstraint(result []ast.Node, constraintName string, generated *goschema.Database, structToTable map[string]string) []ast.Node {
	for _, constraint := range generated.Constraints {
		if constraint.Name != constraintName {
			continue
		}
		if astConstraint := p.convertConstraintToAST(constraint); astConstraint != nil {
			return append(result, &ast.AlterTableNode{
				Name:       constraint.Table,
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: astConstraint}},
			})
		}
		if constraint.Type == "EXCLUDE" {
			return append(result, ast.NewComment(fmt.Sprintf("WARNING: EXCLUDE constraint %s not supported in MySQL (PostgreSQL-specific feature)", constraint.Name)))
		}
		return result
	}

	if node, ok := p.fieldLevelCheckConstraintNode(constraintName, generated, structToTable); ok {
		if node != nil {
			result = append(result, node)
		}
		return result
	}

	if node, ok := p.fieldLevelForeignKeyConstraintNode(constraintName, generated, structToTable); ok {
		if node != nil {
			result = append(result, node)
		}
	}
	return result
}

// fieldLevelCheckConstraintNode builds the ADD CONSTRAINT node for a synthesized
// field-level check= constraint. Mirrors the PostgreSQL planner. New columns are
// handled by the inline CHECK in ALTER TABLE ADD COLUMN and the comparator
// deliberately skips synthesizing those, so only existing-column field-level
// CHECKs reach here.
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
			name = tableName + "_" + f.Name + "_check"
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
		name := foreignKeyName(tableName, f)
		if name != constraintName {
			continue
		}
		fkRef := fromschema.ParseForeignKeyReference(f.Foreign)
		if fkRef == nil {
			continue
		}
		fkRef.OnDelete = f.OnDelete
		fkRef.OnUpdate = f.OnUpdate
		return p.createForeignKeyAlterStatement(tableName, name, []string{f.Name}, fkRef), true
	}
	return nil, false
}

// removeConstraints removes table-level constraints via ALTER TABLE statements.
//
// This method generates ALTER TABLE DROP statements for constraints that exist
// in the database but not in the generated schema.
//
// # MySQL Constraint Removal
//
// MySQL/MariaDB use a type-specific drop syntax:
//   - DROP FOREIGN KEY <name> for foreign key constraints (DROP CONSTRAINT is
//     not accepted for FKs on MySQL/MariaDB)
//   - DROP CONSTRAINT <name> for CHECK constraints (MySQL 8.0.16+ / MariaDB)
//   - DROP INDEX <name> for UNIQUE constraints
//
// The owning table is carried on diff.ConstraintsRemovedWithTables (the bare
// ConstraintsRemoved name list does not retain it). Constraints that appear in
// BOTH ConstraintsAdded and ConstraintsRemoved are modifications (the
// comparator expresses a changed constraint as remove + add of the same name —
// e.g. an on_delete change on a field-level FK, issue #189). Those are emitted
// as DROP-then-ADD by addNewConstraints, which runs earlier in the pipeline so
// the drop precedes the re-add; dropping them again here would remove the
// freshly added constraint, so they are skipped.
func (p *Planner) removeConstraints(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	addedNames := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedNames[name] = struct{}{}
	}

	// Constraints owned by a table that is itself being dropped do not need an
	// explicit drop — the DROP TABLE cascades them. Emitting one is at best
	// redundant and at worst invalid (MySQL/MariaDB reject DROP CONSTRAINT for a
	// PRIMARY KEY; PRIMARY KEY uses DROP PRIMARY KEY), so skip them.
	droppedTables := make(map[string]struct{}, len(diff.TablesRemoved))
	for _, t := range diff.TablesRemoved {
		droppedTables[t] = struct{}{}
	}

	for _, info := range diff.ConstraintsRemovedWithTables {
		if _, modified := addedNames[info.Name]; modified {
			continue
		}
		if _, dropped := droppedTables[info.TableName]; dropped {
			continue
		}
		// PRIMARY KEY uses a dedicated syntax and is owned by the column/table
		// lifecycle; ptah never emits a standalone PK drop here.
		if strings.EqualFold(info.Type, "PRIMARY KEY") {
			continue
		}
		result = append(result, dropConstraintNode(info))
	}
	return result
}

// dropConstraintNode builds the ALTER TABLE drop statement for a single removed
// constraint, choosing the MySQL/MariaDB type-specific syntax. FOREIGN KEY uses
// DROP FOREIGN KEY; everything else falls back to DROP CONSTRAINT (MySQL
// 8.0.19+ / MariaDB) which covers CHECK and named constraints.
func dropConstraintNode(info types.ConstraintRemovalInfo) ast.Node {
	op := &ast.DropConstraintOperation{
		ConstraintName: info.Name,
		ForeignKey:     strings.EqualFold(info.Type, "FOREIGN KEY"),
	}
	return &ast.AlterTableNode{
		Name:       info.TableName,
		Operations: []ast.AlterOperation{op},
	}
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

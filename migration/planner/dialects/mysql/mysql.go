package mysql

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
// The Planner carries only an immutable capability set and is safe for
// concurrent use across multiple goroutines. Each call to
// GenerateMigrationSQL operates independently without shared state.
type Planner struct {
	// caps describes what the concrete target accepts (issue #225/#226). The
	// MySQL planner serves both MySQL and MariaDB (GetPlanner maps both here);
	// the capability set is what tells them apart — e.g. MariaDB accepts the
	// IF EXISTS guard on constraint drops, MySQL does not. The nil zero value
	// defaults to the current MySQL line preset (capability.MySQL80) via the
	// capabilities accessor, so a bare &Planner{} — the construction shown in
	// this type's own example — behaves exactly like New(). Pass an explicit
	// preset (e.g. capability.MySQLLegacy()) to restrict emissions.
	caps capability.Capabilities
}

// New returns a planner configured with the current MySQL line preset
// (capability.MySQL80: 8.0.19+ and 9.x).
func New() *Planner {
	return NewWithCapabilities(capability.MySQL80())
}

// NewWithCapabilities returns a planner for a specific capability set — e.g.
// capability.MariaDB1011() for MariaDB targets, or a preset composed with
// Capabilities.With for a concrete server version (capability.ForServerVersion).
// The set is expected to be valid (capability.Capabilities.Validate); presets
// from the capability package always are. The set is cloned, so later
// mutations by the caller cannot affect the planner. A nil set defaults to
// the capability.MySQL80 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
}

// capabilities returns the planner's capability set, defaulting the nil zero
// value to the current MySQL line preset. nil deliberately does NOT mean
// "assume nothing": an assume-nothing set would silently downgrade CHECK
// additions to warnings and re-spell CHECK drops as DROP CHECK — destructive
// surprises for a zero-value planner. Restriction must be an explicit choice.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps == nil {
		return capability.MySQL80()
	}
	return p.caps
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

		astNode := fromschema.FromTable(table, allFields, generated.Enums, DialectName)
		for _, column := range astNode.Columns {
			column.ForeignKey = nil
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

func (p *Planner) modifyExistingColumns(result []ast.Node, diff *types.SchemaDiff, tableDiff *types.TableDiff, generated *goschema.Database) []ast.Node {
	for _, colDiff := range tableDiff.ColumnsModified {
		suppressColumnPrimary := false
		if _, hasPrimaryKeyChange := colDiff.Changes["primary_key"]; hasPrimaryKeyChange &&
			primaryKeyColumnChangeOwnedByTableConstraint(diff, tableDiff.TableName, colDiff.ColumnName) {
			colDiff.Changes = maps.Clone(colDiff.Changes)
			delete(colDiff.Changes, "primary_key")
			suppressColumnPrimary = true
			if len(colDiff.Changes) == 0 {
				continue
			}
		}

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
		field := *targetField
		if suppressColumnPrimary {
			field.Primary = false
		}
		columnNode := fromschema.FromField(field, generated.Enums, "mysql")

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

func primaryKeyColumnChangeOwnedByTableConstraint(diff *types.SchemaDiff, tableName, columnName string) bool {
	for _, info := range diff.ConstraintsAddedWithTables {
		if strings.EqualFold(info.Type, "PRIMARY KEY") &&
			info.TableName == tableName &&
			slices.Contains(info.Columns, columnName) {
			return true
		}
	}
	for _, info := range diff.ConstraintsRemovedWithTables {
		if strings.EqualFold(info.Type, "PRIMARY KEY") && info.TableName == tableName {
			return true
		}
	}
	return false
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
		result = p.modifyExistingColumns(result, diff, &tableDiff, generated)

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
	// The IF EXISTS guard on DROP INDEX is capability-gated INTENT (issue
	// #226): MariaDB accepts it, MySQL has no such form. The renderer
	// additionally validates the flag against its own target set, so the
	// guard is emitted only when both layers agree. Gating here (rather than
	// always setting the flag) keeps the capability composable — disabling
	// capability.DropIndexIfExists on a planner actually changes the plan.
	guarded := p.capabilities().Has(capability.DropIndexIfExists)

	// Use the detailed removal info if available (includes table names for MySQL/MariaDB)
	if len(diff.IndexesRemovedWithTables) > 0 {
		for _, indexInfo := range diff.IndexesRemovedWithTables {
			dropIndexNode := ast.NewDropIndex(indexInfo.Name).
				SetTable(indexInfo.TableName)
			if guarded {
				dropIndexNode.SetIfExists()
			}
			result = append(result, dropIndexNode)
		}
	} else {
		// Fallback to the basic removal list (for backward compatibility)
		for _, indexName := range diff.IndexesRemoved {
			dropIndexNode := ast.NewDropIndex(indexName)
			if guarded {
				dropIndexNode.SetIfExists()
			}
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
	nodes, _ := p.GenerateMigrationASTChecked(diff, generated)
	return nodes
}

func (p *Planner) GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
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

	// 4.5. Add and modify views/triggers after tables exist.
	result = p.addNewViews(result, diff, generated)
	result = p.modifyExistingViews(result, diff, generated)
	if err := p.rejectMaterializedViews(diff); err != nil {
		return nil, err
	}
	result = p.addNewTriggers(result, diff, generated)
	result = p.modifyExistingTriggers(result, diff, generated)

	// 5. Add new indexes
	result = p.addNewIndexes(result, diff, generated)

	// 5.5. Add new constraints (must be done after tables and columns exist)
	result = p.addNewConstraints(result, diff, generated)

	// 6. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 6.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff)

	// 6.6. Remove triggers and view-like objects before dependent tables.
	result = p.removeTriggers(result, diff)
	result = p.removeViews(result, diff)

	// 7. Remove tables (dangerous!)
	result = p.removeTables(result, diff)

	// 8. Handle enum removals (MySQL-specific warnings)
	result = p.handleEnumRemovals(result, diff)

	return result, nil
}

func (p *Planner) rejectMaterializedViews(diff *types.SchemaDiff) error {
	if len(diff.MaterializedViewsAdded) == 0 &&
		len(diff.MaterializedViewsModified) == 0 &&
		len(diff.MaterializedViewsRemoved) == 0 {
		return nil
	}
	return fmt.Errorf("materialized views are not supported by MySQL or MariaDB; remove matview definitions for this target")
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
		result = append(result, ast.NewDropView(viewName).SetIfExists())
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
		result = append(result, ast.NewDropTrigger(triggerRef.TriggerName, triggerRef.TableName).SetIfExists())
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

func findTrigger(triggers []goschema.Trigger, tableName, triggerName string) *goschema.Trigger {
	for i := range triggers {
		if triggers[i].Table == tableName && triggers[i].Name == triggerName {
			return &triggers[i]
		}
	}
	return nil
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
// The DROP is emitted here, immediately before the re-ADD, scoped to the exact
// host table(s) being re-added (issue #207); removeConstraints (which runs
// later) skips those (table, name) pairs and owns every remaining pure
// removal. MySQL accepts no IF EXISTS on constraint drops, so this
// exactly-once split between the two functions is what keeps a migration from
// aborting on a duplicate drop or colliding on a missing one.
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

	// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved
	// is a modification (the comparator expresses a changed constraint as
	// remove + add of the same name — e.g. an on_delete change on a field-level
	// FK, issue #189). Its old definition must be dropped before the re-add or
	// the ADD CONSTRAINT collides with the still-present constraint of the same
	// name (errno 1826 for FKs / 3822 for CHECKs).
	removedNames := make(map[string]struct{}, len(diff.ConstraintsRemoved))
	for _, name := range diff.ConstraintsRemoved {
		removedNames[name] = struct{}{}
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

	// Removal info grouped by bare name, so the legacy ConstraintsAdded loop
	// below can scope a modified non-FK constraint's DROP to its concrete host
	// table(s). The comparator records every removal in
	// ConstraintsRemovedWithTables in lockstep with the bare ConstraintsRemoved
	// list, so a modified constraint's host is normally known here even though
	// the bare loop iterates names alone.
	removalsByName := make(map[string][]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalsByName[info.Name] = append(removalsByName[info.Name], info)
	}

	// Hosts actually being re-ADDED under each name. A modified constraint's
	// pre-drop must hit only those hosts — NOT every host that merely has a
	// removal entry for the name. In the MIXED case (issue #207; postgres
	// sibling #206) a shared name is a modify on host A (re-added) and a PURE
	// removal on host B (not re-added): B's drop is owned by removeConstraints,
	// and MySQL has no IF EXISTS on constraint drops to absorb a duplicate, so
	// dropping B here as well would abort the migration on the second drop.
	addedHostsByName := make(map[string]map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// An addition entry with no recorded host is hostless: a "" host
			// would match no removal entry, so keeping it here would make
			// emitModifyDropForName filter out every REAL removal host and
			// skip a required pre-drop. Treat the name as if it had no
			// recorded addition hosts at all.
			continue
		}
		hosts := addedHostsByName[add.Name]
		if hosts == nil {
			hosts = make(map[string]struct{})
			addedHostsByName[add.Name] = hosts
		}
		hosts[add.TableName] = struct{}{}
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
		if add.Type != "PRIMARY KEY" || add.TableName == "" || len(add.Columns) == 0 {
			continue
		}
		if _, modified := removalByTableName[add.TableName+"."+add.Name]; modified {
			continue
		}
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: ast.NewPrimaryKeyConstraint(add.Columns...)}},
		})
		handled[add.Name] = struct{}{}
	}
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// For a modification, emit the DROP FOREIGN KEY from this exact host
		// table before its re-add — only when this host's (table, name) is in
		// the removal set; a pure-add host gets no phantom drop.
		if info, modified := removalByTableName[add.TableName+"."+add.Name]; modified {
			result = p.appendScopedDrop(result, info, droppedForModify)
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		handled[add.Name] = struct{}{}
	}

	// Fallback for added constraints with no table-qualified FK entry above
	// (table-level CHECK/UNIQUE, or field-level synthesis resolved by name).
	for _, constraintName := range diff.ConstraintsAdded {
		if _, done := handled[constraintName]; done {
			continue
		}

		// For a modification, emit the DROP(s) first so they precede the
		// re-add, scoped to the constraint's concrete host table(s) — never a
		// name-keyed single-winner lookup, which collapses multiple removal
		// hosts onto one arbitrary table (issue #207).
		if _, modified := removedNames[constraintName]; modified {
			result = p.emitModifyDropForName(result, constraintName, removalsByName, addedHostsByName[constraintName], droppedForModify)
		}

		result = p.appendAddConstraint(result, constraintName, generated, structToTable)
	}
	return result
}

// emitModifyDropForName appends the DROP(s) that must precede the re-ADD of a
// modified constraint reached via the bare ConstraintsAdded name list (the
// non-FK and field-level synthesis paths; FK modifies are handled per-host in
// the ConstraintsAddedWithTables loop). The comparator records every removal
// in ConstraintsRemovedWithTables in lockstep with the bare list, so the
// owning table and constraint type are normally known: each re-added host gets
// a direct, table-qualified, type-aware drop (DROP FOREIGN KEY /
// DROP CONSTRAINT), deduped per (host, name). A name-keyed single-winner
// lookup must never be used here: with >=2 removal hosts it collapses onto one
// arbitrary host, so the wrong table's constraint is dropped while the
// re-added host's ADD collides with its still-present old constraint
// (errno 1826/3822, issue #207).
//
// The drop is restricted to addedHosts — the hosts actually being re-added
// under this name (ConstraintsAddedWithTables). In the MIXED case a shared
// name is a modify on host A (re-added) and a PURE removal on host B (not
// re-added); B's drop is owned by removeConstraints. MySQL accepts no
// IF EXISTS on constraint drops (only MariaDB does), so — unlike postgres,
// where a duplicate guarded drop degrades to a no-op — dropping B here as well
// would abort the migration on removeConstraints' second drop.
//
// When addedHosts is empty the re-added hosts are unknown — e.g. a
// reverse/down diff fills ConstraintsRemovedWithTables but not
// ConstraintsAddedWithTables (reverseConstraintAdditions restores only
// FOREIGN KEYs, and nothing at all when the introspected schema is absent). In
// that case every recorded removal host is dropped here and removeConstraints
// skips the name entirely (its hostless-re-add rule), so each host is still
// dropped exactly once. A name with no recorded removal host at all emits no
// drop: MySQL has no anonymous-block equivalent of the postgres
// information_schema DO fallback to resolve the owner at runtime, so the
// re-add proceeds alone (pre-existing behavior for hand-built diffs).
func (p *Planner) emitModifyDropForName(
	result []ast.Node,
	name string,
	removalsByName map[string][]types.ConstraintRemovalInfo,
	addedHosts map[string]struct{},
	droppedForModify map[string]struct{},
) []ast.Node {
	for _, info := range removalsByName[name] {
		if info.TableName == "" {
			continue
		}
		if len(addedHosts) > 0 {
			if _, reAdded := addedHosts[info.TableName]; !reAdded {
				continue
			}
		}
		result = p.appendScopedDrop(result, info, droppedForModify)
	}
	return result
}

// appendScopedDrop appends a single table-qualified, type-aware constraint
// drop (ALTER TABLE <host> DROP FOREIGN KEY <name> / DROP CONSTRAINT <name>),
// deduped per (table, name) via dropped so a constraint name shared across
// host tables is dropped once per host and never twice for the same host.
// MySQL accepts no IF EXISTS on these drops, so exactly-once emission is what
// keeps a duplicate drop from aborting the migration.
func (p *Planner) appendScopedDrop(result []ast.Node, info types.ConstraintRemovalInfo, dropped map[string]struct{}) []ast.Node {
	dedupKey := info.TableName + "." + info.Name
	if _, done := dropped[dedupKey]; done {
		return result
	}
	dropped[dedupKey] = struct{}{}
	return append(result, p.dropConstraintNode(info))
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
		Columns:  add.ForeignColumns,
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
		// A CHECK constraint on a target that parses but does not enforce
		// CHECK (capability.CheckConstraintsEnforced absent — MySQL before
		// 8.0.16) would be a silent no-op in the live schema while ptah
		// believes it applied; surface that loudly instead of emitting it
		// (issue #226).
		if constraint.Type == "CHECK" && !p.capabilities().Has(capability.CheckConstraintsEnforced) {
			return append(result, ast.NewComment(fmt.Sprintf("WARNING: CHECK constraint %s skipped - the target parses but does not enforce CHECK constraints (MySQL < 8.0.16)", constraint.Name)))
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
		// Same enforcement gate as the table-level path in
		// appendAddConstraint: never emit a CHECK the target would silently
		// ignore (issue #226).
		if !p.capabilities().Has(capability.CheckConstraintsEnforced) {
			return ast.NewComment(fmt.Sprintf("WARNING: CHECK constraint %s skipped - the target parses but does not enforce CHECK constraints (MySQL < 8.0.16)", name)), true
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
//   - DROP CONSTRAINT <name> for CHECK constraints (MySQL 8.0.19+ / MariaDB)
//   - DROP INDEX <name> for UNIQUE constraints
//   - DROP PRIMARY KEY for PRIMARY KEY constraints
//
// The owning table is carried on diff.ConstraintsRemovedWithTables (the bare
// ConstraintsRemoved name list does not retain it, and MySQL has no runtime
// name-only fallback like the postgres information_schema DO block).
//
// # Modification skip — keyed on (table, name), not the bare name
//
// A constraint whose (table, name) appears in BOTH the additions
// (ConstraintsAddedWithTables) and the removals is a modification: the
// comparator expresses a changed constraint as remove + add of the same name
// (e.g. an on_delete change on a field-level FK, issue #189). Those hosts are
// emitted as DROP-then-ADD by addNewConstraints, which runs earlier in the
// pipeline so the drop precedes the re-add; dropping them again here would
// remove the freshly added constraint.
//
// The skip MUST be keyed on (table, name): a shared constraint name can be a
// modify on host A and a PURE removal on host B. A bare-name skip treats B's
// removal as a modify owned by addNewConstraints and skips it, leaving B's
// stale constraint in place forever (issue #207; postgres sibling #206).
//
// A name that is re-added with NO recorded host (ConstraintsAdded carries the
// name but ConstraintsAddedWithTables has no entry — reverse/down and
// hand-built diffs) is skipped entirely: addNewConstraints already dropped
// every recorded removal host for it (see emitModifyDropForName), and MySQL
// accepts no DROP ... IF EXISTS to absorb a duplicate drop, so a second drop
// here would abort the migration. Exactly-once emission per (table, name) —
// split between the two functions and deduped inside each — is what stands in
// for postgres's IF EXISTS idempotency guard.
func (p *Planner) removeConstraints(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// (table, name) pairs being re-added — modifications owned by
	// addNewConstraints — plus, per name, how many hosts were recorded at all.
	modifyHosts := make(map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	addedHostCounts := make(map[string]int, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// Hostless addition entries do not count as recorded hosts —
			// mirroring addedHostsByName in addNewConstraints — so the
			// hostless-re-add rule below still engages and this side keeps
			// skipping the hosts the add side already dropped.
			continue
		}
		modifyHosts[add.TableName+"."+add.Name] = struct{}{}
		addedHostCounts[add.Name]++
	}
	addedBareNames := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedBareNames[name] = struct{}{}
	}

	// Constraints owned by a table that is itself being dropped do not need an
	// explicit drop — the DROP TABLE cascades them. Emitting one is at best
	// redundant and at worst invalid, so skip them.
	droppedTables := make(map[string]struct{}, len(diff.TablesRemoved))
	for _, t := range diff.TablesRemoved {
		droppedTables[t] = struct{}{}
	}

	dropped := make(map[string]struct{})
	for _, info := range diff.ConstraintsRemovedWithTables {
		if info.TableName == "" {
			// No host recorded: there is no valid table-qualified ALTER TABLE
			// to emit and no runtime fallback on MySQL. Real comparator output
			// always carries the host.
			continue
		}
		if _, modified := modifyHosts[info.TableName+"."+info.Name]; modified {
			// addNewConstraints owns this host's DROP-then-ADD; do not re-drop.
			continue
		}
		if _, added := addedBareNames[info.Name]; added && addedHostCounts[info.Name] == 0 {
			// Hostless re-add: addNewConstraints already dropped every
			// recorded removal host for this name.
			continue
		}
		if _, droppedTable := droppedTables[info.TableName]; droppedTable {
			continue
		}
		result = p.appendScopedDrop(result, info, dropped)
	}
	return result
}

// dropConstraintNode builds the ALTER TABLE drop statement for a single removed
// constraint, choosing the MySQL/MariaDB type-specific syntax and recording the
// planner's capability-derived intent (issue #226):
//
//   - FOREIGN KEY uses DROP FOREIGN KEY (never the generic clause);
//   - UNIQUE uses DROP INDEX on EVERY target (issue #195): a UNIQUE
//     constraint is backed by an index, and ALTER TABLE ... DROP INDEX is the
//     one spelling valid across the entire MySQL/MariaDB family (verified
//     live on MySQL 9.7 and MariaDB 10.11) — the generic clause would be
//     invalid SQL before MySQL 8.0.19;
//   - CHECK uses DROP CHECK when the target lacks the generic DROP CONSTRAINT
//     clause (capability.DropConstraintGeneric absent — MySQL 8.0.16–8.0.18);
//     a target with NEITHER spelling (capability.MySQLLegacy) gets a loud
//     WARNING comment instead of invalid SQL;
//   - everything else uses DROP CONSTRAINT (MySQL 8.0.19+ / MariaDB);
//   - the IF EXISTS guard is requested when the target accepts guarded drops
//     (capability.DropConstraintIfExists — MariaDB; MySQL rejects it). The
//     renderer validates the flag against its own capability set too, so a
//     stray intent flag can never reach a MySQL server. The exactly-once drop
//     discipline from issue #207 therefore stays load-bearing on MySQL, where
//     no guard exists; on MariaDB the guard is belt-and-braces on top of it.
func (p *Planner) dropConstraintNode(info types.ConstraintRemovalInfo) ast.Node {
	caps := p.capabilities()
	op := &ast.DropConstraintOperation{
		ConstraintName: info.Name,
		ForeignKey:     strings.EqualFold(info.Type, "FOREIGN KEY"),
		IfExists:       caps.Has(capability.DropConstraintIfExists),
	}
	switch {
	case op.ForeignKey:
		// DROP FOREIGN KEY carries the type information already.
	case strings.EqualFold(info.Type, "UNIQUE"):
		op.Unique = true
		// The UNIQUE spelling is an index drop, so its guard intent follows
		// the index-drop guard capability rather than the constraint-drop
		// one. Identical on the shipped presets (MariaDB has both, MySQL
		// neither), but a composed set may enable them independently and the
		// intent must match the chosen spelling.
		op.IfExists = caps.Has(capability.DropIndexIfExists)
	case strings.EqualFold(info.Type, "PRIMARY KEY"):
		op.PrimaryKey = true
	case strings.EqualFold(info.Type, "CHECK") && !caps.Has(capability.DropConstraintGeneric):
		if !caps.Has(capability.DropCheckClause) {
			// No generic clause and no DROP CHECK either (MySQLLegacy):
			// there is no valid spelling, so fail loudly instead of
			// emitting SQL the server rejects.
			return ast.NewComment(fmt.Sprintf("WARNING: cannot drop CHECK constraint %s on %s - the target supports neither DROP CONSTRAINT nor DROP CHECK", info.Name, info.TableName))
		}
		op.Check = true
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
		if len(constraint.Columns) == 0 || constraint.ForeignTable == "" || len(constraint.ForeignColumnsOrDefault()) == 0 {
			return nil // Invalid FOREIGN KEY constraint
		}
		ref := &ast.ForeignKeyRef{
			Table:    constraint.ForeignTable,
			Column:   constraint.ForeignColumn,
			Columns:  constraint.ForeignColumns,
			OnDelete: constraint.OnDelete,
			OnUpdate: constraint.OnUpdate,
			Name:     constraint.Name,
		}
		return ast.NewForeignKeyConstraint(constraint.Name, constraint.Columns, ref)

	default:
		return nil // Unsupported constraint type
	}
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

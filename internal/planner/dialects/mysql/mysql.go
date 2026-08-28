package mysql

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/constraintscope"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/internal/planner/tablelookup"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
//	generated := &schemamodel.Database{
//		Tables: []schemamodel.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []schemamodel.Field{
//			{Name: "id", Type: "AUTO_INCREMENT", StructName: "User", Primary: true},
//		},
//	}
//
//	// Generate migration AST nodes
//	nodes, err := planner.GenerateMigrationAST(diff, generated)
//	if err != nil {
//		return err
//	}
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
	// defaults to the current MySQL line preset (capability.MySQL84) via the
	// capabilities accessor, so a bare &Planner{} — the construction shown in
	// this type's own example — behaves exactly like New(). Pass an explicit
	// preset (e.g. capability.MySQLLegacy()) to restrict emissions.
	caps capability.Capabilities
	// dialect is the target conversion platform passed to fromschema. It
	// defaults to mysql so the zero value and New stay backwards-identical for
	// the MySQL-family planner.
	dialect string
}

// New returns a planner configured with the current MySQL line preset
// (capability.MySQL84: 8.4+ and 9.x).
func New() *Planner {
	return NewWithCapabilities(capability.MySQL84())
}

// NewWithCapabilities returns a planner for a specific capability set — e.g.
// capability.MariaDB1011() for MariaDB targets, or a preset composed with
// Capabilities.With for a concrete server version (capability.ForServerVersion).
// The set is expected to be valid (capability.Capabilities.Validate); presets
// from the capability package always are. The set is cloned, so later
// mutations by the caller cannot affect the planner. A nil set defaults to
// the capability.MySQL84 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return NewForDialect(DialectName, caps)
}

// NewForDialect returns a planner that reuses MySQL-family ordering and
// constraint ownership while converting fields for another close-enough target
// dialect. SQL Server uses this to share the generic AST planning path while
// relying on its own renderer for T-SQL syntax.
func NewForDialect(dialect string, caps capability.Capabilities) *Planner {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == "" {
		normalized = DialectName
	}
	return &Planner{caps: caps.Clone(), dialect: normalized}
}

// capabilities returns the planner's capability set, defaulting the nil zero
// value to the current MySQL line preset. nil deliberately does NOT mean
// "assume nothing": an assume-nothing set would silently downgrade CHECK
// additions to warnings and re-spell CHECK drops as DROP CHECK — destructive
// surprises for a zero-value planner. Restriction must be an explicit choice.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps == nil {
		return capability.ForDialect(p.targetDialect())
	}
	return p.caps
}

func (p *Planner) targetDialect() string {
	if p.dialect == "" {
		return DialectName
	}
	return p.dialect
}

func (p *Planner) addEnumChangeWarnings(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if len(diff.EnumsAdded) > 0 {
		astCommentNode := ast.NewComment(fmt.Sprintf("NOTE: %s enums are handled in column definitions. New enums: %v", p.enumDialectLabel(), diff.EnumsAdded))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) handleEnumModifications(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, enumDiff := range diff.EnumsModified {
		if len(enumDiff.ValuesAdded) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: %s enum modifications require updating each column using enum %s. Values added: %v", p.enumDialectLabel(), enumDiff.EnumName, enumDiff.ValuesAdded))
			result = append(result, astCommentNode)
		}
		if len(enumDiff.ValuesRemoved) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: %s cannot remove enum values from %s without recreating or validating affected columns. Values removed: %v", p.enumDialectLabel(), enumDiff.EnumName, enumDiff.ValuesRemoved))
			result = append(result, astCommentNode)
		}
	}
	return result
}

// enumDialectLabel names the engine a refusal is about.
//
// This planner is shared by more than the MySQL family: SQL Server and Oracle
// are aliased onto it, and a refusal that names the wrong engine sends an
// operator to the wrong documentation. Every dialect aliased here needs an arm,
// because the default spells a family it may not belong to.
func (p *Planner) enumDialectLabel() string {
	switch p.targetDialect() {
	case platform.SQLServer:
		return "SQL Server"
	case platform.Oracle:
		return "Oracle"
	default:
		return "MySQL-family"
	}
}

func (p *Planner) addNewTables(result []ast.Node, diff *difftypes.SchemaDiff, desired *schemamodel.Database) []ast.Node {
	orderedTables := deporder.TablesForCreate(desired, diff.TablesAdded.Names())

	// Phase 1: Create tables without foreign key constraints
	result = p.createTablesWithoutForeignKeys(result, desired, orderedTables)

	return result
}

func (p *Planner) addForeignKeyConstraintsForNewTables(result []ast.Node, diff *difftypes.SchemaDiff, desired *schemamodel.Database) []ast.Node {
	return p.addForeignKeyConstraints(result, desired, deporder.TablesForCreate(desired, diff.TablesAdded.Names()))
}

// createTablesWithoutForeignKeys creates all tables without foreign key constraints
func (p *Planner) createTablesWithoutForeignKeys(result []ast.Node, desired *schemamodel.Database, tables []schemamodel.Table) []ast.Node {
	allFields := desired.Fields

	for _, table := range tables {
		astNode := fromschema.FromTable(table, allFields, desired.Enums, p.targetDialect())
		for _, column := range astNode.Columns {
			column.ForeignKey = nil
		}
		result = append(result, astNode)
	}

	return result
}

// addForeignKeyConstraints adds foreign key constraints via ALTER TABLE statements
func (p *Planner) addForeignKeyConstraints(result []ast.Node, desired *schemamodel.Database, tables []schemamodel.Table) []ast.Node {
	for _, table := range tables {
		result = p.addRegularForeignKeys(result, desired, table)
		result = p.addSelfReferencingForeignKeys(result, desired, table)
	}

	return result
}

// addRegularForeignKeys adds regular (non-self-referencing) foreign key constraints
func (p *Planner) addRegularForeignKeys(result []ast.Node, desired *schemamodel.Database, table schemamodel.Table) []ast.Node {
	for _, field := range desired.Fields {
		if !isRegularForeignKeyField(field, table) {
			continue
		}

		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef == nil {
			continue
		}
		fkRef.Table = tablelookup.ResolveReference(desired.Tables, table, fkRef.Table)
		if fkRef.Table == table.QualifiedName() {
			continue
		}
		fkRef.OnDelete = field.OnDelete
		fkRef.OnUpdate = field.OnUpdate
		result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), foreignKeyName(table.Name, field), []string{field.Name}, fkRef))
	}
	return result
}

// addSelfReferencingForeignKeys adds self-referencing foreign key constraints
func (p *Planner) addSelfReferencingForeignKeys(result []ast.Node, desired *schemamodel.Database, table schemamodel.Table) []ast.Node {
	selfRefFKs, exists := desired.SelfReferencingForeignKeys[table.QualifiedName()]
	if !exists {
		return result
	}

	for _, selfRefFK := range selfRefFKs {
		fkRef := fromschema.ParseForeignKeyReference(selfRefFK.Foreign)
		if fkRef != nil {
			fkRef.Table = tablelookup.ResolveReference(desired.Tables, table, fkRef.Table)
			fkRef.OnDelete = selfRefFK.OnDelete
			fkRef.OnUpdate = selfRefFK.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), selfReferencingForeignKeyName(table.Name, selfRefFK), []string{selfRefFK.FieldName}, fkRef))
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
func isRegularForeignKeyField(field schemamodel.Field, table schemamodel.Table) bool {
	return field.StructName == table.StructName && field.Foreign != ""
}

// foreignKeyName returns the constraint name to use for a field-level foreign
// key: the explicit foreign_key_name= when set, otherwise the conventional
// fk_<table>_<column> name shared with the schemadiff comparator and the down
// path via fromschema.GenerateForeignKeyName.
func foreignKeyName(tableName string, field schemamodel.Field) string {
	if field.ForeignKeyName != "" {
		return field.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, field.Name)
}

// selfReferencingForeignKeyName returns the constraint name for a
// self-referencing field-level foreign key, deriving the conventional
// fk_<table>_<field> name when foreign_key_name= was omitted.
func selfReferencingForeignKeyName(tableName string, fk schemamodel.SelfReferencingFK) string {
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

func (p *Planner) addNewTableColumns(
	result []ast.Node,
	tableDiff *difftypes.TableDiff,
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) []ast.Node {
	// The TABLE still has to be declared. The column travels with the change
	// now, so the field lookup that used to fail here cannot -- but the guard
	// it provided is load-bearing on its own: a diff naming `app.users` against
	// a schema that declares `reporting.users` must write no DDL, because the
	// statement would apply cleanly to a relation nobody declared.
	if findGeneratedTable(desired.Tables, tableDiff.TableName, semantics) == nil {
		return result
	}

	// The column itself is no longer looked up: it used to be found by the
	// table's Go STRUCT name and a scan of every field in the schema
	// (stokaro/ptah#2315).
	for _, column := range tableDiff.ColumnsAdded {
		targetField := &column

		{
			columnNode := fromschema.FromField(*targetField, desired.Enums, p.targetDialect())

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

func (p *Planner) modifyExistingColumns(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	tableDiff *difftypes.TableDiff,
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) ([]ast.Node, error) {
	commentRidesAlong := p.columnCommentRidesWithTheColumn()
	for _, colDiff := range tableDiff.ColumnsModified {
		// A column comment travels with the column on MySQL and MariaDB,
		// because MODIFY COLUMN restates the whole definition and that
		// definition carries a COMMENT clause. Oracle has no such clause, so
		// there the comment is a statement of its own -- and a column whose
		// ONLY difference is its comment gets no MODIFY at all, which matters
		// more there than anywhere: this planner's own note records that
		// Oracle's nullability clause is not idempotent, so a MODIFY that
		// changes nothing is a statement waiting to fail (stokaro/ptah#2168).
		if !commentRidesAlong {
			result = appendColumnComment(result, tableDiff.TableName, colDiff)
			if len(colDiff.Changes) == 0 {
				continue
			}
		}
		suppressColumnPrimary := false
		if _, hasPrimaryKeyChange := colDiff.Changes["primary_key"]; hasPrimaryKeyChange &&
			primaryKeyColumnChangeOwnedByTableConstraint(diff, tableDiff.TableName, colDiff.ColumnName, semantics) {
			colDiff.Changes = maps.Clone(colDiff.Changes)
			delete(colDiff.Changes, "primary_key")
			suppressColumnPrimary = true
			if len(colDiff.Changes) == 0 {
				continue
			}
		}
		if err := p.validateColumnModification(tableDiff.TableName, colDiff); err != nil {
			return result, err
		}

		var targetField *schemamodel.Field
		var targetStructName string
		if targetTable := findGeneratedTable(desired.Tables, tableDiff.TableName, semantics); targetTable != nil {
			targetStructName = targetTable.StructName
			for _, field := range desired.Fields {
				if field.StructName == targetStructName && field.Name == colDiff.ColumnName {
					targetField = &field
					break
				}
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
		columnNode := fromschema.FromField(field, desired.Enums, p.targetDialect())

		// Generate ALTER COLUMN statements using AST
		alterNode := &ast.AlterTableNode{
			Name: tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
				Column:              columnNode,
				PreviousType:        previousColumnType(colDiff.Changes["type"]),
				PreviousNullable:    previousColumnNullable(colDiff.Changes["nullable"]),
				HasPreviousNullable: colDiff.Changes["nullable"] != "",
				PreviousDefault:     previousColumnDefault(colDiff.Changes),
				HasPreviousDefault:  columnDefaultChanged(colDiff.Changes),
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
	return result, nil
}

// findGeneratedTable resolves the declared table a TableDiff names.
//
// The database name and the Go struct name are two different namespaces, so they
// are tried in that order and only the first is an identifier: it goes through
// the target's identifier rules, which resolve an absent schema to the dialect's
// default (`dbo` on SQL Server) and fold case where the engine does. The struct
// name is not an identifier and is matched verbatim.
//
// The identifier half matters because the diff and the schema handed to the
// planner do not always spell the schema the same way -- the down direction
// plans against the pre-change database converted back to a goschema, which
// spells every name the way the catalog reported it. Table comparison keys
// through the same semantics, so an `==` here split what the comparator joined
// and the column DDL for that table was silently dropped from the plan.
func findGeneratedTable(
	tables []schemamodel.Table,
	tableName string,
	semantics identifier.Semantics,
) *schemamodel.Table {
	if table := objectlookup.Qualified(tables, tableName, semantics); table != nil {
		return table
	}
	for i := range tables {
		table := &tables[i]
		if table.StructName == tableName {
			return table
		}
	}
	return nil
}

func (p *Planner) validateColumnModification(tableName string, colDiff difftypes.ColumnDiff) error {
	if p.targetDialect() != platform.SQLServer {
		return nil
	}
	var unsupported []string
	for changeType := range colDiff.Changes {
		switch changeType {
		case "type", "nullable":
		default:
			unsupported = append(unsupported, changeType)
		}
	}
	if len(unsupported) == 0 {
		return nil
	}
	slices.Sort(unsupported)
	return &ptaherr.CapabilityError{
		Dialect: p.targetDialect(),
		Feature: "column modification",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"SQL Server planner only supports ALTER COLUMN for type/nullability changes on %s.%s; unsupported changes: %s",
			tableName,
			colDiff.ColumnName,
			strings.Join(unsupported, ", "),
		),
	}
}

// primaryKeyColumnChangeOwnedByTableConstraint reports whether a table-level
// PRIMARY KEY entry in the diff already owns the key this column change would
// otherwise spell inline.
//
// The two table names come from different sources and do not have to agree
// letter for letter: `ConstraintAdditionInfo.TableName` follows the constraint
// declaration, while `TableDiff.TableName` follows `genTable.QualifiedName()`.
// Comparing them with `==` answered "not owned" for a table declared `app.orders`
// whose constraint names it bare, and the planner then emitted BOTH
// `ALTER TABLE app.orders MODIFY COLUMN id INT PRIMARY KEY` and
// `ALTER TABLE orders ADD PRIMARY KEY (id)`. Measured on MySQL 9.7.1, the second
// fails with `ERROR 1068 (42000): Multiple primary key defined` -- the migration
// aborts halfway through. So the question is asked by identity, exactly as the
// column-definition lookup a few lines above already asks it.
func primaryKeyColumnChangeOwnedByTableConstraint(
	diff *difftypes.SchemaDiff,
	tableName, columnName string,
	semantics identifier.Semantics,
) bool {
	for _, info := range diff.ConstraintsAddedWithTables {
		if strings.EqualFold(info.Type, "PRIMARY KEY") &&
			objectlookup.Same(tableName, info.TableName, semantics) &&
			slices.Contains(info.Columns, columnName) {
			return true
		}
	}
	for _, info := range diff.ConstraintsRemovedWithTables {
		if strings.EqualFold(info.Type, "PRIMARY KEY") &&
			objectlookup.Same(tableName, info.TableName, semantics) {
			return true
		}
	}
	return false
}

func (p *Planner) removeColumns(result []ast.Node, tableDiff *difftypes.TableDiff) ([]ast.Node, error) {
	if p.targetDialect() == platform.SQLServer && len(tableDiff.ColumnsRemoved) > 0 {
		return result, &ptaherr.CapabilityError{
			Dialect: p.targetDialect(),
			Feature: "column removal",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf(
				"SQL Server planner does not support automatic DROP COLUMN for %s; write an explicit migration that drops dependent constraints and indexes first",
				tableDiff.TableName,
			),
		}
	}
	for _, column := range tableDiff.ColumnsRemoved {
		// Generate DROP COLUMN statement using AST
		alterNode := &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: column.Name}},
		}
		result = append(result, alterNode)
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: Dropping column %s.%s - This will delete data!", tableDiff.TableName, column.Name))
		result = append(result, astCommentNode)
	}
	return result, nil
}

// columnCommentRidesWithTheColumn reports whether this dialect's MODIFY COLUMN
// carries the comment, so no separate statement is needed.
//
// It is a question about the SQL these engines accept, not about Ptah: MySQL
// and MariaDB have an inline COMMENT clause in a column definition and Oracle
// does not, and the planners share this algorithm.
func (p *Planner) columnCommentRidesWithTheColumn() bool {
	switch p.targetDialect() {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// appendColumnComment emits a column's comment transition as a statement of its
// own, for the dialects that need one.
func appendColumnComment(result []ast.Node, table string, colDiff difftypes.ColumnDiff) []ast.Node {
	if colDiff.CommentChange == nil {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.SetCommentOperation{
			Column:     colDiff.ColumnName,
			Comment:    colDiff.CommentChange.Desired,
			HasCurrent: colDiff.CommentChange.Current != "",
		}},
	})
}

// appendTableComment emits the table's comment transition, if it has one.
func appendTableComment(result []ast.Node, tableDiff difftypes.TableDiff) []ast.Node {
	if tableDiff.CommentChange == nil {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: tableDiff.TableName,
		Operations: []ast.AlterOperation{&ast.SetCommentOperation{
			Comment:    tableDiff.CommentChange.Desired,
			HasCurrent: tableDiff.CommentChange.Current != "",
		}},
	})
}

func (p *Planner) modifyExistingTables(result []ast.Node, diff *difftypes.SchemaDiff, desired *schemamodel.Database) ([]ast.Node, error) {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, tableDiff := range diff.TablesModified {
		astCommentNode := ast.NewComment(fmt.Sprintf("Modify table: %s", tableDiff.TableName))
		result = append(result, astCommentNode)

		// The table's own comment is a table option here, so it needs a
		// statement of its own. A column's does not: MySQL restates the whole
		// column to change anything about it, and MODIFY COLUMN already
		// carries the comment -- which is why only the table half is emitted
		// here while PostgreSQL emits both (stokaro/ptah#2168).
		result = appendTableComment(result, tableDiff)

		// Add new columns
		result = p.addNewTableColumns(result, &tableDiff, desired, semantics)

		// Modify existing columns
		var err error
		result, err = p.modifyExistingColumns(result, diff, &tableDiff, desired, semantics)
		if err != nil {
			return result, err
		}

		// Remove columns (dangerous!)
		result, err = p.removeColumns(result, &tableDiff)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// affectedForeignKey identifies a foreign key that a MySQL/MariaDB column
// change forces the planner to drop before the column modifications and,
// depending on ownership, recreate afterward (issue #694).
//
// MySQL and MariaDB reject ALTER TABLE ... MODIFY on a column that participates
// in a foreign key — as either the referencing or the referenced column (MySQL
// errno 3780, MariaDB errno 1832). The recreation is driven from the schema the
// planner was handed — the target schema on the up path, the introspected
// pre-change schema on the down path — so referencing and referenced column
// types stay in lockstep and the recreated constraint cannot fail as
// incompatible. See planColumnTypeForeignKeyChanges for how the drop and re-add
// are split with the constraint machinery.
type affectedForeignKey struct {
	// table is the referencing (owning) table that carries the constraint and
	// is therefore where DROP/ADD FOREIGN KEY runs.
	table string
	// name is the constraint name.
	name string
	// columns are the local (referencing) columns.
	columns []string
	// ref describes the referenced table, columns, and referential actions.
	ref *ast.ForeignKeyRef
}

// constraintHostKey is the comparison value the DIFF carries for a constraint.
//
// It was this planner's own derivation, folding nothing on the argument that
// both spellings arrived already normalized. The criterion that comment named
// -- the diff carrying its identities rather than its spellings -- is met, so
// the derivation is gone and the carried value is the key
// (stokaro/ptah#1345, stokaro/ptah#1663).
type constraintHostKey = difftypes.ConstraintIdentity

// columnTypeForeignKeyPlan holds the foreign-key statements the planner emits
// around column changes on MySQL/MariaDB (issue #694).
//
//   - drops are the DROP FOREIGN KEY statements emitted BEFORE the column
//     modifications, for every affected foreign key that already exists.
//   - readds are the ADD CONSTRAINT ... FOREIGN KEY statements emitted AFTER the
//     modifications, only for keys the constraint machinery does not itself
//     recreate.
//   - dropped is the set of "table.name" keys already dropped here, so
//     addNewConstraints and removeConstraints suppress their own now-redundant
//     drop of the same key. MySQL accepts no IF EXISTS guard on constraint
//     drops, so a duplicate drop would abort the migration.
type columnTypeForeignKeyPlan struct {
	drops   []ast.Node
	readds  []ast.Node
	dropped map[constraintHostKey]struct{}
}

// planColumnTypeForeignKeyChanges determines which foreign keys a column change
// forces the planner to drop before the modifications and recreate afterward
// (issue #694), and how ownership of the drop and re-add is split with the
// constraint machinery.
//
// A foreign key is affected when one of its referencing or referenced columns is
// changing type, or when the forward plan removes a column from the key's host
// table. MySQL/MariaDB require the FK to be dropped before either operation. A
// nullability or default change keeps the referential type match and is left to
// a bare MODIFY.
//
// Ownership is resolved per (table, name) — never the bare name, which a foreign
// key shared across host tables would conflate (issue #197/#207):
//
//   - ADDED-ONLY (the (table, name) is in the diff's FK additions but not its
//     removals): a brand-new key on an existing column. It does not exist yet,
//     so it is NOT pre-dropped; addNewConstraints adds it after the
//     modifications.
//   - MODIFY (in both additions and removals) or REMOVED-ONLY (in removals): the
//     key exists, so it is pre-dropped here. addNewConstraints owns the re-add
//     (with the new definition) for a MODIFY; a REMOVED-ONLY key has no re-add.
//   - Neither (not in the FK constraint diff at all): a pure column-type change
//     on an unchanged key. This planner owns both the pre-drop and the
//     post-MODIFY re-add.
func (p *Planner) planColumnTypeForeignKeyChanges(diff *difftypes.SchemaDiff, desired *schemamodel.Database) columnTypeForeignKeyPlan {
	plan := columnTypeForeignKeyPlan{dropped: make(map[constraintHostKey]struct{})}
	if diff == nil || desired == nil {
		return plan
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	blockingChanges := foreignKeyBlockingColumnChangesByTable(diff, semantics)
	if len(blockingChanges) == 0 {
		return plan
	}

	addedHosts, removedHosts := foreignKeyConstraintDiffHosts(diff, semantics)
	drops, readds := collectColumnTypeForeignKeyActions(
		desired,
		diff,
		blockingChanges,
		addedHosts,
		removedHosts,
		semantics,
	)

	for _, fk := range drops {
		plan.drops = append(plan.drops, p.dropConstraintNode(difftypes.ConstraintRemovalInfo{
			Name:      fk.name,
			TableName: fk.table,
			Type:      "FOREIGN KEY",
		}))
		plan.dropped[constraintscope.Identity(semantics, fk.table, fk.name)] = struct{}{}
	}
	for _, fk := range readds {
		plan.readds = append(plan.readds, p.createForeignKeyAlterStatement(fk.table, fk.name, fk.columns, fk.ref))
	}
	return plan
}

// collectColumnTypeForeignKeyActions returns, deduped per (table, name) and
// sorted deterministically, the foreign keys to pre-drop and the subset this
// planner also re-adds. See planColumnTypeForeignKeyChanges for the ownership
// rules.
func collectColumnTypeForeignKeyActions(
	desired *schemamodel.Database,
	diff *difftypes.SchemaDiff,
	typeChanged map[string]map[string]struct{},
	addedHosts, removedHosts map[constraintHostKey]struct{},
	semantics identifier.Semantics,
) (drops, readds []affectedForeignKey) {
	seen := make(map[constraintHostKey]struct{})
	foreignKeyRemovalDetails := foreignKeyRemovalDetailsByHost(diff, semantics)

	// Existing foreign keys drawn from the schema handed to the planner: the
	// target schema on the up path, the introspected pre-change schema on the
	// down path. This covers unchanged and modified keys. Added-only keys are
	// not in the database yet, so they are not pre-dropped here.
	for _, fk := range candidateForeignKeys(desired) {
		if !foreignKeyValid(fk) || !foreignKeyTouchesTypeChange(fk, typeChanged, semantics) {
			continue
		}
		hostKey := constraintscope.Identity(semantics, fk.table, fk.name)
		if _, done := seen[hostKey]; done {
			continue
		}
		_, added := addedHosts[hostKey]
		_, removed := removedHosts[hostKey]
		if added && !removed {
			continue // added-only: nothing to pre-drop
		}
		seen[hostKey] = struct{}{}
		drops = append(drops, fk)
		if !added && !removed {
			// Not in the FK constraint diff at all: own the re-add too.
			readds = append(readds, fk)
		}
	}

	// Removed-only foreign keys are not in the schema handed to the planner, so
	// they are drawn from the removal list. They exist in the database and, when
	// their table has a column-type change, must be pre-dropped before the bare
	// MODIFY the server would otherwise reject; they have no re-add.
	for _, info := range diff.ConstraintsRemovedWithTables {
		if !strings.EqualFold(info.Type, "FOREIGN KEY") {
			continue
		}
		hostKey := info.Identity
		if _, added := addedHosts[hostKey]; added {
			continue // MODIFY: handled from the schema above
		}
		if _, done := seen[hostKey]; done {
			continue
		}
		details, detailed := foreignKeyRemovalDetails[info.Identity]
		if detailed && !foreignKeyRemovalTouchesBlockingChange(details, typeChanged, semantics) {
			continue
		}
		seen[hostKey] = struct{}{}
		drops = append(drops, affectedForeignKey{table: info.TableName, name: info.Name})
	}

	sortAffectedForeignKeys(drops)
	sortAffectedForeignKeys(readds)
	return drops, readds
}

// foreignKeyValid reports whether a candidate foreign key carries the fields
// needed to both drop and recreate it.
func foreignKeyValid(fk affectedForeignKey) bool {
	return fk.table != "" && fk.name != "" && fk.ref != nil && len(fk.columns) > 0
}

func sortAffectedForeignKeys(fks []affectedForeignKey) {
	slices.SortFunc(fks, func(a, b affectedForeignKey) int {
		if c := strings.Compare(a.table, b.table); c != 0 {
			return c
		}
		return strings.Compare(a.name, b.name)
	})
}

// foreignKeyConstraintDiffHosts returns the (table, name) hosts — keyed
// "table\x00name" — of the foreign keys the constraint machinery adds and
// removes for this diff. Keying on (table, name), never the bare name, is
// essential: a foreign-key name shared across tables (an embedded
// inline-relation mixin, issue #197/#207) can be a modification on one host and
// untouched on another, and only the modified host defers its re-add to the
// constraint machinery.
func foreignKeyConstraintDiffHosts(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) (added, removed map[constraintHostKey]struct{}) {
	added = make(map[constraintHostKey]struct{})
	removed = make(map[constraintHostKey]struct{})
	for _, info := range diff.ConstraintsAddedWithTables {
		if strings.EqualFold(info.Type, "FOREIGN KEY") {
			added[info.Identity] = struct{}{}
		}
	}
	for _, info := range diff.ConstraintsRemovedWithTables {
		if strings.EqualFold(info.Type, "FOREIGN KEY") {
			removed[info.Identity] = struct{}{}
		}
	}
	return added, removed
}

// candidateForeignKeys enumerates every foreign key in the schema, drawing on
// all three representations: field-level foreign= references (and the
// single-column FKs the down path reconstructs from the database),
// self-referencing foreign keys, and table-level foreign key constraints (and
// the composite FKs the down path reconstructs).
func candidateForeignKeys(desired *schemamodel.Database) []affectedForeignKey {
	structToTable := make(map[string]schemamodel.Table, len(desired.Tables))
	tableByQualifiedName := make(map[string]schemamodel.Table, len(desired.Tables))
	for _, t := range desired.Tables {
		structToTable[t.StructName] = t
		tableByQualifiedName[t.QualifiedName()] = t
	}

	var candidates []affectedForeignKey
	candidates = appendFieldLevelForeignKeys(candidates, desired, structToTable)
	candidates = appendSelfReferencingForeignKeys(candidates, desired, tableByQualifiedName)
	candidates = appendTableLevelForeignKeys(candidates, desired)
	return candidates
}

// appendFieldLevelForeignKeys resolves each field-level foreign key to its
// owning table. The ALTER TABLE target is the qualified table name — matching
// the MODIFY statement and the constraint comparator — while the conventional
// constraint name is derived from the bare table name, matching
// fromschema.GenerateForeignKeyName as used on the FK-creation path and by the
// comparator's synthesis.
func appendFieldLevelForeignKeys(candidates []affectedForeignKey, desired *schemamodel.Database, structToTable map[string]schemamodel.Table) []affectedForeignKey {
	for _, field := range desired.Fields {
		if field.Foreign == "" {
			continue
		}
		ref := fromschema.ParseForeignKeyReference(field.Foreign)
		if ref == nil {
			continue
		}
		ref.OnDelete = field.OnDelete
		ref.OnUpdate = field.OnUpdate
		table, ok := structToTable[field.StructName]
		qualified := field.StructName
		bare := field.StructName
		if ok {
			qualified = table.QualifiedName()
			bare = table.Name
		}
		candidates = append(candidates, affectedForeignKey{
			table:   qualified,
			name:    foreignKeyName(bare, field),
			columns: []string{field.Name},
			ref:     ref,
		})
	}
	return candidates
}

func appendSelfReferencingForeignKeys(
	candidates []affectedForeignKey,
	desired *schemamodel.Database,
	tableByQualifiedName map[string]schemamodel.Table,
) []affectedForeignKey {
	for tableName, fks := range desired.SelfReferencingForeignKeys {
		qualified := tableName
		bare := tableName
		if table, ok := tableByQualifiedName[tableName]; ok {
			qualified = table.QualifiedName()
			bare = table.Name
		} else if ref, ok := tableref.Parse(tableName); ok {
			bare = ref.Name
		}
		for _, fk := range fks {
			ref := fromschema.ParseForeignKeyReference(fk.Foreign)
			if ref == nil {
				continue
			}
			ref.OnDelete = fk.OnDelete
			ref.OnUpdate = fk.OnUpdate
			candidates = append(candidates, affectedForeignKey{
				table:   qualified,
				name:    selfReferencingForeignKeyName(bare, fk),
				columns: []string{fk.FieldName},
				ref:     ref,
			})
		}
	}
	return candidates
}

// declaredConstraintTable is the table a table-level constraint is on.
//
// A declaration names one only when it differs from the struct's own table --
// that is what [schemamodel.Constraint.Table] documents -- so the ordinary
// declaration leaves it empty and the table has to come from the struct. Read
// straight, the empty value reached the renderer and every kind came out as
//
//	ALTER TABLE "" ADD CONSTRAINT "ex1" EXCLUDE USING gist (room WITH =)
//
// which no server takes (stokaro/ptah#2008). The struct's own name is the last
// resort, which is the fallback the field-level paths beside this one already
// use for the same question.
func declaredConstraintTable(constraint schemamodel.Constraint, structToTable map[string]string) string {
	if constraint.Table != "" {
		return constraint.Table
	}
	if table := structToTable[constraint.StructName]; table != "" {
		return table
	}
	return constraint.StructName
}

func appendTableLevelForeignKeys(candidates []affectedForeignKey, desired *schemamodel.Database) []affectedForeignKey {
	for _, constraint := range desired.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		candidates = append(candidates, affectedForeignKey{
			table:   constraint.Table,
			name:    constraint.Name,
			columns: constraint.Columns,
			ref: &ast.ForeignKeyRef{
				Table:    constraint.ForeignTable,
				Column:   constraint.ForeignColumn,
				Columns:  constraint.ForeignColumns,
				OnDelete: constraint.OnDelete,
				OnUpdate: constraint.OnUpdate,
			},
		})
	}
	return candidates
}

// columnTypeChangesByTable returns the columns whose SQL type is changing in
// this diff, keyed table -> set of column names. Only "type" changes are
// collected; nullability, default, uniqueness, and similar changes do not
// disturb a foreign key's referential type match.
func foreignKeyBlockingColumnChangesByTable(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, tableDiff := range diff.TablesModified {
		tableIdentity := semantics.QualifiedTableIdentityKey(tableDiff.TableName)
		for _, colDiff := range tableDiff.ColumnsModified {
			if strings.TrimSpace(colDiff.Changes["type"]) == "" {
				continue
			}
			columns := result[tableIdentity]
			if columns == nil {
				columns = make(map[string]struct{})
				result[tableIdentity] = columns
			}
			columns[semantics.ColumnIdentityKey(colDiff.ColumnName)] = struct{}{}
		}
		if len(tableDiff.ColumnsRemoved) > 0 {
			columns := result[tableIdentity]
			if columns == nil {
				columns = make(map[string]struct{})
				result[tableIdentity] = columns
			}
			for _, column := range tableDiff.ColumnsRemoved {
				columns[semantics.ColumnIdentityKey(column.Name)] = struct{}{}
			}
		}
	}
	return result
}

// foreignKeyTouchesTypeChange reports whether any of the foreign key's local or
// referenced columns is changing type.
func foreignKeyTouchesTypeChange(
	fk affectedForeignKey,
	typeChanged map[string]map[string]struct{},
	semantics identifier.Semantics,
) bool {
	localChanges := typeChanged[semantics.QualifiedTableIdentityKey(fk.table)]
	for _, column := range fk.columns {
		if _, ok := localChanges[semantics.ColumnIdentityKey(column)]; ok {
			return true
		}
	}
	referencedChanges := typeChanged[semantics.QualifiedTableIdentityKey(fk.ref.Table)]
	for _, column := range fk.ref.ReferencedColumns() {
		if _, ok := referencedChanges[semantics.ColumnIdentityKey(column)]; ok {
			return true
		}
	}
	return false
}

func foreignKeyRemovalTouchesBlockingChange(
	info difftypes.ForeignKeyRemovalInfo,
	changes map[string]map[string]struct{},
	semantics identifier.Semantics,
) bool {
	localChanges := changes[semantics.QualifiedTableIdentityKey(info.TableName)]
	for _, column := range info.Columns {
		if _, changed := localChanges[semantics.ColumnIdentityKey(column)]; changed {
			return true
		}
	}
	referencedChanges := changes[semantics.QualifiedTableIdentityKey(info.ForeignTable)]
	for _, column := range info.ForeignColumns {
		if _, changed := referencedChanges[semantics.ColumnIdentityKey(column)]; changed {
			return true
		}
	}
	return false
}

func foreignKeyRemovalDetailsByHost(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) map[constraintHostKey]difftypes.ForeignKeyRemovalInfo {
	details := make(map[constraintHostKey]difftypes.ForeignKeyRemovalInfo, len(diff.ForeignKeysRemovedWithTables))
	for _, info := range diff.ForeignKeysRemovedWithTables {
		if len(info.Columns) == 0 || info.ForeignTable == "" || len(info.ForeignColumns) == 0 {
			continue
		}
		details[info.Identity] = info
	}
	return details
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	indexes *indexscope.Resolver,
) ([]ast.Node, error) {
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff.IndexRemovals(),
	)
	guardedDrops := p.capabilities().Has(capability.DropIndexIfExists)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	for _, ref := range diff.IndexAdditions() {
		index, err := indexes.Resolve(ref)
		if err != nil {
			return nil, err
		}
		for removal := range replacements.Matches(ref) {
			dropIndexNode := ast.NewDropIndex(removal.Name).SetTable(removal.TableName)
			if guardedDrops {
				dropIndexNode.SetIfExists()
			}
			if _, ownedByConstraint := constraintBacked[removal]; ownedByConstraint {
				dropIndexNode.SetEnforcesUniqueConstraint()
			}
			result = append(result, dropIndexNode)
		}
		indexNode := fromschema.FromIndex(index)
		indexNode.Table = ref.TableName
		indexNode.IfNotExists = false
		result = append(result, indexNode)
	}
	return result, nil
}

func (p *Planner) removeIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
) []ast.Node {
	// The IF EXISTS guard on DROP INDEX is capability-gated INTENT (issue
	// #226): MariaDB accepts it, MySQL has no such form. The renderer
	// additionally validates the flag against its own target set, so the
	// guard is emitted only when both layers agree. Gating here (rather than
	// always setting the flag) keeps the capability composable — disabling
	// capability.DropIndexIfExists on a planner actually changes the plan.
	guarded := p.capabilities().Has(capability.DropIndexIfExists)
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff.IndexAdditions(),
	)
	rebuiltAsConstraint := diff.IndexRemovalsRebuiltAsUniqueConstraints()
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	for _, ref := range diff.IndexRemovals() {
		if replacements.Contains(ref) {
			continue
		}
		// A removal a UNIQUE constraint addition rebuilds was already emitted
		// ahead of that addition, which is the only order the server accepts;
		// dropping it again here would land after the add and delete the key
		// the constraint now is.
		if _, rebuilt := rebuiltAsConstraint[ref]; rebuilt {
			continue
		}
		dropIndexNode := ast.NewDropIndex(ref.Name).SetTable(ref.TableName)
		if guarded {
			dropIndexNode.SetIfExists()
		}
		if _, ownedByConstraint := constraintBacked[ref]; ownedByConstraint {
			dropIndexNode.SetEnforcesUniqueConstraint()
		}
		result = append(result, dropIndexNode)
	}
	return result
}

func (p *Planner) removeTables(result []ast.Node, diff *difftypes.SchemaDiff, desired *schemamodel.Database) []ast.Node {
	for _, tableName := range deporder.TableDropOrder(diff.TablesRemoved, desired) {
		dropTableNode := ast.NewDropTable(tableName).
			SetIfExists().
			SetCascade().
			SetComment("WARNING: This will delete all data!")

		result = append(result, dropTableNode)
	}
	return result
}

func (p *Planner) handleEnumRemovals(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, enumName := range diff.EnumsRemoved {
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: %s enum %s removal requires updating all columns that use this enum type!", p.enumDialectLabel(), enumName))
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
//  4. Remove constraints
//  5. Remove indexes (safe operations)
//  6. Remove tables (dangerous - commented out by default)
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
//	generated := &schemamodel.Database{
//		Tables: []schemamodel.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []schemamodel.Field{
//			{Name: "id", Type: "INT AUTO_INCREMENT", StructName: "User", Primary: true},
//			{Name: "status", Type: "ENUM('active','inactive')", StructName: "User"},
//		},
//	}
//
//	nodes, err := planner.GenerateMigrationAST(diff, generated)
//	if err != nil {
//		return err
//	}
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
// Returns a slice of AST nodes representing SQL statements or an error when
// the diff cannot be planned safely. Each node can be rendered to SQL using a
// MySQL-specific visitor.
func (p *Planner) GenerateMigrationAST(diff *difftypes.SchemaDiff, desired *schemamodel.Database) ([]ast.Node, error) {
	var result []ast.Node
	if desired == nil {
		desired = &schemamodel.Database{}
	}
	// One fold, at the door, beside the index resolver that has always been
	// here. A diff the comparator produced arrives with its identities
	// resolved; one an embedder built by hand does not, and the zero identity
	// is a single key -- every such constraint would pair with every other
	// (stokaro/ptah#1663).
	constraintscope.Normalize(diff, diff.EffectiveIdentifierSemantics(p.targetDialect()))
	indexes, err := indexscope.NewResolverWithSemantics(
		p.targetDialect(),
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff,
		desired,
	)
	if err != nil {
		return nil, err
	}

	if err := p.rejectUniqueIncludeConstraints(diff, desired); err != nil {
		return nil, err
	}

	// Note: MySQL doesn't use separate enum types like PostgreSQL
	// Enums are handled inline in column definitions, so we skip enum creation steps

	// 0. Create the schemas the added objects live in, before any of them.
	// SQL Server only; the reason a schema is not created on the other
	// dialects of this planner is on [Planner.planSchemaPreconditions].
	result = p.planSchemaPreconditions(result, diff)

	// 0a. Name the declared objects this target cannot host, before anything
	// else that emits SQL, mirroring the order `schema render` emits them in.
	result = p.reportUnsupportedObjects(result, diff)

	// 0a. Plan the sequences this target does host, before tables: a column
	// DEFAULT may draw from one, and SQL Server enforces that dependency in
	// both directions.
	result = p.planSequences(result, diff)

	// 0a2. Plan the roles this target does manage, before tables, because a
	// grant names a role that has to exist.
	result = p.planRoles(result, diff, desired)

	// 0a3. Plan the domains this target does host, before tables: a column may
	// be declared with the domain as its type.
	result = p.planDomains(result, diff)
	result = p.planCompositeTypes(result, diff)

	// 0b. Plan the stored functions this target does host. Functions are
	// planned before tables because a generated column or a CHECK constraint
	// may call one, and the function must exist first.
	result = p.planFunctions(result, diff, desired)

	// 1. Add enum change warnings (MySQL limitations)
	result = p.addEnumChangeWarnings(result, diff)

	// 2. Handle enum modifications (MySQL limitations)
	result = p.handleEnumModifications(result, diff)

	// 3. Add new tables
	result = p.addNewTables(result, diff, desired)

	// 4. Modify existing tables. On MySQL/MariaDB a column-type change on a
	// column that participates in a foreign key — as the referencing OR the
	// referenced column — is rejected while the constraint exists (MySQL
	// errno 3780, MariaDB errno 1832). The affected foreign keys are dropped
	// before any column modification and, where the constraint machinery does
	// not already own the re-add, recreated after every modification so the
	// change lands cleanly and both ends of the key stay type-compatible
	// (issue #694). A foreign key whose definition also changes (an ON DELETE
	// drift, issue #189) has its pre-MODIFY drop owned here but its re-add left
	// to addNewConstraints, which runs after the modifications; fkPlan.dropped
	// tells that machinery — and removeConstraints — to suppress their own now
	// redundant drop of the same (table, name).
	fkPlan := p.planColumnTypeForeignKeyChanges(diff, desired)
	result = append(result, fkPlan.drops...)

	result, err = p.modifyExistingTables(result, diff, desired)
	if err != nil {
		return nil, err
	}

	result = append(result, fkPlan.readds...)

	// 4.5. Add and modify views/triggers after tables exist.
	result = p.addNewViews(result, diff)
	result = p.modifyExistingViews(result, diff)
	result = p.retargetSynonyms(result, diff)
	result = p.addNewSynonyms(result, diff)
	result = p.addExtendedProperties(result, diff)
	if err := p.rejectMaterializedViews(diff); err != nil {
		return nil, err
	}
	result = p.addNewMaterializedViews(result, diff)
	result = p.modifyExistingMaterializedViews(result, diff)
	result = p.addNewTriggers(result, diff)
	result = p.modifyExistingTriggers(result, diff)

	// 5. Add new indexes
	result, err = p.addNewIndexes(result, diff, indexes)
	if err != nil {
		return nil, err
	}

	// 5.5. Add new constraints (must be done after tables and columns exist).
	// fkPlan.dropped suppresses the drop half of any FK modification whose
	// pre-MODIFY drop was already emitted at step 4.
	result = p.addNewConstraints(result, diff, desired, fkPlan.dropped)

	// 5.6. Add field-level foreign keys for new tables after referenced
	// unique indexes and constraints have been created.
	result = p.addForeignKeyConstraintsForNewTables(result, diff, desired)

	// 5.7. Grant privileges once the objects they name exist.
	result = p.planGrants(result, diff)

	// 5.8. Plan row-level security once the tables its predicates name exist.
	// Unlike sequences, roles and functions this cannot run before tables: a
	// security policy is schema-bound to the table it filters, and the engine
	// resolves that name at creation time.
	result = p.planRLS(result, diff, desired)

	// 6. Remove constraints before indexes. MySQL-family servers keep the
	// backing index after DROP FOREIGN KEY when the index was auto-created, so
	// rollback plans may need to drop both. The FK must go first. fkPlan.dropped
	// suppresses any removed-only FK already dropped at step 4.
	result = p.removeConstraints(result, diff, fkPlan.dropped)

	// 6.6. Remove triggers and view-like objects before dependent tables.
	result = p.removeTriggers(result, diff)
	result = p.removeMaterializedViews(result, diff)
	result = p.removeViews(result, diff)
	result = p.removeExtendedProperties(result, diff)
	result = p.removeSynonyms(result, diff)

	// 6.7. Remove indexes after constraints so FK-backed indexes can be dropped.
	result = p.removeIndexes(result, diff)

	// 6z. Drop the security policies before the tables they are schema-bound
	// to, which the engine will not drop out from under a standing policy.
	result = p.removeRLS(result, diff)

	// 7. Remove tables (dangerous!)
	result = p.removeTables(result, diff, desired)

	// 7a. Remove sequences after the tables whose defaults drew from them.
	result = p.removeSequences(result, diff)

	// 7a2. Remove domains after the tables whose columns were typed by them:
	// Oracle answers ORA-11502 to a domain that still has dependents.
	result = p.removeDomains(result, diff)
	result = p.removeCompositeTypes(result, diff)

	// 7b. Revoke, then drop the roles, after the tables their grants named.
	result = p.removeGrantsAndRoles(result, diff)

	// 8. Handle enum removals (MySQL-specific warnings)
	result = p.handleEnumRemovals(result, diff)

	return result, nil
}

func (p *Planner) rejectUniqueIncludeConstraints(diff *difftypes.SchemaDiff, desired *schemamodel.Database) error {
	if diff != nil {
		for _, add := range diff.ConstraintsAddedWithTables {
			if !strings.EqualFold(add.Type, "UNIQUE") || len(add.IncludeColumns) == 0 {
				continue
			}
			return p.uniqueIncludeUnsupportedError(add.Name)
		}
	}
	if desired == nil {
		return nil
	}
	for _, constraint := range desired.Constraints {
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(constraint.IncludeColumns) == 0 {
			continue
		}
		return p.uniqueIncludeUnsupportedError(constraint.Name)
	}
	return nil
}

func (p *Planner) uniqueIncludeUnsupportedError(constraintName string) error {
	return &ptaherr.CapabilityError{
		Dialect: p.targetDialect(),
		Feature: "unique constraint include columns",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s does not support PostgreSQL INCLUDE columns on UNIQUE constraints; remove include columns from constraint %s or target PostgreSQL",
			p.enumDialectLabel(),
			constraintName,
		),
	}
}

func (p *Planner) rejectMaterializedViews(diff *difftypes.SchemaDiff) error {
	if len(diff.MaterializedViewsAdded) == 0 &&
		len(diff.MaterializedViewsModified) == 0 &&
		len(diff.MaterializedViewsRemoved) == 0 {
		return nil
	}
	// A target that has the object is planned rather than refused.
	//
	// This planner serves four engines and only one of them owns materialized
	// views: measured, MaterializedViews is false on the MySQL, MariaDB and SQL
	// Server presets and true on both Oracle presets. Keying the refusal on the
	// planner rather than on the capability made Ptah answer the same question
	// three different ways -- the preset published a check mark, the renderer
	// emitted the DDL, and this refusal told an Oracle user that MYSQL does not
	// support it (stokaro/ptah#1883).
	if p.capabilities().Has(capability.MaterializedViews) {
		return nil
	}
	// Same reason as enumDialectLabel: this planner serves more engines than
	// its package name. MySQL and MariaDB keep the joint spelling because the
	// two share the refusal and existing callers assert on it.
	engine := p.enumDialectLabel()
	if engine == "MySQL-family" {
		engine = "MySQL or MariaDB"
	}
	// The names are part of the refusal: "remove matview definitions" tells an
	// operator what kind of thing to look for, not which one, and a schema with
	// forty objects is then a search (stokaro/ptah#1628).
	message := fmt.Sprintf(
		"materialized views are not supported by %s; remove matview definitions for this target: %s",
		engine, strings.Join(changedMaterializedViewNames(diff), ", "))
	return &ptaherr.CapabilityError{
		Dialect: p.targetDialect(),
		Feature: "materialized views",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: message,
	}
}

func (p *Planner) addNewViews(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The view travels WITH the change, so this renders what it was handed
	// rather than looking the name back up in the desired schema.
	for _, view := range diff.ViewsAdded {
		result = append(result, fromschema.FromView(view))
	}
	return result
}

func (p *Planner) modifyExistingViews(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The view this change leaves behind travels WITH it (stokaro/ptah#2315).
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Desired.Name == "" {
			continue
		}
		result = append(result, fromschema.FromView(viewDiff.Desired).SetReplace())
	}
	return result
}

func (p *Planner) removeViews(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, view := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(view.Name).SetIfExists())
	}
	return result
}

func (p *Planner) addNewSynonyms(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The target travels WITH the change, so this renders what it was handed
	// rather than looking the name back up in the desired schema.
	for _, synonym := range diff.SynonymsAdded {
		result = append(result, fromschema.FromSynonym(synonym))
	}
	return result
}

// retargetSynonyms drops and recreates a synonym whose target changed.
//
// The pair is emitted together, drop first, because T-SQL has no ALTER SYNONYM
// and CREATE SYNONYM refuses a name that already exists. Splitting the two
// across the add and remove phases would put the create before the drop and
// fail at the server.
func (p *Planner) retargetSynonyms(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The synonym travels WITH the change (stokaro/ptah#2315). The two target
	// strings are the change; the create needs the object.
	for _, synonymDiff := range diff.SynonymsModified {
		synonym := synonymDiff.Desired
		if synonym.Name == "" {
			continue
		}
		result = append(result, ast.NewDropSynonym(synonymDiff.SynonymName).SetIfExists())
		result = append(result, fromschema.FromSynonym(synonym))
	}
	return result
}

func (p *Planner) removeSynonyms(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, synonym := range diff.SynonymsRemoved {
		result = append(result, ast.NewDropSynonym(synonym.QualifiedName()).SetIfExists())
	}
	return result
}

// addExtendedProperties emits the extended properties a diff adds and the
// updates it plans for the ones whose value changed.
//
// Both go here, after the tables and views exist: sp_addextendedproperty and
// sp_updateextendedproperty resolve @level1name through the catalog and answer
// `Cannot find the object ... because it does not exist or you do not have
// permission` when the owner is not there yet.
//
// An update rather than a drop and an add, because SQL Server has the
// statement and dropping first would take the property away for the length of
// the script.
func (p *Planner) addExtendedProperties(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, ref := range diff.ExtendedPropertiesAdded {
		result = append(result, extendedPropertyNode(ast.ExtendedPropertyAdd, ref))
	}
	for _, changed := range diff.ExtendedPropertiesModified {
		result = append(result, extendedPropertyNode(ast.ExtendedPropertyUpdate, changed.ExtendedPropertyRef))
	}
	return result
}

// removeExtendedProperties drops the properties a diff removes, before the
// objects they hang off are dropped.
//
// The order is the one SQL Server forces rather than a preference: dropping
// the table takes its properties with it, and a sp_dropextendedproperty that
// runs afterwards answers `Property cannot be dropped. Property does not
// exist`.
func (p *Planner) removeExtendedProperties(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, ref := range diff.ExtendedPropertiesRemoved {
		result = append(result, extendedPropertyNode(ast.ExtendedPropertyDrop, ref))
	}
	return result
}

// extendedPropertyNode builds the node for one operation on one property.
func extendedPropertyNode(
	operation ast.ExtendedPropertyOperation,
	ref difftypes.ExtendedPropertyRef,
) *ast.ExtendedPropertyNode {
	return ast.NewExtendedProperty(operation, ref.Name).
		SetOwner(ref.Schema, ref.Table, ref.Column).
		SetValue(ref.Value)
}

func (p *Planner) addNewTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The definition travels WITH the entry (stokaro/ptah#2315).
	for _, triggerRef := range diff.TriggersAdded {
		if triggerRef.Desired.Name != "" {
			result = append(result, fromschema.FromTrigger(triggerRef.Desired))
		}
	}
	return result
}

func (p *Planner) modifyExistingTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, triggerDiff := range diff.TriggersModified {
		if triggerDiff.Desired.Name != "" {
			result = append(result, fromschema.FromTrigger(triggerDiff.Desired).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, triggerRef := range diff.TriggersRemoved {
		result = append(result, ast.NewDropTrigger(triggerRef.TriggerName, triggerRef.TableName).SetIfExists())
	}
	return result
}

// addNewMaterializedViews emits the declared materialized views a diff adds.
//
// It runs beside addNewViews rather than inside it because the two produce
// different statements, and it is a no-op on every target whose preset does not
// carry the object: rejectMaterializedViews has already refused those.
func (p *Planner) addNewMaterializedViews(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
) []ast.Node {
	// The view travels WITH the change, so this renders what it was handed
	// rather than looking the name back up in the desired schema.
	for _, view := range diff.MaterializedViewsAdded {
		result = append(result, fromschema.FromMaterializedView(view))
	}
	return result
}

// modifyExistingMaterializedViews replaces a changed materialized view.
//
// Oracle has no CREATE OR REPLACE for one -- the statement is CREATE
// MATERIALIZED VIEW and nothing else -- so a change is a drop and a create, the
// same shape the PostgreSQL planner uses for the same reason.
func (p *Planner) modifyExistingMaterializedViews(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
) []ast.Node {
	// The view travels WITH the change (stokaro/ptah#2315).
	for _, viewDiff := range diff.MaterializedViewsModified {
		if view := viewDiff.Desired; view.Name != "" {
			result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists())
			result = append(result, fromschema.FromMaterializedView(view))
		}
	}
	return result
}

// removeMaterializedViews drops the materialized views a diff removes, before
// the tables they read.
func (p *Planner) removeMaterializedViews(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, view := range diff.MaterializedViewsRemoved {
		result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists())
	}
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
func (p *Planner) addNewConstraints(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	bracketDropped map[constraintHostKey]struct{},
) []ast.Node {
	// Resolve struct → table name once for the field-level synthesis fallbacks.
	structToTable := make(map[string]string, len(desired.Tables))
	for _, t := range desired.Tables {
		structToTable[t.StructName] = t.Name
	}

	state := newConstraintPlanState(diff, diff.EffectiveIdentifierSemantics(p.targetDialect()))

	// A foreign key whose column also changes type was already dropped before
	// the column modifications (issue #694, step 4). Seeding those (table, name)
	// keys into droppedForModify makes the re-add paths below skip the now
	// redundant drop while still emitting the ADD with the new definition —
	// MySQL has no IF EXISTS guard on constraint drops, so a duplicate drop
	// would abort the migration.
	maps.Copy(state.droppedForModify, bracketDropped)

	result = p.addPrimaryKeyConstraintsWithTables(result, diff.ConstraintsAddedWithTables, state)
	result = p.addCheckAndUniqueConstraintsWithTables(
		result,
		diff.ConstraintsAddedWithTables,
		state.removalByTableName,
		state.handled,
		state.droppedForModify,
		diff.IndexRemovalsRebuiltAsUniqueConstraints(),
		state.semantics,
	)
	result = p.addNamedConstraintsByKind(result, diff, desired, structToTable, state, nonForeignKeyConstraints)
	result = p.addForeignKeyConstraintsWithTables(result, diff.ConstraintsAddedWithTables, state)
	result = p.addNamedConstraintsByKind(result, diff, desired, structToTable, state, foreignKeyConstraints)
	return result
}

type constraintKindFilter int

const (
	nonForeignKeyConstraints constraintKindFilter = iota
	foreignKeyConstraints
)

type constraintPlanState struct {
	semantics          identifier.Semantics
	removedNames       map[string]struct{}
	removalByTableName map[constraintHostKey]difftypes.ConstraintRemovalInfo
	removalsByName     map[string][]difftypes.ConstraintRemovalInfo
	addedHostsByName   map[string]map[string]struct{}
	handled            map[string]struct{}
	droppedForModify   map[constraintHostKey]struct{}
}

func newConstraintPlanState(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) constraintPlanState {
	// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved
	// is a modification (the comparator expresses a changed constraint as
	// remove + add of the same name — e.g. an on_delete change on a field-level
	// FK, issue #189). Its old definition must be dropped before the re-add or
	// the ADD CONSTRAINT collides with the still-present constraint of the same
	// name (errno 1826 for FKs / 3822 for CHECKs).
	removedNames := make(map[string]struct{}, len(diff.ConstraintsRemovedWithTables))
	for _, name := range constraintscope.RemovalNames(diff) {
		removedNames[semantics.IndexIdentityKey(name)] = struct{}{}
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
	removalByTableName := make(map[constraintHostKey]difftypes.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalByTableName[info.Identity] = info
	}

	// Removal info grouped by bare name, so the name-only ConstraintsAdded loop
	// below can scope a modified non-FK constraint's DROP to its concrete host
	// table(s). The comparator records every removal in
	// ConstraintsRemovedWithTables in lockstep with the bare ConstraintsRemoved
	// list, so a modified constraint's host is normally known here even though
	// the bare loop iterates names alone.
	removalsByName := make(map[string][]difftypes.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		name := semantics.IndexIdentityKey(info.Name)
		removalsByName[name] = append(removalsByName[name], info)
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
		name := semantics.IndexIdentityKey(add.Name)
		hosts := addedHostsByName[name]
		if hosts == nil {
			hosts = make(map[string]struct{})
			addedHostsByName[name] = hosts
		}
		hosts[semantics.QualifiedTableIdentityKey(add.TableName)] = struct{}{}
	}

	return constraintPlanState{
		semantics:          semantics,
		removedNames:       removedNames,
		removalByTableName: removalByTableName,
		removalsByName:     removalsByName,
		addedHostsByName:   addedHostsByName,
		handled:            make(map[string]struct{}),
		droppedForModify:   make(map[constraintHostKey]struct{}),
	}
}

func (p *Planner) addPrimaryKeyConstraintsWithTables(
	result []ast.Node,
	additions []difftypes.ConstraintAdditionInfo,
	state constraintPlanState,
) []ast.Node {
	// Prefer the table-qualified additions when present. A field-level FK from an
	// embedded inline-relation mixin shares one name across every host table, and
	// down migrations restore modified CHECK/UNIQUE definitions from the
	// introspected DB schema. ConstraintsAddedWithTables carries the concrete
	// table + definition. Names handled here are recorded so the name-only loop
	// skips them.
	for _, add := range additions {
		if add.Type != "PRIMARY KEY" || add.TableName == "" || len(add.Columns) == 0 {
			continue
		}
		if _, modified := state.removalByTableName[add.Identity]; modified {
			continue
		}
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: ast.NewPrimaryKeyConstraint(add.Columns...)}},
		})
		state.handled[state.semantics.IndexIdentityKey(add.Name)] = struct{}{}
	}
	return result
}

func (p *Planner) addNamedConstraintsByKind(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	structToTable map[string]string,
	state constraintPlanState,
	kind constraintKindFilter,
) []ast.Node {
	wantForeignKey := kind == foreignKeyConstraints
	// Fallback for added constraints with no table-qualified FK entry above
	// (table-level CHECK/UNIQUE, or field-level synthesis resolved by name).
	for _, constraintName := range constraintscope.AdditionNames(diff) {
		constraintIdentity := state.semantics.IndexIdentityKey(constraintName)
		if _, done := state.handled[constraintIdentity]; done {
			continue
		}
		if p.constraintNameIsForeignKey(constraintName, desired, structToTable) != wantForeignKey {
			continue
		}

		// For a modification, emit the DROP(s) first so they precede the
		// re-add, scoped to the constraint's concrete host table(s) — never a
		// name-keyed single-winner lookup, which collapses multiple removal
		// hosts onto one arbitrary table (issue #207).
		if _, modified := state.removedNames[constraintIdentity]; modified {
			result = p.emitModifyDropForName(
				result,
				constraintIdentity,
				state.removalsByName,
				state.addedHostsByName[constraintIdentity],
				state.droppedForModify,
				state.semantics,
			)
		}

		result = p.appendAddConstraint(result, constraintName, desired, structToTable)
	}
	return result
}

func (p *Planner) addForeignKeyConstraintsWithTables(
	result []ast.Node,
	additions []difftypes.ConstraintAdditionInfo,
	state constraintPlanState,
) []ast.Node {
	for _, add := range additions {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// For a modification, emit the DROP FOREIGN KEY from this exact host
		// table before its re-add — only when this host's (table, name) is in
		// the removal set; a pure-add host gets no phantom drop.
		key := add.Identity
		if info, modified := state.removalByTableName[key]; modified {
			result = p.appendScopedDrop(result, info, state.droppedForModify, state.semantics)
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		state.handled[state.semantics.IndexIdentityKey(add.Name)] = struct{}{}
	}
	return result
}

func (p *Planner) constraintNameIsForeignKey(constraintName string, desired *schemamodel.Database, structToTable map[string]string) bool {
	for _, constraint := range desired.Constraints {
		if constraint.Name == constraintName {
			return strings.EqualFold(constraint.Type, "FOREIGN KEY")
		}
	}
	for _, field := range desired.Fields {
		if field.Foreign == "" {
			continue
		}
		tableName := structToTable[field.StructName]
		if tableName == "" {
			tableName = field.StructName
		}
		if foreignKeyName(tableName, field) == constraintName {
			return true
		}
	}
	return false
}

func (p *Planner) addCheckAndUniqueConstraintsWithTables(
	result []ast.Node,
	additions []difftypes.ConstraintAdditionInfo,
	removalByTableName map[constraintHostKey]difftypes.ConstraintRemovalInfo,
	handled map[string]struct{},
	droppedForModify map[constraintHostKey]struct{},
	rebuiltIndexes map[difftypes.IndexRef]struct{},
	semantics identifier.Semantics,
) []ast.Node {
	for _, add := range additions {
		constraint := p.constraintAdditionNode(add)
		if constraint == nil {
			continue
		}
		key := add.Identity
		if info, modified := removalByTableName[key]; modified {
			result = p.appendScopedDrop(result, info, droppedForModify, semantics)
		}
		result = p.dropIndexRebuiltAsConstraint(result, add, rebuiltIndexes)
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}},
		})
		handled[semantics.IndexIdentityKey(add.Name)] = struct{}{}
	}
	return result
}

// dropIndexRebuiltAsConstraint drops the key this UNIQUE constraint addition is
// about to rebuild, before the addition rather than after it.
//
// A unique key and its constraint are one catalog row here, so
// `ADD CONSTRAINT ... UNIQUE` collides with a key of that name on that table:
// MySQL 9.7.1 answers `Error 1061 (42000): Duplicate key name 'uq_users_email'`.
// The pipeline emits constraint additions before index removals, so the drop is
// emitted here and [Planner.removeIndexes] leaves it alone; see
// [difftypes.SchemaDiff.IndexRemovalsRebuiltAsUniqueConstraints].
func (p *Planner) dropIndexRebuiltAsConstraint(
	result []ast.Node,
	add difftypes.ConstraintAdditionInfo,
	rebuiltIndexes map[difftypes.IndexRef]struct{},
) []ast.Node {
	ref := difftypes.IndexRef{Name: add.Name, TableName: add.TableName}
	if _, rebuilt := rebuiltIndexes[ref]; !rebuilt {
		return result
	}
	dropIndexNode := ast.NewDropIndex(ref.Name).SetTable(ref.TableName)
	if p.capabilities().Has(capability.DropIndexIfExists) {
		dropIndexNode.SetIfExists()
	}
	return append(result, dropIndexNode)
}

func (p *Planner) constraintAdditionNode(add difftypes.ConstraintAdditionInfo) *ast.ConstraintNode {
	if add.TableName == "" {
		return nil
	}
	switch add.Type {
	case "CHECK":
		if add.CheckExpression == "" || !p.capabilities().Has(capability.CheckConstraintsEnforced) {
			return nil
		}
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       add.Name,
			Expression: add.CheckExpression,
		}
	case "UNIQUE":
		if len(add.Columns) == 0 {
			return nil
		}
		return ast.NewUniqueConstraint(add.Name, add.Columns...)
	default:
		return nil
	}
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
// ConstraintsAddedWithTables because the prior definition could not be
// reconstructed from schema context. In that case every recorded removal host
// is dropped here and removeConstraints skips the name entirely (its
// hostless-re-add rule), so each host is still dropped exactly once. A name
// with no recorded removal host at all emits no drop: MySQL has no
// anonymous-block equivalent of the postgres
// information_schema DO fallback to resolve the owner at runtime, so the
// re-add proceeds alone (pre-existing behavior for hand-built diffs).
func (p *Planner) emitModifyDropForName(
	result []ast.Node,
	name string,
	removalsByName map[string][]difftypes.ConstraintRemovalInfo,
	addedHosts map[string]struct{},
	droppedForModify map[constraintHostKey]struct{},
	semantics identifier.Semantics,
) []ast.Node {
	for _, info := range removalsByName[name] {
		if info.TableName == "" {
			continue
		}
		if len(addedHosts) > 0 {
			if _, reAdded := addedHosts[semantics.QualifiedTableIdentityKey(info.TableName)]; !reAdded {
				continue
			}
		}
		result = p.appendScopedDrop(result, info, droppedForModify, semantics)
	}
	return result
}

// appendScopedDrop appends a single table-qualified, type-aware constraint
// drop (ALTER TABLE <host> DROP FOREIGN KEY <name> / DROP CONSTRAINT <name>),
// deduped per (table, name) via dropped so a constraint name shared across
// host tables is dropped once per host and never twice for the same host.
// MySQL accepts no IF EXISTS on these drops, so exactly-once emission is what
// keeps a duplicate drop from aborting the migration.
func (p *Planner) appendScopedDrop(
	result []ast.Node,
	info difftypes.ConstraintRemovalInfo,
	dropped map[constraintHostKey]struct{},
	semantics identifier.Semantics,
) []ast.Node {
	dedupKey := info.Identity
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
func (p *Planner) foreignKeyAdditionNode(add difftypes.ConstraintAdditionInfo) *ast.AlterTableNode {
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
func (p *Planner) appendAddConstraint(result []ast.Node, constraintName string, desired *schemamodel.Database, structToTable map[string]string) []ast.Node {
	for _, constraint := range desired.Constraints {
		if constraint.Name != constraintName {
			continue
		}
		// A CHECK constraint on a target that parses but does not enforce
		// CHECK (capability.CheckConstraintsEnforced absent — MySQL before
		// 8.0.16) would be a silent no-op in the live schema while ptah
		// believes it applied; surface that loudly instead of emitting it
		// (issue #226).
		if constraint.Type == "CHECK" && !p.capabilities().Has(capability.CheckConstraintsEnforced) {
			return append(result, ast.NewComment(fmt.Sprintf("WARNING: CHECK constraint %s skipped - %s", constraint.Name, p.checkNotEnforcedMessage())))
		}
		if astConstraint := p.convertConstraintToAST(constraint); astConstraint != nil {
			return append(result, &ast.AlterTableNode{
				Name:       declaredConstraintTable(constraint, structToTable),
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: astConstraint}},
			})
		}
		if constraint.Type == "EXCLUDE" {
			return append(result, ast.NewComment(fmt.Sprintf("WARNING: EXCLUDE constraint %s not supported in %s (PostgreSQL-specific feature)", constraint.Name, p.constraintDialectLabel())))
		}
		return result
	}

	if node, ok := p.fieldLevelCheckConstraintNode(constraintName, desired, structToTable); ok {
		if node != nil {
			result = append(result, node)
		}
		return result
	}

	if node, ok := p.fieldLevelForeignKeyConstraintNode(constraintName, desired, structToTable); ok {
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
func (p *Planner) fieldLevelCheckConstraintNode(constraintName string, desired *schemamodel.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range desired.Fields {
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
			return ast.NewComment(fmt.Sprintf("WARNING: CHECK constraint %s skipped - %s", name, p.checkNotEnforcedMessage())), true
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

func (p *Planner) checkNotEnforcedMessage() string {
	switch p.targetDialect() {
	case platform.SQLServer:
		return "the target does not enforce CHECK constraints"
	default:
		return "the target parses but does not enforce CHECK constraints (MySQL < 8.0.16)"
	}
}

func (p *Planner) constraintDialectLabel() string {
	switch p.targetDialect() {
	case platform.SQLServer:
		return "SQL Server"
	default:
		return "MySQL"
	}
}

// fieldLevelForeignKeyConstraintNode builds the ADD CONSTRAINT node for a
// synthesized field-level foreign= constraint whose on_delete / on_update action
// changed (issue #189). Without this the FK would be dropped (via
// removeConstraints) but never re-added with the new action — a destructive,
// silently-broken migration. New columns/tables are handled by the inline FK in
// CREATE TABLE / ALTER TABLE ADD COLUMN and the comparator deliberately skips
// synthesizing those, so only existing-column FK action changes reach here.
func (p *Planner) fieldLevelForeignKeyConstraintNode(constraintName string, desired *schemamodel.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range desired.Fields {
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
//
// FK removals are still emitted when the owning table is also being dropped.
// MySQL/MariaDB do not support PostgreSQL-style DROP TABLE CASCADE, and mutual
// FK cycles cannot be solved by table ordering alone. Dropping the FKs first
// gives both acyclic graphs and cycles a deterministic rollback path. Non-FK
// constraints on dropped tables stay implicit in the DROP TABLE operation.
func (p *Planner) removeConstraints(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	bracketDropped map[constraintHostKey]struct{},
) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	// (table, name) pairs being re-added — modifications owned by
	// addNewConstraints — plus, per name, how many hosts were recorded at all.
	modifyHosts := make(map[constraintHostKey]struct{}, len(diff.ConstraintsAddedWithTables))
	addedHostCounts := make(map[string]int, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// Hostless addition entries do not count as recorded hosts —
			// mirroring addedHostsByName in addNewConstraints — so the
			// hostless-re-add rule below still engages and this side keeps
			// skipping the hosts the add side already dropped.
			continue
		}
		modifyHosts[add.Identity] = struct{}{}
		addedHostCounts[semantics.IndexIdentityKey(add.Name)]++
	}
	addedBareNames := make(map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	for _, name := range constraintscope.AdditionNames(diff) {
		addedBareNames[semantics.IndexIdentityKey(name)] = struct{}{}
	}

	droppedTables := make(map[string]struct{}, len(diff.TablesRemoved))
	for _, t := range diff.TablesRemoved {
		droppedTables[semantics.QualifiedTableIdentityKey(t)] = struct{}{}
	}

	dropped := make(map[constraintHostKey]struct{})
	for _, info := range diff.ConstraintsRemovedWithTables {
		if info.TableName == "" {
			// No host recorded: there is no valid table-qualified ALTER TABLE
			// to emit and no runtime fallback on MySQL. Real comparator output
			// always carries the host.
			continue
		}
		key := info.Identity
		if _, modified := modifyHosts[key]; modified {
			// addNewConstraints owns this host's DROP-then-ADD; do not re-drop.
			continue
		}
		if _, preDropped := bracketDropped[key]; preDropped {
			// A column-type change already dropped this foreign key before the
			// column modifications (issue #694); do not drop it a second time.
			continue
		}
		nameIdentity := semantics.IndexIdentityKey(info.Name)
		if _, added := addedBareNames[nameIdentity]; added && addedHostCounts[nameIdentity] == 0 {
			// Hostless re-add: addNewConstraints already dropped every
			// recorded removal host for this name.
			continue
		}
		if _, droppedTable := droppedTables[semantics.QualifiedTableIdentityKey(info.TableName)]; droppedTable &&
			!strings.EqualFold(info.Type, "FOREIGN KEY") {
			continue
		}
		result = p.appendScopedDrop(result, info, dropped, semantics)
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
func (p *Planner) dropConstraintNode(info difftypes.ConstraintRemovalInfo) ast.Node {
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

// convertConstraintToAST converts a schemamodel.Constraint to an ast.ConstraintNode for MySQL.
//
// This helper method handles the conversion between the schema annotation representation
// and the AST representation used for SQL generation, taking into account MySQL-specific
// limitations and syntax differences.
func (p *Planner) convertConstraintToAST(constraint schemamodel.Constraint) *ast.ConstraintNode {
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

// previousColumnDefault returns the default a column carried before the change.
//
// The comparison records the change under one of two keys -- "default" for a
// literal and "default_expr" for an expression -- so both are read. An empty
// answer means the column had no default, which is a different fact from not
// knowing; columnDefaultChanged carries that one.
func previousColumnDefault(changes map[string]string) string {
	for _, key := range []string{"default", "default_expr"} {
		if change, present := changes[key]; present {
			before, _, ok := strings.Cut(change, " -> ")
			if ok {
				return strings.TrimSpace(before)
			}
		}
	}
	return ""
}

// columnDefaultChanged reports whether the comparison recorded a default change
// at all.
func columnDefaultChanged(changes map[string]string) bool {
	_, literal := changes["default"]
	_, expression := changes["default_expr"]
	return literal || expression
}

func previousColumnNullable(change string) bool {
	before, _, ok := strings.Cut(change, " -> ")
	return ok && strings.TrimSpace(before) == "true"
}

// changedMaterializedViewNames lists every materialized view the diff touches,
// sorted and deduplicated so the refusal reads the same on every run.
func changedMaterializedViewNames(diff *difftypes.SchemaDiff) []string {
	names := slices.Concat(diff.MaterializedViewsAdded.Names(), diff.MaterializedViewsRemoved.Names())
	for _, view := range diff.MaterializedViewsModified {
		names = append(names, view.ViewName)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

package mysqllike

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer/dialects/internal/bufwriter"
	"github.com/stokaro/ptah/core/renderer/types"
)

var (
	_ types.RenderVisitor = (*Renderer)(nil)
)

// Renderer provides MySQL-like-specific SQL rendering
type Renderer struct {
	dialect      string
	dialectUpper string
	w            *bufwriter.Writer
	// caps describes what the target dialect line actually accepts. The
	// renderer is the VALIDITY layer of the capability model (issue #226): a
	// planner records intent on AST nodes (e.g. IfExists), and the renderer
	// drops any modifier the concrete target would reject — MySQL 8/9 reject
	// IF EXISTS on constraint and index drops, MariaDB accepts both.
	caps capability.Capabilities
}

// New creates a new MySQL-like renderer. The target capabilities are resolved
// from the dialect name (capability.ForDialect), so "mysql" gets the strict
// MySQL preset and "mariadb" the MariaDB one.
func New(dialect string, buf *bufwriter.Writer) *Renderer {
	return &Renderer{
		w:            buf,
		dialect:      dialect,
		dialectUpper: strings.ToUpper(dialect),
		caps:         capability.ForDialect(dialect),
	}
}

// escapeValue properly escapes a string value for use in SQL
func (r *Renderer) escapeValue(value string) string {
	// Escape single quotes by doubling them (MySQL/MariaDB standard)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

// dropConstraintSQL renders a single ALTER TABLE constraint drop.
//
// MySQL/MariaDB constraint drops are type-specific. Foreign keys use the
// dedicated DROP FOREIGN KEY spelling — the one form valid across the entire
// family, including servers that predate the generic DROP CONSTRAINT clause
// (current lines happen to accept the generic clause for FKs too — verified
// live on MySQL 9.7 and MariaDB 10.11 — but there is no reason to give up the
// universal spelling). CHECK constraints on a target without the generic
// clause use DROP CHECK; everything else uses DROP CONSTRAINT.
//
// This is the VALIDITY half of the capability model (issue #226): the planner
// records intent, and the renderer resolves modifiers and spellings against
// ITS target set —
//   - the IF EXISTS guard is MariaDB-only within this family (MySQL rejects
//     it on every constraint-drop spelling), so it renders only when
//     capability.DropConstraintIfExists is present;
//   - the DROP CHECK spelling (op.Check, requested by planners for MySQL
//     8.0.16–8.0.18) exists only on MySQL (capability.DropCheckClause) —
//     MariaDB rejects it (verified live on 10.11), so a stray Check flag
//     reaching a MariaDB renderer degrades to the generic clause, which every
//     CHECK-capable MariaDB accepts.
func (r *Renderer) dropConstraintSQL(table string, op *ast.DropConstraintOperation) string {
	dropSQL := fmt.Sprintf("ALTER TABLE %s DROP", table)
	guarded := op.IfExists && r.caps.Has(capability.DropConstraintIfExists)
	switch {
	case op.ForeignKey:
		dropSQL += " FOREIGN KEY"
		if guarded {
			dropSQL += " IF EXISTS"
		}
	case op.Check && r.caps.Has(capability.DropCheckClause):
		dropSQL += " CHECK"
	case op.Unique:
		// ALTER TABLE ... DROP INDEX drops a UNIQUE constraint's backing
		// index and is valid across the entire MySQL/MariaDB family, so the
		// planner-requested spelling needs no capability gate here. The
		// IF EXISTS guard on this spelling is MariaDB-only (verified live:
		// MariaDB 10.11 accepts it, incl. on an absent index; MySQL 9.7
		// rejects it), so it is gated on the index-drop guard capability.
		dropSQL += " INDEX"
		if op.IfExists && r.caps.Has(capability.DropIndexIfExists) {
			dropSQL += " IF EXISTS"
		}
	case guarded:
		dropSQL += " CONSTRAINT IF EXISTS"
	default:
		dropSQL += " CONSTRAINT"
	}
	return dropSQL + " " + op.ConstraintName
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	// Build DROP INDEX statement for MySQL/MariaDB
	var parts []string
	parts = append(parts, "DROP INDEX")

	// The IF EXISTS guard on DROP INDEX is MariaDB-only (10.1.4+); MySQL has
	// no such form and rejects it. Planners record the guard intent per THEIR
	// capability set (capability.DropIndexIfExists); the renderer validates
	// it again against its own target set, so the guard reaches the SQL only
	// when both layers agree (issue #226).
	if node.IfExists && r.caps.Has(capability.DropIndexIfExists) {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, node.Name)

	// MySQL/MariaDB requires table name in DROP INDEX
	if node.Table != "" {
		parts = append(parts, "ON", node.Table)
	}

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	// MySQL/MariaDB doesn't support separate type definitions
	// Enums are handled inline in column definitions
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("-- %s does not support CREATE TYPE - enums are handled inline in column definitions", r.dialectUpper)
	return nil
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	// MySQL/MariaDB doesn't support ALTER TYPE operations
	// Type changes are handled through ALTER TABLE MODIFY COLUMN
	r.w.WriteLinef("-- %s does not support ALTER TYPE - type changes are handled through ALTER TABLE MODIFY COLUMN", r.dialectUpper)
	return nil
}

func (r *Renderer) Dialect() string {
	return r.dialect
}

func (r *Renderer) Reset() {
	r.w.Reset()
}

func (r *Renderer) Output() string {
	return r.w.Output()
}

// Render renders an AST node to SQL and returns the result
func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		return "", err
	}
	return r.Output(), nil
}

// GetDialect returns the database dialect (alias for Dialect for compatibility)
func (r *Renderer) GetDialect() string {
	return r.Dialect()
}

// GetOutput returns the current generated SQL output (alias for Output for compatibility)
func (r *Renderer) GetOutput() string {
	return r.Output()
}

// VisitCreateTable renders MariaDB-specific CREATE TABLE statements
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	// Table comment
	if node.Comment != "" {
		r.w.WriteLinef("-- %s TABLE: %s (%s) --", r.dialectUpper, node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- %s TABLE: %s --", r.dialectUpper, node.Name)
	}

	// CREATE TABLE statement
	r.w.WriteLinef("CREATE TABLE %s (", node.Name)

	var lines []string

	// Render columns
	for _, column := range node.Columns {
		line, err := r.renderColumn(column)
		if err != nil {
			return fmt.Errorf("error rendering column %s: %w", column.Name, err)
		}
		lines = append(lines, line)
	}

	// Render table-level constraints
	for _, constraint := range node.Constraints {
		line, err := r.renderConstraint(constraint)
		if err != nil {
			return fmt.Errorf("error rendering constraint: %w", err)
		}
		if line != "" {
			lines = append(lines, line)
		}
	}

	// Join all lines
	for i, line := range lines {
		if i == len(lines)-1 {
			r.w.WriteLine(line) // Last line without comma
		} else {
			r.w.WriteLinef("%s,", line)
		}
	}

	r.w.Write(")")

	// Close table definition with MariaDB-specific options
	if len(node.Options) > 0 {
		options := r.renderTableOptions(node.Options)
		if options != "" {
			r.w.Write(" ")
			r.w.Write(options)
		}
	}

	r.w.WriteLine(";")
	r.w.WriteLine("")

	// Only one newline instead of two for better spacing
	return nil
}

// VisitAlterTable renders MariaDB-specific ALTER TABLE statements
func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	return r.visitAlterTableWithEnums(node, nil)
}

// VisitColumn is called when visiting individual columns (used by other visitors)
func (r *Renderer) VisitColumn(node *ast.ColumnNode) error {
	// This is typically called from within other visitors
	// The actual rendering is done by RenderColumn
	return nil
}

// VisitConstraint is called when visiting individual constraints (used by other visitors)
func (r *Renderer) VisitConstraint(node *ast.ConstraintNode) error {
	// This is typically called from within other visitors
	// The actual rendering is done by RenderConstraint
	return nil
}

// VisitIndex renders a CREATE INDEX statement for MySQL
func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	var parts []string

	parts = append(parts, "CREATE")

	if node.Unique {
		parts = append(parts, "UNIQUE")
	}

	parts = append(parts, "INDEX")
	parts = append(parts, node.Name)
	parts = append(parts, "ON")
	parts = append(parts, node.Table)
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(node.Columns, ", ")))

	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitEnum renders enum handling for MariaDB (inline ENUM types like MySQL)
func (r *Renderer) VisitEnum(node *ast.EnumNode) error {
	// MariaDB doesn't have separate enum types like PostgreSQL
	// Enums are defined inline in column definitions like MySQL
	// So this method doesn't render anything for MariaDB
	return nil
}

// VisitComment renders a comment
func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	r.w.WriteLinef("-- %s --", node.Text)
	return nil
}

// VisitDropTable renders MariaDB-specific DROP TABLE statements
func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	// Build DROP TABLE statement with MariaDB-specific features
	var parts []string
	parts = append(parts, "DROP TABLE")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, node.Name)

	// MariaDB doesn't support CASCADE for DROP TABLE like PostgreSQL
	// Ignore the Cascade flag for MariaDB

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

// VisitDropType renders DROP TYPE statements for MariaDB
func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	// MariaDB doesn't have separate enum types like PostgreSQL
	// This operation is not applicable for MariaDB, so we just add a comment
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("-- MariaDB does not support DROP TYPE - enums are handled inline in column definitions")
	return nil
}

// VisitCreateView renders a CREATE VIEW statement for MySQL/MariaDB.
func (r *Renderer) VisitCreateView(node *ast.CreateViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	create := "CREATE VIEW"
	if node.Replace {
		create = "CREATE OR REPLACE VIEW"
	}
	r.w.WriteLinef("%s %s AS", create, node.Name)
	r.w.WriteLine(strings.TrimSpace(node.Body))
	if node.WithCheck {
		r.w.WriteLine("WITH CHECK OPTION")
	}
	r.w.WriteLine(";")
	return nil
}

// VisitDropView renders a DROP VIEW statement for MySQL/MariaDB.
func (r *Renderer) VisitDropView(node *ast.DropViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP VIEW"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, node.Name)
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateMaterializedView renders an explicit unsupported warning.
func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("-- %s does not support CREATE MATERIALIZED VIEW %s", r.dialectUpper, node.Name)
	return nil
}

// VisitDropMaterializedView renders an explicit unsupported warning.
func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("-- %s does not support DROP MATERIALIZED VIEW %s", r.dialectUpper, node.Name)
	return nil
}

// VisitRefreshMaterializedView renders an explicit unsupported warning.
func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("-- %s does not support REFRESH MATERIALIZED VIEW %s", r.dialectUpper, node.Name)
	return nil
}

// VisitCreateTrigger renders a CREATE TRIGGER statement for MySQL/MariaDB.
func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.Replace && !r.caps.Has(capability.CreateOrReplaceTrigger) {
		r.w.WriteLinef("DROP TRIGGER IF EXISTS %s;", node.Name)
	}
	create := "CREATE TRIGGER"
	if node.Replace && r.caps.Has(capability.CreateOrReplaceTrigger) {
		create = "CREATE OR REPLACE TRIGGER"
	}
	r.w.WriteLinef("%s %s %s %s ON %s FOR EACH ROW %s;",
		create, node.Name, node.Timing, node.Event, node.Table, strings.TrimSpace(node.Body))
	return nil
}

// VisitDropTrigger renders a DROP TRIGGER statement for MySQL/MariaDB.
func (r *Renderer) VisitDropTrigger(node *ast.DropTriggerNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP TRIGGER"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, node.Name)
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// RenderColumn renders a column definition
func (r *Renderer) renderColumn(column *ast.ColumnNode) (string, error) {
	var parts []string

	// Column name and type
	parts = append(parts, fmt.Sprintf("  %s %s", column.Name, column.Type))

	// Column constraints
	if column.Primary {
		parts = append(parts, "PRIMARY KEY")
	} else {
		if !column.Nullable {
			parts = append(parts, "NOT NULL")
		}
		if column.Unique {
			parts = append(parts, "UNIQUE")
		}
	}

	// Auto increment (dialect-specific)
	if column.AutoInc {
		parts = append(parts, r.renderAutoIncrement())
	}

	// Default value
	switch {
	case column.Default == nil:
		// No default value
	case column.Default.Value != "":
		parts = append(parts, fmt.Sprintf("DEFAULT %s", r.escapeValue(column.Default.Value)))
	case column.Default.Expression != "":
		parts = append(parts, fmt.Sprintf("DEFAULT %s", column.Default.Expression))
	}

	// Check constraint. When `check_name=` is provided, emit the explicit
	// `CONSTRAINT <name> CHECK (...)` form so the constraint round-trips
	// stably through MySQL/MariaDB introspection (which otherwise auto-names
	// CHECKs as `<table>_chk_N` and would not match the drift detector's
	// expected name).
	if column.Check != "" {
		if column.CheckName != "" {
			parts = append(parts, fmt.Sprintf("CONSTRAINT %s CHECK (%s)", column.CheckName, column.Check))
		} else {
			parts = append(parts, fmt.Sprintf("CHECK (%s)", column.Check))
		}
	}

	return strings.Join(parts, " "), nil
}

// renderAutoIncrement renders auto increment (dialect-specific, override in subclasses)
func (r *Renderer) renderAutoIncrement() string {
	return "AUTO_INCREMENT" // Default MySQL/MariaDB style
}

// renderTableOptions renders MariaDB table options (same as MySQL)
func (r *Renderer) renderTableOptions(options map[string]string) string {
	var parts []string
	for key, value := range options {
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(parts, " ")
}

// renderConstraint renders a table-level constraint
func (r *Renderer) renderConstraint(constraint *ast.ConstraintNode) (string, error) {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(constraint.Columns, ", ")), nil
	case ast.UniqueConstraint:
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s UNIQUE (%s)", constraint.Name, strings.Join(constraint.Columns, ", ")), nil
		}
		return fmt.Sprintf("  UNIQUE (%s)", strings.Join(constraint.Columns, ", ")), nil
	case ast.ForeignKeyConstraint:
		return r.renderForeignKeyConstraint(constraint)
	case ast.CheckConstraint:
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", constraint.Name, constraint.Expression), nil
		}
		return fmt.Sprintf("  CHECK (%s)", constraint.Expression), nil
	default:
		return "", fmt.Errorf("unknown constraint type: %v", constraint.Type)
	}
}

// renderForeignKeyConstraint renders a foreign key constraint
func (r *Renderer) renderForeignKeyConstraint(constraint *ast.ConstraintNode) (string, error) {
	if constraint.Reference == nil {
		return "", fmt.Errorf("foreign key constraint missing reference")
	}

	ref := constraint.Reference
	result := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		constraint.Name,
		strings.Join(constraint.Columns, ", "),
		ref.Table,
		ref.Column)

	if ref.OnDelete != "" {
		result += fmt.Sprintf(" ON DELETE %s", ref.OnDelete)
	}

	if ref.OnUpdate != "" {
		result += fmt.Sprintf(" ON UPDATE %s", ref.OnUpdate)
	}

	return result, nil
}

// renderColumnWithEnums renders a column with enum support for MariaDB
func (r *Renderer) renderColumnWithEnums(column *ast.ColumnNode, enumValues []string) (string, error) {
	var parts []string

	// Handle enum types inline for MariaDB
	columnType := column.Type
	if len(enumValues) > 0 {
		// Convert to MariaDB ENUM syntax
		quotedValues := make([]string, len(enumValues))
		for i, value := range enumValues {
			quotedValues[i] = r.escapeValue(value)
		}
		columnType = fmt.Sprintf("ENUM(%s)", strings.Join(quotedValues, ", "))
	}

	// Column name and type
	parts = append(parts, fmt.Sprintf("  %s %s", column.Name, columnType))

	// Column constraints - MariaDB order: PRIMARY KEY, then NOT NULL, then UNIQUE
	if column.Primary {
		parts = append(parts, "PRIMARY KEY")
		if column.AutoInc {
			parts = append(parts, r.renderAutoIncrement())
		}
	} else {
		if !column.Nullable {
			parts = append(parts, "NOT NULL")
		}
		if column.Unique {
			parts = append(parts, "UNIQUE")
		}
		if column.AutoInc {
			parts = append(parts, r.renderAutoIncrement())
		}
	}

	// Default values
	if column.Default != nil {
		if column.Default.Expression != "" {
			parts = append(parts, fmt.Sprintf("DEFAULT %s", column.Default.Expression))
		} else if column.Default.Value != "" {
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", column.Default.Value))
		}
	}

	// Check constraints
	if column.Check != "" {
		parts = append(parts, fmt.Sprintf("CHECK (%s)", column.Check))
	}

	// Comments
	if column.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", column.Comment))
	}

	return strings.Join(parts, " "), nil
}

// VisitAlterTableWithEnums renders MariaDB-specific ALTER TABLE statements with enum support
func (r *Renderer) visitAlterTableWithEnums(node *ast.AlterTableNode, enums map[string][]string) error {
	r.w.WriteLine("-- ALTER statements: --")

	for _, operation := range node.Operations {
		switch op := operation.(type) {
		case *ast.AddColumnOperation:
			// Get enum values for this column type
			var enumValues []string
			if enums != nil {
				enumValues = enums[op.Column.Type]
			}

			line, err := r.renderColumnWithEnums(op.Column, enumValues)
			if err != nil {
				return fmt.Errorf("error rendering add column: %w", err)
			}
			// Remove the leading spaces from column rendering for ALTER
			line = strings.TrimPrefix(line, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", node.Name, line)

		case *ast.AddConstraintOperation:
			constraintLine, err := r.renderConstraint(op.Constraint)
			if err != nil {
				return fmt.Errorf("error rendering add constraint: %w", err)
			}
			// Remove the leading spaces from constraint rendering for ALTER
			constraintLine = strings.TrimPrefix(constraintLine, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", node.Name, constraintLine)

		case *ast.DropConstraintOperation:
			r.w.WriteLinef("%s;", r.dropConstraintSQL(node.Name, op))

		case *ast.DropColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP COLUMN %s;", node.Name, op.ColumnName)

		case *ast.ModifyColumnOperation:
			// Get enum values for this column type
			var enumValues []string
			if enums != nil {
				enumValues = enums[op.Column.Type]
			}

			// MariaDB uses MODIFY COLUMN syntax like MySQL
			line, err := r.renderColumnWithEnums(op.Column, enumValues)
			if err != nil {
				return fmt.Errorf("error rendering modify column: %w", err)
			}
			// Remove the leading spaces from column rendering for ALTER
			line = strings.TrimPrefix(line, "  ")
			r.w.WriteLinef("ALTER TABLE %s MODIFY COLUMN %s;", node.Name, line)

		case *ast.RenameColumnOperation:
			// MySQL 8.0+ and MariaDB 10.5.2+ both support the canonical
			// `ALTER TABLE x RENAME COLUMN old TO new` form. The runtime
			// version is the caller's concern; older servers will fail at
			// migration apply time rather than at SQL generation time.
			r.w.WriteLinef("ALTER TABLE %s RENAME COLUMN %s TO %s;", node.Name, op.OldName, op.NewName)

		case *ast.AddSkippingIndexOperation:
			// Data-skipping indexes are a ClickHouse-specific construct; no
			// MySQL/MariaDB equivalent exists. Emit a self-explanatory
			// comment so the migration is still readable and diffable.
			r.w.WriteLinef("-- %s: data-skipping indexes are ClickHouse-specific; ignored.", r.dialectUpper)

		case *ast.ModifyTTLOperation:
			// Table TTL (row expiration) is a ClickHouse-only feature.
			r.w.WriteLinef("-- %s: table TTL is ClickHouse-specific; ignored.", r.dialectUpper)

		default:
			return fmt.Errorf("unknown alter operation type: %T", operation)
		}
	}

	r.w.WriteLine("")
	return nil
}

// VisitExtension renders CREATE EXTENSION statements for MySQL-like databases (no-op)
func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	// MySQL-like databases don't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- Extension %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- Extension %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitDropExtension renders DROP EXTENSION statements for MySQL-like databases (no-op)
func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	// MySQL-like databases don't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitCreateFunction renders CREATE FUNCTION statements for MySQL-like databases (no-op)
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	// MySQL-like databases don't support PostgreSQL-style functions
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitCreatePolicy renders CREATE POLICY statements for MySQL-like databases (no-op)
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	// MySQL-like databases don't support Row-Level Security policies
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitAlterTableEnableRLS renders ALTER TABLE ENABLE RLS statements for MySQL-like databases (no-op)
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	// MySQL-like databases don't support Row-Level Security
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in %s: %s", node.Table, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in %s", node.Table, r.dialect)
	}
	return nil
}

// VisitDropFunction returns an error since PostgreSQL functions are not supported in MySQL
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	return fmt.Errorf("DROP FUNCTION is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitDropPolicy returns an error since RLS policies are not supported in MySQL
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	return fmt.Errorf("DROP POLICY is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitAlterTableDisableRLS returns an error since RLS is not supported in MySQL
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	return fmt.Errorf("ALTER TABLE DISABLE ROW LEVEL SECURITY is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitCreateRole returns an error since PostgreSQL roles are not supported in MySQL
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	return fmt.Errorf("CREATE ROLE is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitDropRole returns an error since PostgreSQL roles are not supported in MySQL
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	return fmt.Errorf("DROP ROLE is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitAlterRole returns an error since PostgreSQL roles are not supported in MySQL
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	return fmt.Errorf("ALTER ROLE is not supported in %s (PostgreSQL-specific feature)", r.dialectUpper)
}

// VisitRawSQL refuses to emit raw SQL. The only emitter of RawSQLNode today
// is the PostgreSQL planner's constraint-drop path, which produces a PG-
// specific DO block; routing that through a MySQL-family renderer would
// produce a migration the target server can't execute. If a future caller
// genuinely needs dialect-neutral raw SQL it should add a typed escape hatch
// rather than relying on this method.
func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	return fmt.Errorf("RawSQLNode is not supported in %s: %q was emitted by a Postgres-specific planner path", r.dialectUpper, node.SQL)
}

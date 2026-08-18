// Package mysqllike implements the shared SQL renderer for the MySQL family of
// dialects. It is the validity layer of the capability model: planners record
// intent on AST nodes, and this renderer drops modifiers the concrete target
// (MySQL or MariaDB) would reject.
package mysqllike

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/tableref"
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
	return NewWithCapabilities(dialect, buf, capability.ForDialect(dialect))
}

// NewWithCapabilities creates a new MySQL-like renderer for a concrete target
// capability set. Live database paths should pass the set resolved from the
// server version; offline paths use New and therefore the dialect default.
func NewWithCapabilities(dialect string, buf *bufwriter.Writer, caps capability.Capabilities) *Renderer {
	return &Renderer{
		w:            buf,
		dialect:      dialect,
		dialectUpper: strings.ToUpper(dialect),
		caps:         caps.Clone(),
	}
}

// escapeValue properly escapes a string value for use in SQL
func (r *Renderer) escapeValue(value string) string {
	// Escape single quotes by doubling them (MySQL/MariaDB standard)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func (r *Renderer) renderDefaultLiteral(column *ast.ColumnNode) string {
	value := strings.TrimSpace(column.Default.Value)
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value
	}
	if literal, ok := renderBooleanDefaultLiteral(column.Type, value); ok {
		return literal
	}
	if value == "" || mysqlDefaultNeedsLiteralQuotes(column.Type, value) {
		return r.escapeValue(column.Default.Value)
	}
	return column.Default.Value
}

func renderBooleanDefaultLiteral(columnType, value string) (string, bool) {
	if !isMySQLBooleanType(columnType) {
		return "", false
	}
	switch strings.ToLower(value) {
	case "false":
		return "0", true
	case "true":
		return "1", true
	default:
		return "", false
	}
}

func isMySQLBooleanType(columnType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(columnType))
	return normalized == "bool" ||
		normalized == "boolean" ||
		strings.HasPrefix(normalized, "tinyint(1)")
}

func mysqlDefaultNeedsLiteralQuotes(columnType, value string) bool {
	if isMySQLNullDefault(value) || isMySQLTemporalDefaultExpression(columnType, value) {
		return false
	}

	normalizedType := strings.ToLower(strings.TrimSpace(columnType))
	switch {
	case strings.HasPrefix(normalizedType, "enum("), strings.HasPrefix(normalizedType, "set("):
		return true
	case strings.Contains(normalizedType, "char"), strings.Contains(normalizedType, "text"):
		return true
	case strings.Contains(normalizedType, "binary"), strings.Contains(normalizedType, "blob"), normalizedType == "json":
		return true
	case strings.Contains(normalizedType, "date"), strings.Contains(normalizedType, "time"), strings.Contains(normalizedType, "year"):
		return true
	default:
		return false
	}
}

func isMySQLNullDefault(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "NULL")
}

func isMySQLTemporalDefaultExpression(columnType, value string) bool {
	normalizedType := strings.ToLower(strings.TrimSpace(columnType))
	if !strings.Contains(normalizedType, "date") && !strings.Contains(normalizedType, "time") && !strings.Contains(normalizedType, "year") {
		return false
	}

	normalizedValue := strings.ToUpper(strings.TrimSpace(value))
	normalizedValue = strings.TrimSuffix(normalizedValue, "()")
	switch normalizedValue {
	case "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP", "NOW":
		return true
	default:
		return strings.HasPrefix(normalizedValue, "CURRENT_TIMESTAMP(") ||
			strings.HasPrefix(normalizedValue, "CURRENT_TIME(") ||
			strings.HasPrefix(normalizedValue, "LOCALTIME(") ||
			strings.HasPrefix(normalizedValue, "LOCALTIMESTAMP(") ||
			strings.HasPrefix(normalizedValue, "NOW(")
	}
}

func escapeIdentifier(identifier string) string {
	return escapeIdentifierValue(unquoteIdentifier(identifier))
}

func escapeIdentifierValue(identifier string) string {
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return "`" + escaped + "`"
}

func escapeQualifiedIdentifier(identifier string) string {
	parts := splitQualifiedIdentifier(identifier)
	for i, part := range parts {
		parts[i] = escapeIdentifierValue(part)
	}
	return strings.Join(parts, ".")
}

func escapeIdentifierList(identifiers []string) []string {
	escaped := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		escaped[i] = escapeQualifiedIdentifier(identifier)
	}
	return escaped
}

func unquoteIdentifier(identifier string) string {
	if len(identifier) < 2 {
		return identifier
	}
	switch {
	case identifier[0] == '`' && identifier[len(identifier)-1] == '`':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`")
	case identifier[0] == '"' && identifier[len(identifier)-1] == '"':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
	case identifier[0] == '[' && identifier[len(identifier)-1] == ']':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "]]", "]")
	}
	return identifier
}

func splitQualifiedIdentifier(identifier string) []string {
	ref, ok := tableref.Parse(identifier)
	if !ok {
		return []string{identifier}
	}
	if !ref.Qualified {
		return []string{ref.Name}
	}
	return []string{ref.Schema, ref.Name}
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
	dropSQL := fmt.Sprintf("ALTER TABLE %s DROP", escapeQualifiedIdentifier(table))
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
	case op.PrimaryKey:
		dropSQL += " PRIMARY KEY"
		return dropSQL
	case guarded:
		dropSQL += " CONSTRAINT IF EXISTS"
	default:
		dropSQL += " CONSTRAINT"
	}
	return dropSQL + " " + escapeIdentifier(op.ConstraintName)
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

	parts = append(parts, escapeIdentifier(node.Name))

	// MySQL/MariaDB requires table name in DROP INDEX
	if node.Table != "" {
		parts = append(parts, "ON", escapeQualifiedIdentifier(node.Table))
	}

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

// VisitCreateType names the user-defined type Ptah does not generate for this
// target. MySQL and MariaDB have no CREATE TYPE object at all; an enum lives in
// the column definition and reaches this renderer that way, never as a node.
//
// The diagnostic used to name no object -- "MYSQL does not support CREATE TYPE -
// enums are handled inline in column definitions" -- which was survivable only
// while the converter dropped domain, composite and range nodes before this
// renderer saw one. It does not any more, so a schema declaring three domains
// produced three identical lines naming none of them, and the sentence was about
// enums while the node was a domain (stokaro/ptah#929 item 5).
func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.notGenerated("CREATE TYPE", node.Name)
	return nil
}

// VisitCreateSchema renders a CREATE SCHEMA statement.
func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}
	var parts []string
	parts = append(parts, "CREATE SCHEMA"+guard, escapeIdentifier(node.Name))
	if node.Charset != "" {
		parts = append(parts, "DEFAULT CHARACTER SET", node.Charset)
	}
	if node.Collate != "" {
		parts = append(parts, "COLLATE", node.Collate)
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateDatabase renders a CREATE DATABASE statement.
func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}
	r.w.WriteLinef("CREATE DATABASE%s %s;", guard, escapeIdentifier(node.Name))
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

func (r *Renderer) VisitUpsert(_ *ast.UpsertNode) error {
	return fmt.Errorf("%w: %s: upsert rendering is not implemented", ptaherr.ErrUnsupportedFeature, r.dialect)
}

// VisitCreateTable renders MariaDB-specific CREATE TABLE statements
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	// Table comment
	if node.Comment != "" {
		r.w.WriteLinef("-- %s TABLE: %s (%s) --", r.dialectUpper, node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- %s TABLE: %s --", r.dialectUpper, node.Name)
	}

	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}

	if len(node.Columns) == 0 && len(node.Constraints) == 0 && node.SelectBody != "" {
		r.w.Writef("CREATE TABLE%s %s", guard, escapeQualifiedIdentifier(node.Name))
		if len(node.Options) > 0 {
			options := r.renderTableOptions(node.Options)
			if options != "" {
				r.w.Write(" ")
				r.w.Write(options)
			}
		}
		r.w.WriteLinef(" %s;", strings.TrimSpace(node.SelectBody))
		r.w.WriteLine("")
		return nil
	}

	// CREATE TABLE statement
	r.w.WriteLinef("CREATE TABLE%s %s (", guard, escapeQualifiedIdentifier(node.Name))

	var lines []string

	// Render columns
	for _, column := range node.Columns {
		line, err := r.renderColumn(column)
		if err != nil {
			return fmt.Errorf("error rendering column %s: %w", column.Name, err)
		}
		lines = append(lines, line)
	}

	for _, column := range node.Columns {
		if !r.rendersNamedColumnCheckAsTableConstraint(column) {
			continue
		}
		lines = append(lines, fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", escapeIdentifier(column.CheckName), column.Check))
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

	if node.SelectBody != "" {
		r.w.Write(" ")
		r.w.Write(strings.TrimSpace(node.SelectBody))
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

	if indexType := mysqlIndexPrefixType(node.Type); indexType != "" {
		parts = append(parts, indexType)
	}

	parts = append(parts, "INDEX")
	parts = append(parts, escapeIdentifier(node.Name))
	parts = append(parts, "ON")
	parts = append(parts, escapeQualifiedIdentifier(node.Table))
	columnSpec := fmt.Sprintf("(%s)", strings.Join(renderIndexParts(node.EffectiveParts()), ", "))
	if node.Parser != "" {
		columnSpec += fmt.Sprintf(" /*!50100 WITH PARSER %s */", escapeIdentifier(node.Parser))
	}
	parts = append(parts, columnSpec)

	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func renderIndexParts(parts []ast.IndexPart) []string {
	specs := make([]string, 0, len(parts))
	for _, part := range parts {
		spec := escapeQualifiedIdentifier(part.Reference())
		if part.Expr != "" {
			spec = fmt.Sprintf("(%s)", part.Expr)
		}
		if part.Prefix != "" && part.Expr == "" {
			spec += " (" + part.Prefix + ")"
		}
		if part.Desc {
			spec += " DESC"
		}
		specs = append(specs, spec)
	}
	return specs
}

func mysqlIndexPrefixType(indexType string) string {
	normalized := strings.ToUpper(strings.TrimSpace(indexType))
	switch normalized {
	case "FULLTEXT", "SPATIAL":
		return normalized
	default:
		return ""
	}
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

	parts = append(parts, strings.Join(escapeIdentifierList(node.TableNames()), ", "))

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
	r.w.WriteLinef("%s %s AS", create, escapeQualifiedIdentifier(node.Name))
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
	parts = append(parts, escapeQualifiedIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateMaterializedView refuses: MySQL and MariaDB have no materialized
// view object.
//
// This used to render a comment. A comment makes `schema render` exit 0 on a
// model the planner refuses at `schema apply` time, so the surface a user is
// told to validate with disagreed with the surface that executes. Refusing here
// makes them agree, and matches how SQLite already answers the same input.
func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	return r.materializedViewsUnsupported("CREATE MATERIALIZED VIEW", node.Name)
}

// VisitDropMaterializedView refuses for the same reason as
// VisitCreateMaterializedView.
func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	return r.materializedViewsUnsupported("DROP MATERIALIZED VIEW", node.Name)
}

// VisitRefreshMaterializedView refuses for the same reason as
// VisitCreateMaterializedView.
func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	return r.materializedViewsUnsupported("REFRESH MATERIALIZED VIEW", node.Name)
}

func (r *Renderer) materializedViewsUnsupported(statement, name string) error {
	return fmt.Errorf("%w: %s: %s %s: materialized views are not supported by MySQL or MariaDB; remove matview definitions for this target",
		ptaherr.ErrUnsupportedFeature, r.dialect, statement, name)
}

// VisitCreateTrigger renders a CREATE TRIGGER statement for MySQL/MariaDB.
//
// MySQL and MariaDB have row-level triggers only. A FOR EACH STATEMENT trigger
// is refused rather than rendered as FOR EACH ROW: silently changing the level
// makes the trigger fire once per affected row instead of once per statement,
// which is a different program. SQLite already refuses the same input, and this
// matches it.
func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	forEach := strings.ToUpper(strings.TrimSpace(node.ForEach))
	if forEach == "" {
		forEach = "ROW"
	}
	if forEach != "ROW" {
		return fmt.Errorf("%w: %s: FOR EACH %s triggers are not supported", ptaherr.ErrUnsupportedFeature, r.dialect, forEach)
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.Replace && !r.caps.Has(capability.CreateOrReplaceTrigger) {
		r.w.WriteLinef("DROP TRIGGER IF EXISTS %s;", escapeIdentifier(node.Name))
	}
	create := "CREATE TRIGGER"
	if node.Replace && r.caps.Has(capability.CreateOrReplaceTrigger) {
		create = "CREATE OR REPLACE TRIGGER"
	}
	r.w.WriteLinef("%s %s %s %s ON %s FOR EACH ROW %s",
		create, escapeIdentifier(node.Name), node.Timing, node.Event, escapeQualifiedIdentifier(node.Table), terminateStatement(node.Body))
	return nil
}

// terminateStatement returns body with exactly one trailing semicolon. A body
// annotation is naturally spelled as a complete SQL statement ending in ";",
// and appending another one unconditionally produced ";;".
func terminateStatement(body string) string {
	trimmed := strings.TrimSpace(body)
	if strings.HasSuffix(trimmed, ";") {
		return trimmed
	}
	return trimmed + ";"
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
	parts = append(parts, escapeIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// RenderColumn renders a column definition
func (r *Renderer) renderColumn(column *ast.ColumnNode) (string, error) {
	var parts []string

	// Column name and type
	columnType := r.renderColumnType(column, column.Type)
	parts = append(parts, fmt.Sprintf("  %s %s", escapeIdentifier(column.Name), columnType))
	parts = r.appendColumnCharsetCollate(parts, column)

	parts = r.appendColumnMainClauses(parts, column)
	return r.renderColumnTail(parts, column), nil
}

func (r *Renderer) appendColumnMainClauses(parts []string, column *ast.ColumnNode) []string {
	if column.GeneratedExpression != "" {
		parts = append(parts, renderGeneratedColumn(column))
		return appendMySQLColumnConstraints(parts, column)
	}

	parts = appendMySQLColumnConstraints(parts, column)
	if column.AutoInc {
		parts = append(parts, r.renderAutoIncrement())
	}
	return parts
}

func appendMySQLColumnConstraints(parts []string, column *ast.ColumnNode) []string {
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
	return parts
}

func (r *Renderer) renderColumnTail(parts []string, column *ast.ColumnNode) string {
	return strings.Join(r.appendColumnTail(parts, column), " ")
}

func (r *Renderer) appendColumnTail(parts []string, column *ast.ColumnNode) []string {
	// Default value
	switch {
	case column.Default == nil:
		// No default value
	case column.Default.HasLiteral():
		parts = append(parts, fmt.Sprintf("DEFAULT %s", r.renderDefaultLiteral(column)))
	case column.Default.Expression != "":
		parts = append(parts, fmt.Sprintf("DEFAULT %s", column.Default.Expression))
	}
	if column.UpdateExpression != "" {
		parts = append(parts, "ON UPDATE", column.UpdateExpression)
	}

	// Check constraint. When `check_name=` is provided, emit the explicit
	// `CONSTRAINT <name> CHECK (...)` form so the constraint round-trips
	// stably through introspection (which otherwise auto-names CHECKs as
	// `<table>_chk_N` and would not match the drift detector's expected name).
	if column.Check != "" && !r.rendersNamedColumnCheckAsTableConstraint(column) {
		if column.CheckName != "" {
			parts = append(parts, fmt.Sprintf("CONSTRAINT %s CHECK (%s)", escapeIdentifier(column.CheckName), column.Check))
		} else {
			parts = append(parts, fmt.Sprintf("CHECK (%s)", column.Check))
		}
	}
	if r.needsMariaDBJSONCheck(column) {
		parts = append(parts, fmt.Sprintf("CHECK (json_valid(%s))", escapeIdentifier(column.Name)))
	}

	return parts
}

func (r *Renderer) rendersNamedColumnCheckAsTableConstraint(column *ast.ColumnNode) bool {
	return r.dialect == "mariadb" && column.Check != "" && column.CheckName != ""
}

func (r *Renderer) renderColumnType(column *ast.ColumnNode, columnType string) string {
	if r.isMariaDBJSONColumn(column) {
		return "longtext"
	}
	return columnType
}

func (r *Renderer) appendColumnCharsetCollate(parts []string, column *ast.ColumnNode) []string {
	charset := column.Charset
	collate := column.Collate
	if r.isMariaDBJSONColumn(column) {
		if charset == "" {
			charset = "utf8mb4"
		}
		if collate == "" {
			collate = "utf8mb4_bin"
		}
	}
	if charset != "" {
		parts = append(parts, "CHARACTER SET", charset)
	}
	if collate != "" {
		parts = append(parts, "COLLATE", collate)
	}
	return parts
}

func (r *Renderer) isMariaDBJSONColumn(column *ast.ColumnNode) bool {
	return r.dialect == "mariadb" && strings.EqualFold(column.Type, "json")
}

func (r *Renderer) needsMariaDBJSONCheck(column *ast.ColumnNode) bool {
	return r.isMariaDBJSONColumn(column) && !strings.Contains(strings.ToLower(column.Check), "json_valid(")
}

func renderGeneratedColumn(column *ast.ColumnNode) string {
	sql := fmt.Sprintf("GENERATED ALWAYS AS (%s)", column.GeneratedExpression)
	if column.GeneratedKind != "" {
		sql += " " + strings.ToUpper(strings.TrimSpace(column.GeneratedKind))
	}
	return sql
}

// renderAutoIncrement renders auto increment (dialect-specific, override in subclasses)
func (r *Renderer) renderAutoIncrement() string {
	return "AUTO_INCREMENT" // Default MySQL/MariaDB style
}

// renderTableOptions renders MariaDB table options (same as MySQL)
func (r *Renderer) renderTableOptions(options map[string]string) string {
	knownOrder := []string{"ENGINE", "AUTO_INCREMENT", "CHARSET", "COLLATE"}
	parts := make([]string, 0, len(options))
	renderOption := func(key string) {
		if value, ok := options[key]; ok {
			parts = append(parts, fmt.Sprintf("%s=%s", key, value))
		}
	}

	for _, key := range knownOrder {
		renderOption(key)
	}

	var unknownKeys []string
	for key := range options {
		if !slices.Contains(knownOrder, key) {
			unknownKeys = append(unknownKeys, key)
		}
	}
	slices.Sort(unknownKeys)
	for _, key := range unknownKeys {
		renderOption(key)
	}
	return strings.Join(parts, " ")
}

// renderConstraint renders a table-level constraint
func (r *Renderer) renderConstraint(constraint *ast.ConstraintNode) (string, error) {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return fmt.Sprintf("  PRIMARY KEY (%s)", renderMySQLConstraintColumns(constraint)), nil
	case ast.UniqueConstraint:
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s UNIQUE (%s)", escapeIdentifier(constraint.Name), renderMySQLConstraintColumns(constraint)), nil
		}
		return fmt.Sprintf("  UNIQUE (%s)", renderMySQLConstraintColumns(constraint)), nil
	case ast.ForeignKeyConstraint:
		return r.renderForeignKeyConstraint(constraint)
	case ast.CheckConstraint:
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", escapeIdentifier(constraint.Name), constraint.Expression), nil
		}
		return fmt.Sprintf("  CHECK (%s)", constraint.Expression), nil
	default:
		return "", fmt.Errorf("unknown constraint type: %v", constraint.Type)
	}
}

func renderMySQLConstraintColumns(constraint *ast.ConstraintNode) string {
	if len(constraint.ColumnParts) == 0 {
		return strings.Join(escapeIdentifierList(constraint.Columns), ", ")
	}
	parts := make([]string, 0, len(constraint.ColumnParts))
	for _, column := range constraint.ColumnParts {
		part := escapeIdentifier(column.Name)
		if column.Prefix != "" {
			part += " (" + column.Prefix + ")"
		}
		if column.Desc {
			part += " DESC"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

// renderForeignKeyConstraint renders a foreign key constraint
func (r *Renderer) renderForeignKeyConstraint(constraint *ast.ConstraintNode) (string, error) {
	if constraint.Reference == nil {
		return "", fmt.Errorf("foreign key constraint missing reference")
	}

	ref := constraint.Reference
	result := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		escapeIdentifier(constraint.Name),
		strings.Join(escapeIdentifierList(constraint.Columns), ", "),
		escapeQualifiedIdentifier(ref.Table),
		strings.Join(escapeIdentifierList(ref.ReferencedColumns()), ", "))

	if ref.OnDelete != "" {
		result += fmt.Sprintf(" ON DELETE %s", ref.OnDelete)
	}

	if ref.OnUpdate != "" {
		result += fmt.Sprintf(" ON UPDATE %s", ref.OnUpdate)
	}
	if ref.Deferrable || ref.Initially != "" {
		// Neither engine parses the clause at all: measured, `DEFERRABLE
		// INITIALLY DEFERRED` on a foreign key is error 1064 on MySQL 8.4.11
		// and on MariaDB 11.8.8. Rendering the constraint without it would
		// produce one that rejects exactly the writes the author deferred the
		// check for, at apply time on data rather than here on a line of DDL
		// (stokaro/ptah#1624).
		return "", fmt.Errorf(
			"%w: %s does not support DEFERRABLE foreign keys; constraint %q declares one",
			ptaherr.ErrUnsupportedFeature, r.dialect, constraint.Name)
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
	columnType = r.renderColumnType(column, columnType)
	parts = append(parts, fmt.Sprintf("  %s %s", escapeIdentifier(column.Name), columnType))
	parts = r.appendColumnCharsetCollate(parts, column)

	parts = r.appendColumnMainClauses(parts, column)
	parts = r.appendColumnTail(parts, column)

	// Comments
	if column.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT %s", r.escapeValue(column.Comment)))
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
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", escapeQualifiedIdentifier(node.Name), line)

		case *ast.AddConstraintOperation:
			constraintLine, err := r.renderConstraint(op.Constraint)
			if err != nil {
				return fmt.Errorf("error rendering add constraint: %w", err)
			}
			// Remove the leading spaces from constraint rendering for ALTER
			constraintLine = strings.TrimPrefix(constraintLine, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", escapeQualifiedIdentifier(node.Name), constraintLine)

		case *ast.DropConstraintOperation:
			r.w.WriteLinef("%s;", r.dropConstraintSQL(node.Name, op))

		case *ast.DropColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP COLUMN %s;", escapeQualifiedIdentifier(node.Name), escapeIdentifier(op.ColumnName))

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
			r.w.WriteLinef("ALTER TABLE %s MODIFY COLUMN %s;", escapeQualifiedIdentifier(node.Name), line)

		case *ast.RenameColumnOperation:
			// MySQL 8.0+ and MariaDB 10.5.2+ both support the canonical
			// `ALTER TABLE x RENAME COLUMN old TO new` form. The runtime
			// version is the caller's concern; older servers will fail at
			// migration apply time rather than at SQL generation time.
			r.w.WriteLinef("ALTER TABLE %s RENAME COLUMN %s TO %s;",
				escapeQualifiedIdentifier(node.Name), escapeIdentifier(op.OldName), escapeIdentifier(op.NewName))
		case *ast.RenameTableOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME TO %s;", escapeQualifiedIdentifier(node.Name), escapeQualifiedIdentifier(op.NewName))

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

// VisitCreateFunction renders a CREATE FUNCTION statement for MySQL/MariaDB.
//
// # One statement, not two
//
// This renders the CREATE alone. It used to prefix every function with its own
// `DROP FUNCTION IF EXISTS`, because neither engine offers the replace form
// Ptah's PostgreSQL renderer relies on for the same node -- `CREATE OR REPLACE
// FUNCTION f() RETURNS integer DETERMINISTIC RETURN 2` is Error 1064 on MySQL
// 26.7.0 -- so a modified function needed the pair.
//
// Putting the pair in one visitor put two statements in one element of
// [renderer.GetOrderedCreateStatements], and that list is not always split
// before it is executed. [planner.GenerateSchemaDiffSQLStatements] runs
// sqlutil.SplitSQLStatements over its output, which is why the planner path
// worked; the compatibility dev-database path does not. `materializeOnDev`
// passes each element unchanged to ExecuteSQL, and convertMySQLURL does not
// enable go-sql-driver's multiStatements option, so materializing any desired
// schema containing a function failed at the second statement. Measured on
// both engines through dbschema.ConnectToDatabase with the default DSN:
//
//	Error 1064 (42000): ... right syntax to use near
//	'CREATE FUNCTION `p_fn`(a INT) RETURNS int DETERMINISTIC ...' at line 2
//
// The drop a replacement still needs is now a separate node the planner emits
// in front of this one; see planFunctions in the MySQL-family planner. That
// keeps the invariant every other visitor already holds -- one node renders one
// statement -- rather than making one caller compensate for one visitor.
//
// # The characteristic
//
// A characteristic is always emitted. With binary logging on and
// log_bin_trust_function_creators off -- the MySQL 26.7.0 image's own defaults
// -- a function declared without one is refused outright:
//
//	CREATE FUNCTION f() RETURNS integer RETURN 1
//	  -> Error 1418 (HY000): This function has none of DETERMINISTIC, NO SQL,
//	     or READS SQL DATA in its declaration and binary logging is enabled
//
// Which characteristic encodes which volatility, and the measured grid of what
// the server accepts, lives in [mysqlroutine.Characteristic]. It is written
// there rather than here because the reader has to invert it, and the two
// halves drifting apart is what made a declared STABLE function plan the same
// destructive replacement on every apply.
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	if !r.caps.Has(capability.Functions) {
		r.notGenerated("CREATE FUNCTION", node.Name)
		return nil
	}
	// A routine body is written in a language, and MySQL and MariaDB run
	// exactly one: SQL. A function declared LANGUAGE plpgsql is PostgreSQL
	// procedural code, and no envelope makes it run here -- the worked example
	// is `RETURNS VOID ... BEGIN PERFORM set_config(...); END;`, where the
	// return type alone is Error 1064 on MySQL 26.7.0 before the body is even
	// reached.
	//
	// It stays a SKIP rather than becoming a refusal, and that was measured
	// rather than assumed. A refusal breaks a workflow that works today:
	// applying ONE schema across postgres, mysql and mariadb. Ptah has no way to
	// scope a declared object to a dialect -- `//ptah:schema:function` accepts
	// name, params, returns, language, security, volatility, body and comment,
	// and internal/annotationmeta grants `platform.<dialect>.<key>` overrides to
	// exactly three directives (field, embedded, table), none of which is a
	// function. An unknown attribute is a hard parse error, so `platform=` or
	// `dialect=` cannot even be written. Until a declaration can say "this
	// object is PostgreSQL's", refusing here would leave an operator with a
	// multi-dialect schema no way to express what they already express by
	// declaring a plpgsql function and letting non-PostgreSQL targets pass it
	// by. The only alternative available today is `exclude` in ptah.yaml, which
	// is an operator-side filter at invocation, not a property of the
	// declaration.
	//
	// What the message says is new, and it is the part worth keeping. The skip
	// used to name only the language. [goschema.Function.Canonicalize] defaults
	// an UNSET language to plpgsql -- PostgreSQL's default, baked into a
	// dialect-neutral type -- so a function annotated without `language=` lands
	// here too and is skipped when it should have been generated. Measured on
	// MySQL 26.7.0 and MariaDB 12.3.2: `schema apply` exits 0 having created
	// nothing, and the diff asks for the same function forever. That trap costs
	// an afternoon to find, so the comment names it and names the one word that
	// settles it.
	//
	// The message still never blames the engine. `-- CREATE FUNCTION f1 not
	// supported in MySQL` was false because MySQL hosts functions perfectly well
	// (stokaro/ptah#929); this is about the declaration, and it says so.
	// The predicate is mysqlroutine.RunsLanguage rather than a comparison
	// written here, because the MySQL-family planner has to reach the same
	// answer: it must not plan the DROP half of a replacement whose CREATE half
	// this branch is about to skip.
	if !mysqlroutine.RunsLanguage(node.Language) {
		language := strings.ToLower(strings.TrimSpace(node.Language))
		r.w.WriteLinef(
			"-- %s: CREATE FUNCTION %s declares language %s, which this target does not run; skipped.",
			r.dialectUpper, escapeIdentifier(node.Name), language)
		r.w.WriteLinef(
			"--   If this body is SQL, declare language=\"sql\": an annotation that omits the")
		r.w.WriteLinef(
			"--   language is defaulted to plpgsql and is skipped here for the same reason.")
		return nil
	}
	// Both refusals happen before anything is written. A value this target
	// cannot represent must not reach the output at all: the planner emits a
	// DROP for a replacement in front of this node, and a CREATE that is
	// refused after that drop has already been rendered would leave a
	// migration whose only effect is to delete the operator's function.
	characteristic, err := mysqlroutine.Characteristic(node.Volatility)
	if err != nil {
		return &ptaherr.RenderError{
			Dialect: r.dialect,
			Err:     err,
			Message: fmt.Sprintf("function %s: %s", node.Name, err.Error()),
		}
	}
	security, err := mysqlroutine.SecurityClause(node.Security)
	if err != nil {
		return &ptaherr.RenderError{
			Dialect: r.dialect,
			Err:     err,
			Message: fmt.Sprintf("function %s: %s", node.Name, err.Error()),
		}
	}
	if err := mysqlroutine.ValidateSignature(node.Parameters, node.Returns); err != nil {
		return &ptaherr.RenderError{
			Dialect: r.dialect,
			Err:     err,
			Message: fmt.Sprintf("function %s: %s", node.Name, err.Error()),
		}
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	header := fmt.Sprintf("CREATE FUNCTION %s(%s) RETURNS %s",
		escapeQualifiedIdentifier(node.Name), strings.TrimSpace(node.Parameters), strings.TrimSpace(node.Returns))
	parts := []string{header, characteristic}
	if security != "" {
		parts = append(parts, security)
	}
	parts = append(parts, terminateStatement(node.Body))
	r.w.WriteLinef("%s", strings.Join(parts, " "))
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

// VisitDropFunction renders a DROP FUNCTION statement for MySQL/MariaDB.
//
// A target whose capability set declines Functions still only gets the named
// skip, and it gets it on this half too: its CREATE counterpart answers the
// same way, and an error on only the DOWN half would abort a rollback script
// whose UP half rendered happily.
//
// CASCADE is dropped rather than rendered. Neither engine has it on DROP
// FUNCTION, and a routine has no dependent objects to cascade to in their
// model, so silently omitting it changes nothing the operator asked for.
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	if !r.caps.Has(capability.Functions) {
		r.notGenerated("DROP FUNCTION", node.Name)
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP FUNCTION"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, escapeQualifiedIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateSequence reports that Ptah does not generate standalone sequence
// objects for this target.
//
// MySQL has no SEQUENCE object at all. MariaDB does -- 10.3 and later, verified
// live: `CREATE SEQUENCE s START WITH 1000 NOCYCLE` on MariaDB 10.11.18 lands a
// row with TABLE_TYPE = SEQUENCE -- but Ptah has no MariaDB sequence
// introspection and no MySQL-family sequence planning, so emitting the CREATE
// here would make `schema render` produce a statement `schema apply` never
// plans and a reader never sees again: a plan that cannot converge. That is a
// worse failure than the one this replaces, so the capability stays off until
// the reader and planner arrive; capability.MariaDB1011 records the same thing.
//
// The point of this comment existing at all is that the sequence used to be
// dropped by the converter before any renderer ran, so `--dialect mariadb`
// omitted it with no statement and no diagnostic (stokaro/ptah#931 item 8).
func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE SEQUENCE %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE SEQUENCE %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitAlterSequence renders ALTER SEQUENCE for MySQL-like databases (no-op).
func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- ALTER SEQUENCE %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- ALTER SEQUENCE %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitDropSequence renders DROP SEQUENCE for MySQL-like databases (no-op).
func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP SEQUENCE %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
	} else {
		r.w.WriteLinef("-- DROP SEQUENCE %s not supported in %s", node.Name, r.dialect)
	}
	return nil
}

// VisitDropPolicy names the policy drop Ptah does not generate for this target,
// matching VisitCreatePolicy above rather than aborting the DOWN half alone.
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	r.notGenerated("DROP POLICY", node.Name)
	return nil
}

// VisitAlterTableDisableRLS names the row-level security change Ptah does not
// generate for this target, matching VisitAlterTableEnableRLS above.
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	r.notGenerated("DISABLE ROW LEVEL SECURITY on", node.Table)
	return nil
}

// notGenerated records that Ptah does not generate the named operation for
// this MySQL-family target, in the same sentence the SQL Server renderer uses.
//
// The sentence names the generator rather than the engine: some operations
// routed here have an engine-specific equivalent that Ptah does not model.
// Roles are deliberately not handled by this helper. A declared role is
// first-class state, and Ptah has no MySQL-family role reader or convergent
// planner, so its visitors fail closed instead of reporting comment-only
// success.
func (r *Renderer) notGenerated(kind, name string) {
	if name == "" {
		r.w.WriteLinef("-- %s: %s is not generated for this target; skipped.", r.dialectUpper, kind)
		return
	}
	r.w.WriteLinef("-- %s: %s %s is not generated for this target; skipped.", r.dialectUpper, kind, name)
}

// VisitCreateRole refuses the PostgreSQL-shaped role Ptah cannot converge on
// this target. MySQL and MariaDB both host roles, but Ptah has no reader or
// planner for their role model, so a successful comment-only render would
// discard declared state.
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	return r.unsupportedRole("CREATE ROLE", node.Name)
}

// VisitDropRole refuses rather than reporting a successful comment-only drop.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	return r.unsupportedRole("DROP ROLE", node.Name)
}

// VisitAlterRole refuses rather than silently omitting a detected role change.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	return r.unsupportedRole("ALTER ROLE", node.Name)
}

func (r *Renderer) unsupportedRole(operation, name string) error {
	return unsupportedRoleError(r.dialect, operation, name)
}

// VisitGrantPrivilege names the grant Ptah does not generate for this target.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	r.notGenerated("grant", node.Role)
	return nil
}

// VisitRevokePrivilege names the revoke Ptah does not generate for this target.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	r.notGenerated("revoke", node.Role)
	return nil
}

// VisitRawSQL renders a literal SQL fragment verbatim. Dialect-specific
// routine nodes use this path to preserve executable routine bodies while
// keeping parser metadata available to callers.
func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	sql := strings.TrimSpace(node.SQL)
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}
	r.w.WriteLine(sql)
	return nil
}

// VisitCreateSynonym names the synonym as unsupported. Neither MySQL nor
// MariaDB has a synonym object; the nearest construct is a view, which is a
// different thing with different resolution rules.
func (r *Renderer) VisitCreateSynonym(node *ast.CreateSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- Synonym %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
		return nil
	}
	r.w.WriteLinef("-- Synonym %s not supported in %s", node.Name, r.dialect)
	return nil
}

// VisitDropSynonym names the drop as unsupported, for the same reason.
func (r *Renderer) VisitDropSynonym(node *ast.DropSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP SYNONYM %s not supported in %s: %s", node.Name, r.dialect, node.Comment)
		return nil
	}
	r.w.WriteLinef("-- DROP SYNONYM %s not supported in %s", node.Name, r.dialect)
	return nil
}

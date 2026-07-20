// Package sqlite renders Ptah AST nodes to SQLite DDL.
package sqlite

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/internal/bufwriter"
)

const DialectName = "sqlite"

type Renderer struct {
	w bufwriter.Writer
}

func New() *Renderer {
	return &Renderer{}
}

func (r *Renderer) Dialect() string { return DialectName }

func (r *Renderer) GetDialect() string { return r.Dialect() }

func (r *Renderer) Reset() { r.w.Reset() }

func (r *Renderer) Output() string { return r.w.Output() }

func (r *Renderer) GetOutput() string { return r.Output() }

func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		return "", err
	}
	return r.Output(), nil
}

func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	r.notSupported("schemas", node.Name)
	return nil
}

func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	r.notSupported("databases", node.Name)
	return nil
}

func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}

	if len(node.Columns) == 0 && len(node.Constraints) == 0 && node.SelectBody != "" {
		r.w.Writef("CREATE TABLE%s %s", guard, escapeQualifiedIdentifier(node.Name))
		r.writeTableOptions(node.Options)
		r.w.WriteLinef(" AS %s;", strings.TrimSpace(node.SelectBody))
		return nil
	}

	r.w.WriteLinef("CREATE TABLE%s %s (", guard, escapeQualifiedIdentifier(node.Name))

	lines := make([]string, 0, len(node.Columns)+len(node.Constraints))
	for _, column := range node.Columns {
		line, err := renderColumn(column)
		if err != nil {
			return fmt.Errorf("render column %s: %w", column.Name, err)
		}
		lines = append(lines, line)
	}
	for _, constraint := range node.Constraints {
		line, err := renderConstraint(constraint)
		if err != nil {
			return fmt.Errorf("render constraint: %w", err)
		}
		if line != "" {
			lines = append(lines, line)
		}
	}

	for i, line := range lines {
		if i == len(lines)-1 {
			r.w.WriteLine(line)
			continue
		}
		r.w.WriteLinef("%s,", line)
	}

	r.w.Write(")")
	r.writeTableOptions(node.Options)
	r.w.WriteLine(";")
	return nil
}

func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	for _, operation := range node.Operations {
		switch op := operation.(type) {
		case *ast.AddColumnOperation:
			line, err := renderColumn(op.Column)
			if err != nil {
				return fmt.Errorf("render added column %s: %w", op.Column.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", escapeQualifiedIdentifier(node.Name), strings.TrimSpace(line))
		case *ast.RenameColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME COLUMN %s TO %s;",
				escapeQualifiedIdentifier(node.Name),
				escapeIdentifier(op.OldName),
				escapeIdentifier(op.NewName),
			)
		case *ast.RenameTableOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME TO %s;", escapeQualifiedIdentifier(node.Name), escapeIdentifier(op.NewName))
		case *ast.DropColumnOperation, *ast.ModifyColumnOperation, *ast.DropConstraintOperation, *ast.AddConstraintOperation:
			return unsupportedFeaturef("%T requires a table rebuild plan", operation)
		default:
			return unsupportedFeaturef("unsupported alter table operation %T", operation)
		}
	}
	return nil
}

func (r *Renderer) VisitColumn(_ *ast.ColumnNode) error { return nil }

func (r *Renderer) VisitConstraint(_ *ast.ConstraintNode) error { return nil }

func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"CREATE"}
	if node.Unique {
		parts = append(parts, "UNIQUE")
	}
	parts = append(parts, "INDEX")
	if node.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	parts = append(parts, escapeIdentifier(node.Name), "ON", escapeQualifiedIdentifier(node.Table))
	parts = append(parts, "("+strings.Join(renderIndexParts(node.EffectiveParts()), ", ")+")")
	if strings.TrimSpace(node.Condition) != "" {
		parts = append(parts, "WHERE", strings.TrimSpace(node.Condition))
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP INDEX"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, escapeIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitEnum(_ *ast.EnumNode) error {
	return nil
}

func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	r.w.WriteLinef("-- %s", node.Text)
	return nil
}

func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP TABLE"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, strings.Join(escapeQualifiedIdentifierList(node.TableNames()), ", "))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	r.notSupported("CREATE TYPE", node.Name)
	return nil
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	r.notSupported("ALTER TYPE", node.Name)
	return nil
}

func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	r.notSupported("DROP TYPE", node.Name)
	return nil
}

func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	r.notSupported("extensions", node.Name)
	return nil
}

func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	r.notSupported("DROP EXTENSION", node.Name)
	return nil
}

func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	r.notSupported("CREATE FUNCTION", node.Name)
	return nil
}

func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	r.notSupported("DROP FUNCTION", node.Name)
	return nil
}

func (r *Renderer) VisitCreateView(node *ast.CreateViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.WithCheck {
		return unsupportedFeaturef("WITH CHECK OPTION views are not supported")
	}
	create := "CREATE VIEW"
	if node.Replace {
		create = "CREATE VIEW"
		r.w.WriteLinef("DROP VIEW IF EXISTS %s;", escapeQualifiedIdentifier(node.Name))
	}
	r.w.WriteLinef("%s %s AS", create, escapeQualifiedIdentifier(node.Name))
	r.w.WriteLine(strings.TrimSpace(node.Body))
	if node.WithCheck {
		r.w.WriteLine("WITH CHECK OPTION")
	}
	r.w.WriteLine(";")
	return nil
}

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

func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	r.notSupported("CREATE MATERIALIZED VIEW", node.Name)
	return nil
}

func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	r.notSupported("DROP MATERIALIZED VIEW", node.Name)
	return nil
}

func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	r.notSupported("REFRESH MATERIALIZED VIEW", node.Name)
	return nil
}

func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	forEach := strings.ToUpper(strings.TrimSpace(node.ForEach))
	if forEach == "" {
		forEach = "ROW"
	}
	if forEach != "ROW" {
		return unsupportedFeaturef("FOR EACH %s triggers are not supported", forEach)
	}

	if node.Replace {
		r.w.WriteLinef("DROP TRIGGER IF EXISTS %s;", escapeIdentifier(node.Name))
	}
	body := strings.TrimSuffix(strings.TrimSpace(node.Body), ";")
	r.w.WriteLinef("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s;",
		escapeIdentifier(node.Name),
		node.Timing,
		node.Event,
		escapeQualifiedIdentifier(node.Table),
		body,
	)
	return nil
}

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

func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	r.notSupported("RLS policies", node.Name)
	return nil
}

func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	r.notSupported("DROP POLICY", node.Name)
	return nil
}

func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	r.notSupported("row-level security", node.Table)
	return nil
}

func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	r.notSupported("row-level security", node.Table)
	return nil
}

func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	r.notSupported("roles", node.Name)
	return nil
}

func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	r.notSupported("DROP ROLE", node.Name)
	return nil
}

func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	r.notSupported("ALTER ROLE", node.Name)
	return nil
}

func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	r.notSupported("GRANT", node.Role)
	return nil
}

func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	r.notSupported("REVOKE", node.Role)
	return nil
}

func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	r.w.WriteLine(strings.TrimSpace(node.SQL))
	return nil
}

func (r *Renderer) writeTableOptions(options map[string]string) {
	if strings.EqualFold(options["STRICT"], "true") {
		r.w.Write(" STRICT")
	}
	if strings.EqualFold(options["WITHOUT_ROWID"], "true") || strings.EqualFold(options["WITHOUT ROWID"], "true") {
		r.w.Write(" WITHOUT ROWID")
	}
}

func (r *Renderer) notSupported(feature, name string) {
	if name == "" {
		r.w.WriteLinef("-- SQLITE: %s is not supported", feature)
		return
	}
	r.w.WriteLinef("-- SQLITE: %s %q is not supported", feature, name)
}

func renderColumn(column *ast.ColumnNode) (string, error) {
	if column == nil {
		return "", fmt.Errorf("nil column")
	}
	parts := []string{"  " + escapeIdentifier(column.Name), mapColumnType(column)}
	if column.AutoInc && !column.Primary {
		return "", unsupportedFeaturef("AUTOINCREMENT requires an INTEGER PRIMARY KEY column")
	}
	if column.Primary {
		parts = append(parts, "PRIMARY KEY")
	} else if !column.Nullable {
		parts = append(parts, "NOT NULL")
	}
	if column.AutoInc {
		parts = append(parts, "AUTOINCREMENT")
	}
	if column.Unique {
		parts = append(parts, "UNIQUE")
	}
	if column.GeneratedExpression != "" {
		kind := strings.ToUpper(strings.TrimSpace(column.GeneratedKind))
		if kind == "" {
			kind = "VIRTUAL"
		}
		parts = append(parts, fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", column.GeneratedExpression, kind))
	}
	switch {
	case column.Default == nil:
	case column.Default.HasLiteral():
		parts = append(parts, "DEFAULT", renderDefaultLiteral(column.Default.Value))
	case column.Default.Expression != "":
		parts = append(parts, "DEFAULT", column.Default.Expression)
	}
	if column.Check != "" {
		if column.CheckName != "" {
			parts = append(parts, "CONSTRAINT", escapeIdentifier(column.CheckName), "CHECK", "("+column.Check+")")
		} else {
			parts = append(parts, "CHECK", "("+column.Check+")")
		}
	}
	if column.ForeignKey != nil {
		parts = append(parts, renderInlineForeignKey(column.ForeignKey))
	}
	return strings.Join(parts, " "), nil
}

func mapColumnType(column *ast.ColumnNode) string {
	upper := strings.ToUpper(strings.TrimSpace(column.Type))
	base := upper
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	switch base {
	case "BOOLEAN", "BOOL":
		return "INTEGER"
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL", "AUTO_INCREMENT":
		return "INTEGER"
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "TEXT", "CITEXT", "ENUM":
		return "TEXT"
	case "BYTEA", "BLOB":
		return "BLOB"
	case "DOUBLE PRECISION":
		return "REAL"
	default:
		return column.Type
	}
}

func renderDefaultLiteral(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value
	}
	return escapeStringLiteral(value)
}

func renderConstraint(constraint *ast.ConstraintNode) (string, error) {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return "  PRIMARY KEY (" + strings.Join(escapeIdentifierList(constraint.Columns), ", ") + ")", nil
	case ast.UniqueConstraint:
		prefix := "  "
		if constraint.Name != "" {
			prefix += "CONSTRAINT " + escapeIdentifier(constraint.Name) + " "
		}
		return prefix + "UNIQUE (" + strings.Join(escapeIdentifierList(constraint.Columns), ", ") + ")", nil
	case ast.ForeignKeyConstraint:
		if constraint.Reference == nil {
			return "", fmt.Errorf("foreign key constraint missing reference")
		}
		return "  " + renderNamedForeignKey(constraint.Name, constraint.Columns, constraint.Reference), nil
	case ast.CheckConstraint:
		prefix := "  "
		if constraint.Name != "" {
			prefix += "CONSTRAINT " + escapeIdentifier(constraint.Name) + " "
		}
		return prefix + "CHECK (" + constraint.Expression + ")", nil
	default:
		return "", fmt.Errorf("sqlite: unsupported constraint type %v", constraint.Type)
	}
}

func renderInlineForeignKey(ref *ast.ForeignKeyRef) string {
	return "REFERENCES " + escapeQualifiedIdentifier(ref.Table) + " (" +
		strings.Join(escapeIdentifierList(ref.ReferencedColumns()), ", ") + ")" +
		renderReferentialActions(ref)
}

func renderNamedForeignKey(name string, columns []string, ref *ast.ForeignKeyRef) string {
	prefix := ""
	if name != "" {
		prefix = "CONSTRAINT " + escapeIdentifier(name) + " "
	}
	return prefix + "FOREIGN KEY (" + strings.Join(escapeIdentifierList(columns), ", ") + ") REFERENCES " +
		escapeQualifiedIdentifier(ref.Table) + " (" + strings.Join(escapeIdentifierList(ref.ReferencedColumns()), ", ") + ")" +
		renderReferentialActions(ref)
}

func renderReferentialActions(ref *ast.ForeignKeyRef) string {
	var parts []string
	if ref.OnDelete != "" {
		parts = append(parts, "ON DELETE "+ref.OnDelete)
	}
	if ref.OnUpdate != "" {
		parts = append(parts, "ON UPDATE "+ref.OnUpdate)
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func renderIndexParts(parts []ast.IndexPart) []string {
	rendered := make([]string, 0, len(parts))
	for _, part := range parts {
		spec := escapeQualifiedIdentifier(part.Reference())
		if part.Expr != "" {
			spec = part.Expr
		}
		if part.Desc {
			spec += " DESC"
		}
		rendered = append(rendered, spec)
	}
	return rendered
}

func escapeStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func escapeIdentifier(identifier string) string {
	unquoted := unquoteIdentifier(identifier)
	escaped := strings.ReplaceAll(unquoted, `"`, `""`)
	return `"` + escaped + `"`
}

func escapeQualifiedIdentifier(identifier string) string {
	parts := splitQualifiedIdentifier(identifier)
	for i, part := range parts {
		parts[i] = escapeIdentifier(part)
	}
	return strings.Join(parts, ".")
}

func escapeIdentifierList(identifiers []string) []string {
	escaped := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		escaped[i] = escapeIdentifier(identifier)
	}
	return escaped
}

func escapeQualifiedIdentifierList(identifiers []string) []string {
	escaped := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		escaped[i] = escapeQualifiedIdentifier(identifier)
	}
	return escaped
}

func unquoteIdentifier(identifier string) string {
	if len(identifier) >= 2 {
		switch {
		case identifier[0] == '"' && identifier[len(identifier)-1] == '"':
			return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
		case identifier[0] == '`' && identifier[len(identifier)-1] == '`':
			return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`")
		}
	}
	return identifier
}

func splitQualifiedIdentifier(identifier string) []string {
	parts := []string{""}
	inQuotes := false
	inBackticks := false
	for i := range len(identifier) {
		character := identifier[i]
		switch character {
		case '"':
			inQuotes = !inQuotes
		case '`':
			inBackticks = !inBackticks
		case '.':
			if !inQuotes && !inBackticks {
				parts = append(parts, "")
				continue
			}
		}
		parts[len(parts)-1] += string(character)
	}
	return parts
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: sqlite: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

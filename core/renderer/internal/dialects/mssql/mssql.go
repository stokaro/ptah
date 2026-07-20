// Package mssql renders Ptah AST nodes to Microsoft SQL Server T-SQL DDL.
package mssql

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/internal/bufwriter"
)

const DialectName = platform.SQLServer

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
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.IfNotExists {
		r.w.WriteLinef("IF SCHEMA_ID(%s) IS NULL", escapeStringLiteral(node.Name))
		r.w.WriteLinef("    EXEC(%s);", escapeStringLiteral("CREATE SCHEMA "+escapeQualifiedIdentifier(node.Name)))
		return nil
	}
	r.w.WriteLinef("CREATE SCHEMA %s;", escapeQualifiedIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	if node.IfNotExists {
		r.w.WriteLinef("IF DB_ID(%s) IS NULL", escapeStringLiteral(node.Name))
		r.w.WriteLinef("    CREATE DATABASE %s;", escapeIdentifier(node.Name))
		return nil
	}
	r.w.WriteLinef("CREATE DATABASE %s;", escapeIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.SelectBody != "" {
		return unsupportedFeaturef("CREATE TABLE AS SELECT is not supported")
	}
	if node.IfNotExists {
		r.w.WriteLinef("IF OBJECT_ID(%s, 'U') IS NULL", escapeStringLiteral(node.Name))
	}
	r.w.WriteLinef("CREATE TABLE %s (", escapeQualifiedIdentifier(node.Name))

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
	r.w.WriteLine(");")
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
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", escapeQualifiedIdentifier(node.Name), strings.TrimSpace(line))
		case *ast.AddConstraintOperation:
			line, err := renderConstraint(op.Constraint)
			if err != nil {
				return fmt.Errorf("render added constraint: %w", err)
			}
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", escapeQualifiedIdentifier(node.Name), strings.TrimSpace(line))
		case *ast.DropConstraintOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP CONSTRAINT %s;",
				escapeQualifiedIdentifier(node.Name),
				escapeIdentifier(op.ConstraintName),
			)
		case *ast.DropColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP COLUMN %s;",
				escapeQualifiedIdentifier(node.Name),
				escapeIdentifier(op.ColumnName),
			)
		case *ast.ModifyColumnOperation:
			line, err := renderColumnForAlter(op.Column)
			if err != nil {
				return fmt.Errorf("render modified column %s: %w", op.Column.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s;", escapeQualifiedIdentifier(node.Name), line)
		case *ast.RenameColumnOperation:
			r.w.WriteLinef("EXEC sp_rename %s, %s, 'COLUMN';",
				escapeStringLiteral(node.Name+"."+op.OldName),
				escapeStringLiteral(op.NewName),
			)
		case *ast.RenameTableOperation:
			r.w.WriteLinef("EXEC sp_rename %s, %s;",
				escapeStringLiteral(node.Name),
				escapeStringLiteral(op.NewName),
			)
		case *ast.AddSkippingIndexOperation, *ast.ModifyTTLOperation:
			r.notSupported("ClickHouse table option", node.Name)
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
	if node.IfNotExists {
		r.w.WriteLinef("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = %s AND object_id = OBJECT_ID(%s))",
			escapeStringLiteral(node.Name),
			escapeStringLiteral(node.Table),
		)
	}
	parts := []string{"CREATE"}
	if node.Unique {
		parts = append(parts, "UNIQUE")
	}
	parts = append(parts, "INDEX", escapeIdentifier(node.Name), "ON", escapeQualifiedIdentifier(node.Table))
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
	if node.Table == "" {
		return unsupportedFeaturef("DROP INDEX requires table name")
	}
	parts := []string{"DROP INDEX"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, escapeIdentifier(node.Name), "ON", escapeQualifiedIdentifier(node.Table))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitEnum(_ *ast.EnumNode) error { return nil }

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	r.notSupported("CREATE TYPE", node.Name)
	return nil
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	r.notSupported("ALTER TYPE", node.Name)
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
	create := "CREATE VIEW"
	if node.Replace {
		create = "CREATE OR ALTER VIEW"
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
	create := "CREATE TRIGGER"
	if node.Replace {
		create = "CREATE OR ALTER TRIGGER"
	}
	body := strings.TrimSpace(node.Body)
	if body == "" {
		return unsupportedFeaturef("CREATE TRIGGER requires a body")
	}
	if !strings.HasPrefix(strings.ToUpper(body), "AS") {
		body = "AS " + body
	}
	r.w.WriteLinef("%s %s ON %s %s %s;",
		create,
		escapeQualifiedIdentifier(node.Name),
		escapeQualifiedIdentifier(node.Table),
		renderTriggerEvent(node),
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
	parts = append(parts, escapeQualifiedIdentifier(node.Name))
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
	sql := strings.TrimSpace(node.SQL)
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}
	r.w.WriteLine(sql)
	return nil
}

func (r *Renderer) notSupported(feature, name string) {
	if name == "" {
		r.w.WriteLinef("-- SQLSERVER: %s is not supported", feature)
		return
	}
	r.w.WriteLinef("-- SQLSERVER: %s %q is not supported", feature, name)
}

func renderColumn(column *ast.ColumnNode) (string, error) {
	if column == nil {
		return "", fmt.Errorf("nil column")
	}
	if column.GeneratedExpression != "" {
		return "  " + escapeIdentifier(column.Name) + " " + renderGeneratedColumn(column), nil
	}
	parts := []string{"  " + escapeIdentifier(column.Name), mapColumnType(column.Type)}
	if column.AutoInc {
		parts = append(parts, renderIdentity(column))
	}
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
	appendDefault(&parts, column)
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

func renderColumnForAlter(column *ast.ColumnNode) (string, error) {
	if column == nil {
		return "", fmt.Errorf("nil column")
	}
	parts := []string{escapeIdentifier(column.Name), mapColumnType(column.Type)}
	if !column.Nullable {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}
	return strings.Join(parts, " "), nil
}

func appendDefault(parts *[]string, column *ast.ColumnNode) {
	switch {
	case column.Default == nil:
	case column.Default.HasLiteral():
		*parts = append(*parts, "DEFAULT", renderDefaultLiteral(column.Default.Value))
	case column.Default.Expression != "":
		*parts = append(*parts, "DEFAULT", column.Default.Expression)
	}
}

func renderIdentity(column *ast.ColumnNode) string {
	start := strings.TrimSpace(column.IdentityStart)
	if start == "" {
		start = "1"
	}
	increment := strings.TrimSpace(column.IdentityIncrement)
	if increment == "" {
		increment = "1"
	}
	return fmt.Sprintf("IDENTITY(%s,%s)", start, increment)
}

func renderGeneratedColumn(column *ast.ColumnNode) string {
	sql := fmt.Sprintf("AS (%s)", column.GeneratedExpression)
	if strings.EqualFold(strings.TrimSpace(column.GeneratedKind), "PERSISTED") {
		sql += " PERSISTED"
	}
	return sql
}

func mapColumnType(columnType string) string {
	upper := strings.ToUpper(strings.TrimSpace(columnType))
	base := upper
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	switch base {
	case "INTEGER", "INT4", "SERIAL":
		return "INT"
	case "BIGSERIAL":
		return "BIGINT"
	case "BOOLEAN", "BOOL":
		return "BIT"
	case "TEXT", "CITEXT":
		return "NVARCHAR(MAX)"
	case "VARCHAR", "CHARACTER VARYING":
		return replaceTypeName(columnType, "NVARCHAR")
	case "CHAR", "CHARACTER":
		return replaceTypeName(columnType, "NCHAR")
	case "BYTEA", "BLOB":
		return "VARBINARY(MAX)"
	case "DOUBLE PRECISION":
		return "FLOAT"
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return "DATETIMEOFFSET"
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return "DATETIME2"
	default:
		return columnType
	}
}

func replaceTypeName(original, replacement string) string {
	if idx := strings.Index(original, "("); idx >= 0 {
		return replacement + original[idx:]
	}
	return replacement
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
		return "  PRIMARY KEY (" + renderConstraintColumns(constraint) + ")", nil
	case ast.UniqueConstraint:
		prefix := "  "
		if constraint.Name != "" {
			prefix += "CONSTRAINT " + escapeIdentifier(constraint.Name) + " "
		}
		return prefix + "UNIQUE (" + renderConstraintColumns(constraint) + ")", nil
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
		return "", fmt.Errorf("sqlserver: unsupported constraint type %v", constraint.Type)
	}
}

func renderConstraintColumns(constraint *ast.ConstraintNode) string {
	if len(constraint.ColumnParts) == 0 {
		return strings.Join(escapeIdentifierList(constraint.Columns), ", ")
	}
	parts := make([]string, 0, len(constraint.ColumnParts))
	for _, column := range constraint.ColumnParts {
		part := escapeIdentifier(column.Name)
		if column.Desc {
			part += " DESC"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
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

func renderTriggerEvent(node *ast.CreateTriggerNode) string {
	timing := strings.ToUpper(strings.TrimSpace(node.Timing))
	if timing == "BEFORE" {
		timing = "AFTER"
	}
	if timing == "" {
		timing = "AFTER"
	}
	event := strings.ToUpper(strings.TrimSpace(node.Event))
	if event == "" {
		event = "INSERT"
	}
	return timing + " " + event
}

func escapeStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func escapeIdentifier(identifier string) string {
	unquoted := unquoteIdentifier(identifier)
	escaped := strings.ReplaceAll(unquoted, "]", "]]")
	return "[" + escaped + "]"
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
		case identifier[0] == '[' && identifier[len(identifier)-1] == ']':
			return strings.ReplaceAll(identifier[1:len(identifier)-1], "]]", "]")
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
	inBrackets := false
	inQuotes := false
	inBackticks := false
	for i := 0; i < len(identifier); i++ {
		character := identifier[i]
		switch character {
		case '[':
			if !inQuotes && !inBackticks {
				inBrackets = true
			}
		case ']':
			if inBrackets && i+1 < len(identifier) && identifier[i+1] == ']' {
				parts[len(parts)-1] += "]]"
				i++
				continue
			}
			inBrackets = false
		case '"':
			if !inBrackets && !inBackticks {
				inQuotes = !inQuotes
			}
		case '`':
			if !inBrackets && !inQuotes {
				inBackticks = !inBackticks
			}
		case '.':
			if !inBrackets && !inQuotes && !inBackticks {
				parts = append(parts, "")
				continue
			}
		}
		parts[len(parts)-1] += string(character)
	}
	return parts
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: sqlserver: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

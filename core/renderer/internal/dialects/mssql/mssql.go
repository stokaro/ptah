// Package mssql renders Ptah AST nodes to Microsoft SQL Server T-SQL DDL.
package mssql

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
)

const DialectName = platform.SQLServer

type Renderer struct {
	w    bufwriter.Writer
	caps capability.Capabilities
}

func New() *Renderer {
	return NewWithCapabilities(capability.SQLServer2022())
}

// NewWithCapabilities constructs a SQL Server renderer for a concrete server
// capability set. The set is cloned so later caller mutations cannot change
// rendering behavior (stokaro/ptah#916).
func NewWithCapabilities(caps capability.Capabilities) *Renderer {
	return &Renderer{caps: caps.Clone()}
}

func (r *Renderer) capabilities() capability.Capabilities { return r.caps }

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
			// SQL Server has one spelling for every constraint kind, so
			// op.ForeignKey and op.Check -- which a planner sets for targets
			// that need MySQL's dedicated clauses -- are deliberately ignored
			// here. The guard is not: it is ACCEPTED on every supported line,
			// and dropping it turned a re-runnable statement into one that
			// fails the second time (stokaro/ptah#916).
			guard := ""
			if op.IfExists && r.capabilities().Has(capability.DropConstraintIfExists) {
				guard = "IF EXISTS "
			}
			r.w.WriteLinef("ALTER TABLE %s DROP CONSTRAINT %s%s;",
				escapeQualifiedIdentifier(node.Name),
				guard,
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
			// No capability gate: sp_rename IS the SQL Server rename, and it
			// is what capability.RenameColumnClause being false on every SQL
			// Server line records -- `ALTER TABLE ... RENAME COLUMN` is
			// "Incorrect syntax near 'RENAME'" on 15.0, 16.0 and 17.0 alike,
			// so there is no arm where the clause becomes the right emission
			// (stokaro/ptah#916).
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
	if node.IfExists && r.capabilities().Has(capability.DropIndexIfExists) {
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

// VisitCreateSequence names the sequence Ptah declines to generate for SQL
// Server. It does NOT claim SQL Server has no sequences -- it has had them since
// 2012, and the T-SQL spelling is close enough to the standard that emitting one
// would be easy.
//
// What is missing is the other two thirds. capability.SQLServer2022 sets
// Sequences: false because internal/dbschema/mssql does not read sequences back
// into goschema.Database.Sequences and internal/planner/dialects/mssql plans
// nothing for them, so a CREATE SEQUENCE emitted here would be a statement
// `schema apply` never plans and `db read` never sees again: an apply loop that
// re-adds the same object forever. core/renderer.TestRender_SequencesCapability
// AgreesWithTheGenerator holds the two sides together, so flipping this to emit
// requires the reader and the planner to land in the same change.
func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	r.notSupported("CREATE SEQUENCE", node.Name)
	return nil
}

func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	r.notSupported("ALTER SEQUENCE", node.Name)
	return nil
}

func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	r.notSupported("DROP SEQUENCE", node.Name)
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

// VisitCreateMaterializedView refuses: SQL Server has no materialized view
// object (an indexed view is a different construct with different rules).
//
// This used to render a comment. A comment makes `schema render` exit 0 on a
// model the planner refuses at `schema apply` time, so the surface a user is
// told to validate with disagreed with the surface that executes.
func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	return materializedViewsUnsupported("CREATE MATERIALIZED VIEW", node.Name)
}

// VisitDropMaterializedView refuses for the same reason as
// VisitCreateMaterializedView.
func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	return materializedViewsUnsupported("DROP MATERIALIZED VIEW", node.Name)
}

// VisitRefreshMaterializedView refuses for the same reason as
// VisitCreateMaterializedView.
func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	return materializedViewsUnsupported("REFRESH MATERIALIZED VIEW", node.Name)
}

func materializedViewsUnsupported(statement, name string) error {
	return unsupportedFeaturef("%s %s: materialized views are not supported by SQL Server; remove matview definitions for this target", statement, name)
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
	event, err := renderTriggerEvent(node)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.ToUpper(body), "AS") {
		body = "AS " + body
	}
	r.w.WriteLinef("%s %s ON %s %s %s",
		create,
		escapeQualifiedIdentifier(node.Name),
		escapeQualifiedIdentifier(node.Table),
		event,
		terminateStatement(body),
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

// notSupported records that Ptah does not generate the named object for a SQL
// Server target.
//
// The sentence is about the generator, not about the engine, and that is the
// whole point of the wording. It used to read "... is not supported", which is
// a claim about SQL Server, and on this renderer several of those claims are
// false: SQL Server has had CREATE SEQUENCE since 2012, it has database roles,
// it has scalar and table-valued functions, it has alias types (CREATE TYPE
// <name> FROM <base>), and it has row-level security through security policies.
// Ptah declines all of them for a reason that has nothing to do with the
// engine: capability.SQLServer2022 leaves Sequences, RoleManagement and
// RowLevelSecurity off because there is no SQL Server reader and no SQL Server
// planner for those kinds, and a CREATE the reader never sees again and the
// planner never plans is a schema that cannot converge -- the same reason
// capability.MariaDB1011 keeps Sequences off for an engine that has had them
// since 10.3.
//
// Getting that sentence right became load-bearing when the converter stopped
// gating emission by dialect name: before, these nodes were deleted before they
// reached this renderer, so a false diagnostic was invisible. Now every declared
// object arrives here (stokaro/ptah#929 item 5).
func (r *Renderer) notSupported(feature, name string) {
	if name == "" {
		r.w.WriteLinef("-- SQLSERVER: %s is not generated for this target; skipped.", feature)
		return
	}
	r.w.WriteLinef("-- SQLSERVER: %s %q is not generated for this target; skipped.", feature, name)
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

// renderColumnForAlter renders the column body of `ALTER TABLE ... ALTER COLUMN`.
//
// SQL Server makes a primary key column NOT NULL and will not let it go: an
// ALTER COLUMN that respells a key column as NULL is refused because the
// primary key constraint depends on it. ast.ColumnNode.Nullable no longer
// carries "a key column is NOT NULL" for the AST, because SQLite does not have
// that rule (stokaro/ptah#1235), so this renderer applies it where the dialect
// is known -- the same branch renderColumn takes on the CREATE TABLE path.
// Measured live on PostgreSQL, whose ALTER path had the identical hole and
// planned an unappliable `DROP NOT NULL`; this dialect is guarded by
// inspection, with no live SQL Server to measure against.
func renderColumnForAlter(column *ast.ColumnNode) (string, error) {
	if column == nil {
		return "", fmt.Errorf("nil column")
	}
	parts := []string{escapeIdentifier(column.Name), mapColumnType(column.Type)}
	if !column.Nullable || column.Primary {
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

// renderTriggerEvent builds the timing/event clause of a T-SQL trigger.
//
// SQL Server DML triggers are AFTER or INSTEAD OF; there is no BEFORE. A BEFORE
// trigger is refused rather than rewritten to AFTER, because the rewrite moves
// the body from running ahead of the write to running behind it — the two see
// different table state, and a BEFORE trigger that adjusts the row being
// written cannot do so at all once it is AFTER.
func renderTriggerEvent(node *ast.CreateTriggerNode) (string, error) {
	timing := strings.ToUpper(strings.TrimSpace(node.Timing))
	if timing == "" {
		timing = "AFTER"
	}
	if timing == "BEFORE" {
		return "", unsupportedFeaturef("BEFORE triggers are not supported; SQL Server offers AFTER and INSTEAD OF")
	}
	event := strings.ToUpper(strings.TrimSpace(node.Event))
	if event == "" {
		event = "INSERT"
	}
	return timing + " " + event, nil
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

// splitQualifiedIdentifier splits on the dots that separate name parts while
// leaving dots inside a bracketed, double-quoted or backtick-quoted part alone.
// A doubled closing bracket is SQL Server's escape for a literal bracket and
// does not end the bracketed part.
//
// Each part is a SLICE of the input, never a character-by-character copy. The
// delimiters this scan recognizes are ASCII, and UTF-8 is self synchronizing --
// no byte of a multi-byte sequence is ever below 0x80 -- so a byte scan can
// find them without decoding, and slicing hands every other byte back exactly
// as it arrived. The previous form accumulated `string(character)` from a byte,
// which re-encodes each byte as its own code point: `Ä` (C3 84) came back out
// as `Ã` plus U+0084, renaming every non-ASCII object. See stokaro/ptah#1352.
//
// Decoding to runes would fix that case and introduce another: text that is not
// valid UTF-8 -- a Latin-1 schema file, say -- decodes to U+FFFD per bad byte
// and would be rewritten just as silently. A splitter owes its caller the bytes
// it was given.
func splitQualifiedIdentifier(identifier string) []string {
	var parts []string
	start := 0
	inBrackets := false
	inQuotes := false
	inBackticks := false
	for i := 0; i < len(identifier); i++ {
		switch identifier[i] {
		case '[':
			if !inQuotes && !inBackticks {
				inBrackets = true
			}
		case ']':
			if inBrackets && i+1 < len(identifier) && identifier[i+1] == ']' {
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
				parts = append(parts, identifier[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, identifier[start:])
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: sqlserver: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

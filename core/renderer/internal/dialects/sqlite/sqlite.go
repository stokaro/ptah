// Package sqlite renders Ptah AST nodes to SQLite DDL.
package sqlite

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"go.5x5.cz/ptah/internal/sqlident"
)

const DialectName = "sqlite"

type Renderer struct {
	w    bufwriter.Writer
	caps capability.Capabilities
}

// New constructs a renderer for the SQLite the offline paths assume, which is
// the newest measured line rather than the oldest supported one: a rendered
// file is read by whatever SQLite the operator has, and describing it as the
// 3.24 floor would comment out statements every engine Ptah links accepts.
func New() *Renderer {
	return NewWithCapabilities(capability.SQLite3())
}

// NewWithCapabilities constructs a SQLite renderer for a concrete server
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

// defaultSchema is the namespace every unqualified SQLite object already
// belongs to. It is not a schema anybody creates.
const defaultSchema = "main"

func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	if node.Name == defaultSchema {
		// `main` is where the connection already is, so there is nothing to
		// create and nothing to refuse. An introspected SQLite database
		// describes it as a schema — the pinned Atlas community binary v1.3.0
		// renders `schema "main" {}` in HCL and no statement at all in SQL —
		// and turning that into a "not supported" comment would put a refusal
		// in the output for something the author never asked for
		// (stokaro/ptah#1264).
		return nil
	}
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

	if module := node.Options[ast.SQLiteVirtualModuleOption]; module != "" {
		r.writeCreateVirtualTable(node, guard, module)
		return nil
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
		line, err := renderColumn(column, r.capabilities())
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
			line, err := renderColumn(op.Column, r.capabilities())
			if err != nil {
				return fmt.Errorf("render added column %s: %w", op.Column.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", escapeQualifiedIdentifier(node.Name), strings.TrimSpace(line))
		case *ast.RenameColumnOperation:
			if !r.capabilities().Has(capability.RenameColumnClause) {
				// SQLite below 3.25 has no RENAME COLUMN at all: the rename is
				// done by rebuilding the table, which is a different plan than
				// the one this node describes. Emitting the clause anyway
				// produces a file the target refuses on the first statement,
				// so the operator is told which one was dropped instead.
				r.notSupported("ALTER TABLE ... RENAME COLUMN", op.OldName)
				continue
			}
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
	indexName, tableName := sqliteIndexTarget(node.Name, node.Table)
	parts := []string{"CREATE"}
	if node.Unique {
		parts = append(parts, "UNIQUE")
	}
	parts = append(parts, "INDEX")
	if node.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	parts = append(parts, indexName, "ON", tableName)
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
	parts = append(parts, sqliteDropIndexTarget(node.Name, node.Table))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// sqliteDropIndexTarget renders the index a DROP INDEX names.
//
// It is [sqliteIndexTarget]'s index half plus the case CREATE INDEX never has:
// a drop with no owning table recorded, where the index name is the only place
// a schema qualifier can live and `DROP INDEX app.idx` puts one there. Escaping
// it whole would emit "app.idx" as a single identifier and name an index nobody
// created. See [ast.DropIndexNode.Name].
func sqliteDropIndexTarget(indexName, tableName string) string {
	tableParts := splitQualifiedIdentifier(tableName)
	if len(tableParts) < 2 {
		return escapeQualifiedIdentifier(indexName)
	}
	schema := strings.Join(tableParts[:len(tableParts)-1], ".")
	return escapeQualifiedIdentifier(schema) + "." + escapeIdentifier(indexName)
}

func sqliteIndexTarget(indexName, tableName string) (renderedIndexName, renderedTableName string) {
	tableParts := splitQualifiedIdentifier(tableName)
	if len(tableParts) < 2 {
		return escapeIdentifier(indexName), escapeIdentifier(tableName)
	}
	schema := strings.Join(tableParts[:len(tableParts)-1], ".")
	return escapeQualifiedIdentifier(schema) + "." + escapeIdentifier(indexName),
		escapeIdentifier(tableParts[len(tableParts)-1])
}

func (r *Renderer) VisitUpsert(_ *ast.UpsertNode) error {
	return unsupportedFeaturef("upsert rendering is not implemented")
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

// VisitCreateMaterializedView refuses: SQLite has no materialized view object.
//
// This used to render a comment. A comment makes `schema render` exit 0 on a
// model the planner refuses at `schema apply` time, so the surface a user is
// told to validate with disagreed with the surface that executes. The SQLite
// planner already answers "materialized views are not supported".
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
	return unsupportedFeaturef("%s %s: materialized views are not supported", statement, name)
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

// writeCreateVirtualTable writes the statement that created a virtual table.
//
// Describing one with CREATE TABLE emits a statement that never created it:
// a plain table named `docs` is not a full-text index, `MATCH` against it
// fails, and applying the description makes SQLite's own shadow tables collide
// with the real object later. The column list is deliberately not written --
// a virtual table's columns come from the module, and the module arguments are
// what recreate them. See stokaro/ptah#1028.
func (r *Renderer) writeCreateVirtualTable(node *ast.CreateTableNode, guard, module string) {
	r.w.Writef("CREATE VIRTUAL TABLE%s %s USING %s",
		guard,
		escapeQualifiedIdentifier(node.Name),
		escapeModuleName(module),
	)
	if arguments := node.Options[ast.SQLiteVirtualArgumentsOption]; arguments != "" {
		r.w.Writef("(%s)", arguments)
	}
	r.w.WriteLine(";")
}

// escapeModuleName keeps a plain, nonreserved module identifier bare, the way
// SQLite records it, and quotes punctuation, whitespace, empty names, and
// SQLite keywords. SQLite resolves the module name as an identifier, while a
// bare keyword is parsed as syntax instead.
//
// The rule lives in sqlident because the inspection check has to look for
// exactly the spelling this produces; when the two disagreed, a SQL document
// that carried `USING "fts-5"` was reported as lossy and refused under strict
// compatibility. See stokaro/ptah#1028.
func escapeModuleName(module string) string {
	return sqlident.BareOrQuoted(DialectName, module)
}

func (r *Renderer) writeTableOptions(options map[string]string) {
	var tableOptions []string
	if strings.EqualFold(options["STRICT"], "true") {
		tableOptions = append(tableOptions, "STRICT")
	}
	if strings.EqualFold(options["WITHOUT_ROWID"], "true") || strings.EqualFold(options["WITHOUT ROWID"], "true") {
		tableOptions = append(tableOptions, "WITHOUT ROWID")
	}
	if len(tableOptions) > 0 {
		r.w.Write(" " + strings.Join(tableOptions, ", "))
	}
}

func (r *Renderer) notSupported(feature, name string) {
	if name == "" {
		r.w.WriteLinef("-- SQLITE: %s is not supported", feature)
		return
	}
	r.w.WriteLinef("-- SQLITE: %s %q is not supported", feature, name)
}

func renderColumn(column *ast.ColumnNode, caps capability.Capabilities) (string, error) {
	if column == nil {
		return "", fmt.Errorf("nil column")
	}
	parts := []string{"  " + escapeIdentifier(column.Name), mapColumnType(column)}
	if column.AutoInc && !column.Primary {
		return "", unsupportedFeaturef("AUTOINCREMENT requires an INTEGER PRIMARY KEY column")
	}
	// NOT NULL is written even when the column is the primary key. SQLite does
	// not derive one from the other: `id integer PRIMARY KEY` is a rowid alias
	// whose `pragma table_info.notnull` is 0 and which accepts an explicit NULL
	// insert, so folding an author's NOT NULL into PRIMARY KEY drops a
	// constraint the source declared. Measured against the pinned Atlas
	// community v1.3.0 binary: applying an HCL table with `null = false` on the
	// key column and then asking that binary whether the result matches the
	// same file answered `Schemas are synced, no changes to be made.` only once
	// the NOT NULL survived; without it the binary planned a full table rebuild
	// against a database Ptah had just applied from that file. See
	// stokaro/ptah#1235 group 5.
	if !column.Nullable {
		parts = append(parts, "NOT NULL")
	}
	if column.Primary {
		parts = append(parts, "PRIMARY KEY")
	}
	if column.AutoInc {
		parts = append(parts, "AUTOINCREMENT")
	}
	if column.Unique {
		parts = append(parts, "UNIQUE")
	}
	if column.GeneratedExpression != "" {
		// SQLite gained generated columns in 3.31, well above the 3.25 step
		// this preset's lower arm describes, so a target pinned there cannot
		// parse the clause. Dropping it silently would turn a generated column
		// into an ordinary one (stokaro/ptah#916).
		if !caps.Has(capability.GeneratedColumns) {
			return "", unsupportedFeaturef(
				"the target SQLite does not support generated columns; column %q declares GENERATED ALWAYS AS",
				column.Name)
		}
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
	prefix := ""
	if ref.Name != "" {
		prefix = "CONSTRAINT " + escapeIdentifier(ref.Name) + " "
	}
	return prefix + "REFERENCES " + escapeQualifiedIdentifier(ref.Table) + " (" +
		strings.Join(escapeIdentifierList(ref.ReferencedColumns()), ", ") + ")" +
		renderReferentialActions(ref) + renderDeferral(ref)
}

func renderNamedForeignKey(name string, columns []string, ref *ast.ForeignKeyRef) string {
	prefix := ""
	if name != "" {
		prefix = "CONSTRAINT " + escapeIdentifier(name) + " "
	}
	return prefix + "FOREIGN KEY (" + strings.Join(escapeIdentifierList(columns), ", ") + ") REFERENCES " +
		escapeQualifiedIdentifier(ref.Table) + " (" + strings.Join(escapeIdentifierList(ref.ReferencedColumns()), ", ") + ")" +
		renderReferentialActions(ref) + renderDeferral(ref)
}

// renderDeferral renders DEFERRABLE and its timing.
//
// SQLite's foreign-key grammar carries the clause and the linked engine accepts
// it: measured on sqlite_version() 3.53.3, `CONSTRAINT fk FOREIGN KEY (id)
// REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED` is created without error.
// There is no capability gate here because every SQLite preset Ptah ships has
// the key -- the clause predates every version in the ladder
// (stokaro/ptah#1624).
func renderDeferral(ref *ast.ForeignKeyRef) string {
	if !ref.Deferrable && ref.Initially == "" {
		return ""
	}
	timings := map[string]string{
		"":          "",
		"deferred":  " INITIALLY DEFERRED",
		"immediate": " INITIALLY IMMEDIATE",
	}
	return " DEFERRABLE" + timings[strings.ToLower(strings.TrimSpace(ref.Initially))]
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

// splitQualifiedIdentifier splits on the dots that separate name parts while
// leaving dots inside a double-quoted or backtick-quoted part alone.
//
// Each part is a SLICE of the input, never a character-by-character copy. The
// three delimiters this scan recognizes are ASCII, and UTF-8 is self
// synchronizing -- no byte of a multi-byte sequence is ever below 0x80 -- so a
// byte scan can find them without decoding, and slicing hands every other byte
// back exactly as it arrived. The previous form accumulated `string(character)`
// from a byte, which re-encodes each byte as its own code point: `Ä` (C3 84)
// came back out as `Ã` plus U+0084, renaming every non-ASCII object. See
// stokaro/ptah#1352.
//
// Decoding to runes would fix that case and introduce another: text that is not
// valid UTF-8 -- a Latin-1 schema file, say -- decodes to U+FFFD per bad byte
// and would be rewritten just as silently. A splitter owes its caller the bytes
// it was given.
func splitQualifiedIdentifier(identifier string) []string {
	var parts []string
	start := 0
	inQuotes := false
	inBackticks := false
	for i := range len(identifier) {
		switch identifier[i] {
		case '"':
			inQuotes = !inQuotes
		case '`':
			inBackticks = !inBackticks
		case '.':
			if !inQuotes && !inBackticks {
				parts = append(parts, identifier[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, identifier[start:])
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: sqlite: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

// VisitCreateSynonym refuses: SQLite has no synonym object of any kind.
func (r *Renderer) VisitCreateSynonym(node *ast.CreateSynonymNode) error {
	r.notSupported("CREATE SYNONYM", node.Name)
	return nil
}

// VisitDropSynonym refuses for the same reason.
func (r *Renderer) VisitDropSynonym(node *ast.DropSynonymNode) error {
	r.notSupported("DROP SYNONYM", node.Name)
	return nil
}

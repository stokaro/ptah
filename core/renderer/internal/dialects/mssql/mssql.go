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

// namedColumnCheck gives an unnamed column CHECK the name the comparison will
// look for.
//
// SQL Server names an inline CHECK itself -- `CK__orders__status__571DF1D5`,
// with is_system_named = 1 -- and the hash is per database. The comparison
// looks for the convention an unnamed column check carries everywhere else,
// `<table>_<column>_check`, so the first read-back after CREATE TABLE
// disagreed with the declaration and the next apply renamed the constraint.
// The schema converged, but only on the second run (stokaro/ptah#1716).
//
// A declaration that names its own check keeps that name; only the unnamed
// case is filled in.
func namedColumnCheck(table string, column *ast.ColumnNode) *ast.ColumnNode {
	if column == nil || column.Check == "" || column.CheckName != "" {
		return column
	}
	named := *column
	named.CheckName = unquoteIdentifier(tableLeafName(table)) + "_" + column.Name + "_check"
	return &named
}

// tableLeafName drops the schema qualifier, so a table in a named schema gets
// the same check name it would get in the default one.
func tableLeafName(table string) string {
	parts := splitQualifiedIdentifier(table)
	if len(parts) == 0 {
		return table
	}
	return parts[len(parts)-1]
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
		line, err := renderColumn(namedColumnCheck(node.Name, column))
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

// defaultSchema is where SQL Server puts an object whose name carries no
// schema, and therefore where an existence guard has to look for one.
const defaultSchema = "dbo"

// refuses reports whether this target declines an object kind, writing the
// named skip diagnostic when it does.
//
// It is the same shape [Renderer.notSupported] writes, so a kind gated by a
// capability and a kind this renderer never generates read identically to the
// caller: a comment naming the object rather than a missing statement.
func (r *Renderer) refuses(key capability.Capability, kind, name string) bool {
	if r.capabilities().Has(key) {
		return false
	}
	r.notSupported(strings.ToUpper(kind), name)
	return true
}

// VisitCreateSequence renders a T-SQL CREATE SEQUENCE.
//
// Every clause here was measured against SQL Server 2025 (RTM-CU8),
// 17.0.4075.5, rather than read from the grammar, because T-SQL and PostgreSQL
// disagree in three places and each disagreement is a statement the engine
// refuses:
//
//   - CREATE SEQUENCE IF NOT EXISTS is `Incorrect syntax near the keyword
//     'IF'`. A declaration asking for the guard gets the sys.sequences
//     existence test this renderer already uses for CREATE SCHEMA, which is
//     idempotent on a second run.
//   - CACHE 0 is `The cache size for sequence object must be greater than 0`,
//     so a declared cache of zero renders NO CACHE, which the engine accepts
//     and which is what a cache of zero means.
//   - There is no OWNED BY. A declaration carrying one is reported rather than
//     dropped: the association is not made, and saying so is the difference
//     between a limitation and a surprise.
//
// The clause ORDER is PostgreSQL's, and that is measured too:
// `AS ... INCREMENT BY ... MINVALUE ... MAXVALUE ... START WITH ... CACHE ...
// CYCLE` is accepted, so the shared ordering needs no T-SQL variant.
func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.OwnedBy != "" {
		r.w.WriteLinef("-- SQLSERVER: sequence %q declares OWNED BY %q, which T-SQL has no clause for; "+
			"the association is not made.", node.Name, node.OwnedBy)
	}
	statement := strings.Join(append(
		[]string{"CREATE SEQUENCE", sequenceIdentifier(node.Name, node.Schema)},
		sequenceOptions(node.AsType, node.Start, node.Increment, node.MinValue, node.MaxValue,
			node.Cache, cyclePointer(node.Cycle))...,
	), " ")
	if !node.IfNotExists {
		r.w.WriteLinef("%s;", statement)
		return nil
	}
	r.w.WriteLinef("IF NOT EXISTS (SELECT 1 FROM sys.sequences sq JOIN sys.schemas sc "+
		"ON sc.schema_id = sq.schema_id WHERE sc.name = %s AND sq.name = %s)",
		escapeStringLiteral(sequenceSchemaOrDefault(node.Schema)), escapeStringLiteral(unquoteIdentifier(node.Name)))
	r.w.WriteLinef("    EXEC(%s);", escapeStringLiteral(statement))
	return nil
}

// VisitAlterSequence renders a T-SQL ALTER SEQUENCE, and refuses by name the
// two options the engine will not alter in place.
//
// `ALTER SEQUENCE ... AS <type>` is `Argument 'AS' cannot be used in an ALTER
// SEQUENCE statement`, and `START WITH` is the same refusal for its own
// keyword: T-SQL spells that one RESTART WITH, which also resets the current
// value. Both are named rather than dropped, because a plan that silently
// omits the option an author changed reports success and leaves the sequence
// as it was.
func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}
	if node.AsType != "" {
		r.w.WriteLinef("-- SQLSERVER: sequence %q cannot change its type in place; "+
			"ALTER SEQUENCE refuses AS, so this needs a drop and a create.", node.Name)
	}
	if node.OwnedBy != "" {
		r.w.WriteLinef("-- SQLSERVER: sequence %q declares OWNED BY %q, which T-SQL has no clause for; "+
			"the association is not made.", node.Name, node.OwnedBy)
	}
	options := sequenceOptions("", nil, node.Increment, node.MinValue, node.MaxValue, node.Cache, node.Cycle)
	if node.Start != nil {
		// RESTART WITH, not START WITH: the engine refuses the second spelling
		// in an ALTER, and the first also resets the sequence's current value.
		options = append([]string{fmt.Sprintf("RESTART WITH %d", *node.Start)}, options...)
	}
	if len(options) == 0 {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("ALTER SEQUENCE %s %s;", sequenceIdentifier(node.Name, node.Schema), strings.Join(options, " "))
	return nil
}

// VisitDropSequence renders a T-SQL DROP SEQUENCE.
//
// DROP SEQUENCE IF EXISTS is accepted; CASCADE is `Incorrect syntax near the
// keyword 'CASCADE'`, and there is nothing to render in its place. The engine
// refuses a drop a column default still draws from -- `Cannot DROP SEQUENCE
// because it is being referenced by object` -- which is the same protection
// PostgreSQL gives without CASCADE.
func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.Cascade {
		r.w.WriteLinef("-- SQLSERVER: sequence %q asks for CASCADE, which T-SQL has no clause for; "+
			"the engine refuses a drop a column default still draws from.", node.Name)
	}
	parts := []string{"DROP SEQUENCE"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, sequenceIdentifier(node.Name, node.Schema))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// sequenceIdentifier renders a sequence's escaped, schema-qualified name.
func sequenceIdentifier(name, schema string) string {
	if schema == "" || strings.Contains(name, ".") {
		return escapeQualifiedIdentifier(name)
	}
	return escapeQualifiedIdentifier(schema + "." + name)
}

// sequenceSchemaOrDefault names the schema an unqualified sequence lands in,
// which is the one the existence guard has to look in.
func sequenceSchemaOrDefault(schema string) string {
	if trimmed := unquoteIdentifier(strings.TrimSpace(schema)); trimmed != "" {
		return trimmed
	}
	return defaultSchema
}

// sequenceOptions renders the option clauses in the order the engine accepts.
//
// A cache of zero renders NO CACHE: T-SQL refuses CACHE 0, and zero cached
// values is what NO CACHE means.
func sequenceOptions(asType string, start, increment, minValue, maxValue, cache *int64, cycle *bool) []string {
	parts := make([]string, 0)
	if asType != "" {
		parts = append(parts, "AS "+asType)
	}
	if increment != nil {
		parts = append(parts, fmt.Sprintf("INCREMENT BY %d", *increment))
	}
	if minValue != nil {
		parts = append(parts, fmt.Sprintf("MINVALUE %d", *minValue))
	}
	if maxValue != nil {
		parts = append(parts, fmt.Sprintf("MAXVALUE %d", *maxValue))
	}
	if start != nil {
		parts = append(parts, fmt.Sprintf("START WITH %d", *start))
	}
	if cache != nil {
		parts = append(parts, cacheClause(*cache))
	}
	if cycle != nil {
		parts = append(parts, cycleClauses[*cycle])
	}
	return parts
}

func cacheClause(cache int64) string {
	if cache <= 0 {
		return "NO CACHE"
	}
	return fmt.Sprintf("CACHE %d", cache)
}

// cycleClauses spells both sides of the option. The engine takes NO CYCLE as a
// clause of its own rather than as the absence of CYCLE.
var cycleClauses = map[bool]string{true: "CYCLE", false: "NO CYCLE"}

// cyclePointer renders NO CYCLE for a CREATE only when the declaration asks for
// CYCLE, matching what the PostgreSQL renderer emits for the same node.
func cyclePointer(cycle bool) *bool {
	return map[bool]*bool{true: &cycle, false: nil}[cycle]
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

// VisitCreateSynonym renders a CREATE SYNONYM statement.
//
// The target goes through the same identifier escaping as the alias. It is a
// NAME, not a body: writing it verbatim the way a view's SELECT is written
// would emit an unquoted identifier that breaks on the first reserved word or
// space, and a four-part target naming a linked server would break sooner.
func (r *Renderer) VisitCreateSynonym(node *ast.CreateSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("CREATE SYNONYM %s FOR %s;",
		escapeQualifiedIdentifier(node.Name), escapeQualifiedIdentifier(node.Target))
	return nil
}

// VisitDropSynonym renders a DROP SYNONYM statement.
func (r *Renderer) VisitDropSynonym(node *ast.DropSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	parts := []string{"DROP SYNONYM"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, escapeQualifiedIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
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

// VisitAlterMaterializedViewRefresh refuses for the same reason as
// VisitCreateMaterializedView.
func (r *Renderer) VisitAlterMaterializedViewRefresh(node *ast.AlterMaterializedViewRefreshNode) error {
	return materializedViewsUnsupported("ALTER MATERIALIZED VIEW REFRESH", node.Name)
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

// VisitCreateRole renders a T-SQL CREATE ROLE, and refuses a declaration
// carrying attributes a database role does not have.
//
// This is where the two engines' role models actually differ, and the
// difference is not cosmetic. A SQL Server DATABASE ROLE is a container for
// permissions inside one database; it cannot log in and cannot own a password.
// `CREATE ROLE [r] LOGIN` is `Incorrect syntax near 'LOGIN'` on 17.0.4075.5,
// measured -- the thing that logs in is a LOGIN, a server principal outside any
// database schema.
//
// Writing a comment and creating the role anyway is what would make this a
// silent trap twice over. The author would get a principal that cannot do what
// they wrote; and because the reader can only ever report those attributes
// false, the comparison would report the same pending change on every run
// forever. The fail-closed shape is ClickHouse's and mysqllike's -- name the
// role, name the reason, refuse (stokaro/ptah#1698).
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	if r.refuses(capability.RoleManagement, "roles", node.Name) {
		return nil
	}
	if err := refuseServerLevelRoleAttributes("CREATE ROLE", node); err != nil {
		return err
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("CREATE ROLE %s;", escapeIdentifier(unquoteIdentifier(node.Name)))
	return nil
}

// refuseServerLevelRoleAttributes refuses the declared attributes T-SQL has no
// database role form for, naming them in a fixed order so the sentence reads
// the same each run.
func refuseServerLevelRoleAttributes(operation string, node *ast.CreateRoleNode) error {
	declared := []struct {
		name string
		set  bool
	}{
		{"LOGIN", node.Login},
		{"a password", node.Password != ""},
		{"SUPERUSER", node.Superuser},
		{"CREATEDB", node.CreateDB},
		{"CREATEROLE", node.CreateRole},
		{"REPLICATION", node.Replication},
	}
	unhonored := make([]string, 0, len(declared))
	for _, attribute := range declared {
		if attribute.set {
			unhonored = append(unhonored, attribute.name)
		}
	}
	if len(unhonored) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: sqlserver: %s %s: declares %s, which a SQL Server database role does not have; "+
			"a principal that logs in is a server-level LOGIN, outside this schema",
		ptaherr.ErrUnsupportedFeature, operation, node.Name, strings.Join(unhonored, ", "))
}

// VisitDropRole renders a T-SQL DROP ROLE. IF EXISTS is accepted.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	if r.refuses(capability.RoleManagement, "roles", node.Name) {
		return nil
	}
	parts := []string{"DROP ROLE"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, escapeIdentifier(unquoteIdentifier(node.Name)))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitAlterRole refuses rather than emitting or commenting.
//
// `ALTER ROLE` exists in T-SQL, and it renames a role or moves members in and
// out of it. It does not change attributes, because a database role has none.
// An ALTER reaching here is asking for a PostgreSQL attribute transition, and
// a comment saying so would let the plan apply, report success, and leave the
// role exactly as it was.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("ALTER ROLE", node.Name)
		return nil
	}
	return fmt.Errorf(
		"%w: sqlserver: ALTER ROLE %s: a SQL Server database role carries no attributes to alter; "+
			"T-SQL's own ALTER ROLE renames it or moves its members instead",
		ptaherr.ErrUnsupportedFeature, node.Name)
}

// VisitGrantPrivilege renders a T-SQL GRANT.
//
// Two measured facts shape it. A schema grant is spelled `ON SCHEMA::[name]`,
// and omitting the `::` does not fail safely: `GRANT SELECT ON [app]` looks for
// a TABLE called app, so a schema grant written without it silently targets a
// different object whenever a table of that name exists. And `USAGE` is
// `Incorrect syntax near 'USAGE'` -- PostgreSQL's schema-access privilege has
// no T-SQL counterpart, so it is reported rather than emitted.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	if r.refuses(capability.RoleManagement, "GRANT", node.Role) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	privileges, unsupported := splitTSQLPrivileges(node.Privileges)
	if len(unsupported) > 0 {
		r.w.WriteLinef("-- SQLSERVER: grant to %q names %s, which T-SQL has no privilege for; "+
			"schema access there is a permission on the schema's objects.",
			node.Role, strings.Join(unsupported, ", "))
	}
	if len(privileges) == 0 {
		return nil
	}
	statement := fmt.Sprintf("GRANT %s ON %s TO %s",
		strings.Join(privileges, ", "),
		grantTargetIdentifier(node.ObjectType, node.ObjectName),
		escapeIdentifier(unquoteIdentifier(node.Role)))
	if node.WithOption {
		statement += " WITH GRANT OPTION"
	}
	r.w.WriteLinef("%s;", statement)
	return nil
}

// VisitRevokePrivilege renders a T-SQL REVOKE.
//
// Revoking only the grant option is its own spelling, `REVOKE GRANT OPTION FOR
// ... CASCADE`, and the CASCADE is not optional in practice: the option let the
// grantee grant onward, so those grants have to go with it.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	if r.refuses(capability.RoleManagement, "REVOKE", node.Role) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	privileges, unsupported := splitTSQLPrivileges(node.Privileges)
	if len(unsupported) > 0 {
		r.w.WriteLinef("-- SQLSERVER: revoke from %q names %s, which T-SQL has no privilege for; "+
			"nothing was granted under that name either.",
			node.Role, strings.Join(unsupported, ", "))
	}
	if len(privileges) == 0 {
		return nil
	}
	parts := []string{"REVOKE"}
	if node.GrantOptionFor {
		parts = append(parts, "GRANT OPTION FOR")
	}
	parts = append(parts, strings.Join(privileges, ", "),
		"ON", grantTargetIdentifier(node.ObjectType, node.ObjectName),
		"FROM", escapeIdentifier(unquoteIdentifier(node.Role)))
	if node.GrantOptionFor {
		parts = append(parts, "CASCADE")
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// tsqlAbsentPrivileges are the PostgreSQL privilege names T-SQL has no keyword
// for. USAGE is the one that matters: it is how a PostgreSQL schema grant is
// written, and `GRANT USAGE` is a syntax error here.
var tsqlAbsentPrivileges = map[string]bool{"USAGE": true, "TEMPORARY": true, "TEMP": true}

// splitTSQLPrivileges separates the privileges this target can grant from the
// ones it has no keyword for.
func splitTSQLPrivileges(privileges []string) (supported, unsupported []string) {
	supported = make([]string, 0, len(privileges))
	unsupported = make([]string, 0)
	for _, privilege := range privileges {
		normalized := strings.ToUpper(strings.TrimSpace(privilege))
		if normalized == "" {
			continue
		}
		if tsqlAbsentPrivileges[normalized] {
			unsupported = append(unsupported, normalized)
			continue
		}
		supported = append(supported, normalized)
	}
	return supported, unsupported
}

// grantTargetIdentifier renders a grant's target the way T-SQL names it.
//
// The SCHEMA:: prefix is the whole point. Without it the server resolves the
// name as an object, so a schema grant written bare lands on a table of the
// same name when one exists, and fails with `Cannot find the object` when it
// does not -- a wrong target either way.
func grantTargetIdentifier(objectType, objectName string) string {
	if strings.EqualFold(strings.TrimSpace(objectType), grantObjectTypeSchema) {
		return "SCHEMA::" + escapeIdentifier(unquoteIdentifier(objectName))
	}
	return escapeQualifiedIdentifier(objectName)
}

// grantObjectTypeSchema is the object type whose target is a schema rather than
// an object inside one.
const grantObjectTypeSchema = "SCHEMA"

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
		if err := refuseDeferrable(column.ForeignKey, column.Name); err != nil {
			return "", err
		}
		parts = append(parts, renderInlineForeignKey(column.ForeignKey))
	}
	return strings.Join(parts, " "), nil
}

// refuseDeferrable refuses a foreign key SQL Server cannot host as written.
//
// Measured on 16.0.4265.3: `DEFERRABLE INITIALLY DEFERRED` on a foreign key is
// `Incorrect syntax near 'DEFERRABLE'`. Rendering the constraint without the
// clause would produce one that rejects exactly the writes the author deferred
// the check for, at apply time on data rather than here on a line of DDL
// (stokaro/ptah#1624).
func refuseDeferrable(ref *ast.ForeignKeyRef, name string) error {
	if !ref.Deferrable && ref.Initially == "" {
		return nil
	}
	return unsupportedFeaturef(
		"sqlserver does not support DEFERRABLE foreign keys; constraint %q declares one", name)
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
		if err := refuseDeferrable(constraint.Reference, constraint.Name); err != nil {
			return "", err
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
		return "", unsupportedFeaturef(
			"trigger %q: BEFORE triggers are not supported; SQL Server offers AFTER and INSTEAD OF", node.Name)
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

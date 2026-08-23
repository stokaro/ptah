// Package oracle renders Ptah AST nodes to Oracle DDL.
package oracle

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
)

const DialectName = platform.Oracle

type Renderer struct {
	w    bufwriter.Writer
	caps capability.Capabilities
}

// New constructs a renderer for the Oracle the offline paths assume, which is
// the newest measured line rather than the oldest supported one, for the reason
// the SQLite renderer gives: a rendered file is read by whatever server the
// operator has, and describing it as the 21 floor would drop guards every
// engine in the ladder above accepts.
func New() *Renderer {
	return NewWithCapabilities(capability.Oracle23())
}

// NewWithCapabilities constructs an Oracle renderer for a concrete server
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

// createGuard returns the IF NOT EXISTS clause the target takes, and the empty
// string when it takes none. The caller decides whether the declaration asked
// for a guard at all.
//
// Measured on 23.26, the guard is a guard rather than a clause the parser
// discards: a second `CREATE TABLE IF NOT EXISTS` of a name that exists is
// accepted, where the same statement without the clause answers ORA-00955.
// Measured on 21.3, every spelling of it is a syntax error while a bare CREATE
// TABLE in the same session is accepted.
func (r *Renderer) createGuard() string {
	if r.capabilities().Has(capability.ObjectExistenceGuards) {
		return " IF NOT EXISTS"
	}
	return ""
}

// dropGuard is createGuard's other half, for DROP.
func (r *Renderer) dropGuard() string {
	if r.capabilities().Has(capability.ObjectExistenceGuards) {
		return " IF EXISTS"
	}
	return ""
}

// VisitCreateSchema refuses, because an Oracle schema is not an object anybody
// creates.
//
// A schema here IS a user: objects live in the namespace of the account that
// owns them, and the statement that makes one is CREATE USER, which is an
// account with a password and a quota rather than the namespace this node
// describes. Oracle does have a CREATE SCHEMA statement and it is a different
// thing entirely -- a way to submit several CREATE and GRANT statements as one
// transaction into a schema that already exists. Measured: `CREATE SCHEMA
// ptah_s` answers ORA-02420, missing schema authorization clause.
//
// Rendering CREATE USER from this node would silently create an account, which
// is a privilege decision no schema file should make on an operator's behalf.
func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	r.notSupported("schemas", node.Name)
	return nil
}

func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	r.notSupported("databases", node.Name)
	return nil
}

// namedColumnCheck gives an unnamed column CHECK the name the comparison will
// look for.
//
// Oracle names an inline CHECK itself -- measured, a column declared
// `view_count NUMBER(10) CHECK (view_count >= 0)` comes back from
// ALL_CONSTRAINTS as SYS_C008794, and the number is per database. The
// comparison looks for the convention an unnamed column check carries
// everywhere else, `<table>_<column>_check`, so the first read-back after
// CREATE TABLE disagrees with the declaration and the next apply renames the
// constraint. The schema converges, but only on the second run.
//
// This is the same defect stokaro/ptah#1716 fixed for SQL Server, whose
// CK__orders__status__571DF1D5 is the same idea with a different hash.
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

	guard := ""
	if node.IfNotExists {
		guard = r.createGuard()
	}

	if len(node.Columns) == 0 && len(node.Constraints) == 0 && node.SelectBody != "" {
		r.w.WriteLinef("CREATE TABLE%s %s AS %s;", guard, escapeQualifiedIdentifier(node.Name), strings.TrimSpace(node.SelectBody))
		return nil
	}

	r.w.WriteLinef("CREATE TABLE%s %s (", guard, escapeQualifiedIdentifier(node.Name))

	if err := singleIdentityColumn(node); err != nil {
		return err
	}
	if err := quotedColumnsAreQuotedInExpressions(node); err != nil {
		return err
	}

	lines := make([]string, 0, len(node.Columns)+len(node.Constraints))
	for _, column := range node.Columns {
		line, err := renderColumn(namedColumnCheck(node.Name, column), r.capabilities())
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

// VisitAlterTable renders Oracle's ALTER TABLE, whose clause names differ from
// the SQL every other dialect here writes.
//
// Measured on 23.26 and 21.3 alike:
//
//	ACCEPTED ALTER TABLE t ADD (c NUMBER)
//	REFUSED  ALTER TABLE t ADD COLUMN c NUMBER      ORA-03050 / ORA-00904
//	ACCEPTED ALTER TABLE t MODIFY (c NUMBER(10))
//	REFUSED  ALTER TABLE t ALTER COLUMN c TYPE NUMBER(10)   ORA-01735
//	ACCEPTED ALTER TABLE t DROP COLUMN c
//
// So the column is added without the word COLUMN and changed with MODIFY, and
// only the drop carries the keyword that the other two refuse.
// singleIdentityColumn refuses a table declaring more than one auto-increment
// column.
//
// Oracle allows exactly one identity column per table -- measured, a CREATE
// TABLE with a second one answers ORA-30669, "table can have only one identity
// column" -- where PostgreSQL and MySQL both allow several SERIAL columns and
// the stub schema in this repository declares three.
//
// The refusal names every column involved rather than only the second, because
// which one to keep is the author's decision and a renderer that picked would
// be choosing which key column silently stops generating values.
func singleIdentityColumn(node *ast.CreateTableNode) error {
	var identity []string
	for _, column := range node.Columns {
		if column != nil && generatesIdentity(column) {
			identity = append(identity, column.Name)
		}
	}
	if len(identity) <= 1 {
		return nil
	}
	return unsupportedFeaturef(
		"table %q declares %d auto-increment columns (%s) and Oracle allows one per table",
		node.Name, len(identity), strings.Join(identity, ", "))
}

// quotedColumnsAreQuotedInExpressions refuses a table whose expressions name a
// quoted column without quoting it. See bareReferenceInExpression for what the
// server answers otherwise, and why the check is a scan rather than a parse.
func quotedColumnsAreQuotedInExpressions(node *ast.CreateTableNode) error {
	expressions := make([]string, 0, len(node.Columns)*2+len(node.Constraints))
	for _, column := range node.Columns {
		if column == nil {
			continue
		}
		expressions = append(expressions, column.Check, column.GeneratedExpression)
		if column.Default != nil {
			expressions = append(expressions, column.Default.Expression)
		}
	}
	for _, constraint := range node.Constraints {
		if constraint != nil {
			expressions = append(expressions, constraint.Expression)
		}
	}

	for _, column := range node.Columns {
		if column == nil {
			continue
		}
		for _, expression := range expressions {
			if !bareReferenceInExpression(column.Name, expression) {
				continue
			}
			return unsupportedFeaturef(
				"column %q of table %q needs quoting in Oracle, and the expression %q names it without quotes; "+
					"an unquoted name folds to upper case and refers to a different column, so write %s in the expression",
				column.Name, node.Name, strings.TrimSpace(expression), escapeIdentifier(column.Name))
		}
	}
	return nil
}

func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	table := escapeQualifiedIdentifier(node.Name)
	for _, operation := range node.Operations {
		switch op := operation.(type) {
		case *ast.AddColumnOperation:
			line, err := renderColumn(op.Column, r.capabilities())
			if err != nil {
				return fmt.Errorf("render added column %s: %w", op.Column.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s ADD (%s);", table, strings.TrimSpace(line))
		case *ast.DropColumnOperation:
			cascade := ""
			if op.Cascade {
				cascade = " CASCADE CONSTRAINTS"
			}
			r.w.WriteLinef("ALTER TABLE %s DROP COLUMN %s%s;", table, escapeIdentifier(op.ColumnName), cascade)
		case *ast.ModifyColumnOperation:
			line, err := renderModifiedColumn(op)
			if err != nil {
				return fmt.Errorf("render modified column %s: %w", op.Column.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s MODIFY (%s);", table, strings.TrimSpace(line))
		case *ast.RenameColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME COLUMN %s TO %s;",
				table, escapeIdentifier(op.OldName), escapeIdentifier(op.NewName))
		case *ast.RenameTableOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME TO %s;", table, escapeIdentifier(op.NewName))
		case *ast.AddConstraintOperation:
			line, err := renderConstraint(op.Constraint)
			if err != nil {
				return fmt.Errorf("render added constraint: %w", err)
			}
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", table, strings.TrimSpace(line))
		case *ast.DropConstraintOperation:
			// No guard here on any line, including the one that takes guards
			// everywhere else. Measured on 23.26: `ALTER TABLE t DROP
			// CONSTRAINT IF EXISTS c` answers ORA-01735, invalid ALTER TABLE
			// option, on the same server that accepts the guard on CREATE
			// TABLE and DROP INDEX. DropConstraintIfExists is false on both
			// presets for that reason, and this is where it is spent.
			guard := ""
			if op.IfExists && r.capabilities().Has(capability.DropConstraintIfExists) {
				guard = "IF EXISTS "
			}
			r.w.WriteLinef("ALTER TABLE %s DROP CONSTRAINT %s%s;", table, guard, escapeIdentifier(op.ConstraintName))
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
	if strings.TrimSpace(node.Condition) != "" {
		// Oracle has no WHERE clause on CREATE INDEX. The equivalent it does
		// have -- a function-based index whose expression is NULL for the rows
		// to leave out -- is a different object with different matching rules,
		// so producing one from this node would apply an index the declaration
		// did not ask for.
		return unsupportedFeaturef("partial indexes are not supported; index %q declares a WHERE condition", node.Name)
	}
	parts := []string{"CREATE"}
	if node.Unique {
		parts = append(parts, "UNIQUE")
	}
	create := "INDEX"
	if node.IfNotExists {
		create += r.createGuard()
	}
	parts = append(parts, create)
	parts = append(parts, escapeQualifiedIdentifier(node.Name), "ON", escapeQualifiedIdentifier(node.Table))
	parts = append(parts, "("+strings.Join(renderIndexParts(node.EffectiveParts()), ", ")+")")
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists && r.capabilities().Has(capability.DropIndexIfExists) {
		guard = " IF EXISTS"
	}
	// Oracle names an index in its own schema-wide namespace rather than under
	// the table, so no ON clause follows: `DROP INDEX <name>` is the whole
	// statement.
	r.w.WriteLinef("DROP INDEX%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitUpsert(_ *ast.UpsertNode) error {
	return unsupportedFeaturef("upsert rendering is not implemented")
}

func (r *Renderer) VisitEnum(_ *ast.EnumNode) error { return nil }

func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	r.w.WriteLinef("-- %s", node.Text)
	return nil
}

func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	for _, name := range node.TableNames() {
		// One statement per table: Oracle's DROP TABLE takes a single name,
		// unlike the comma-separated list PostgreSQL and MySQL accept.
		//
		// PURGE, so the table does not land in the recycle bin. It is not a
		// tidiness preference: a dropped table keeps its dependencies there,
		// and a plan that drops a table and then the domain its column was
		// typed by answers
		//
		//	ORA-11538: The domain DOM_EMAIL to be dropped has dependent
		//	objects in the recycle bin.
		//
		// halfway through -- measured on 23.26.2.0.0, and the first plan
		// stokaro/ptah#1920 produced hit exactly that. The alternative,
		// DROP DOMAIN ... FORCE, was measured too and is worse: with a LIVE
		// dependent it succeeds and silently untypes the column, so a NOT NULL
		// the domain enforced is gone and nobody asked. Purging here keeps
		// that refusal (ORA-11502) for the case that deserves it.
		//
		// It is also what internal/dbschema/oracle's own cleanup does, for the
		// storage half of the same reason.
		r.w.WriteLinef("DROP TABLE%s %s PURGE;", guard, escapeQualifiedIdentifier(name))
	}
	return nil
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	// A domain is rendered where the preset says so -- 23 has CREATE DOMAIN
	// and 21 answers ORA-00901 (stokaro/ptah#1920).
	if domain, isDomain := node.TypeDef.(*ast.DomainTypeDef); isDomain && r.domainsRendered() {
		return r.visitCreateDomain(node, domain)
	}
	// Oracle 23 also has a real CREATE TYPE ... AS OBJECT, and this renderer
	// emits none yet, which is what CompositeTypes reads false for on both
	// presets. It refuses rather than writing a comment: a comment makes
	// `schema render` exit 0 on a model the planner refuses at apply time,
	// which is the reason the SQLite renderer stopped commenting its
	// materialized views.
	return unsupportedFeaturef("CREATE TYPE %s: user types are not rendered for Oracle", node.Name)
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	return unsupportedFeaturef("ALTER TYPE %s: user types are not rendered for Oracle", node.Name)
}

func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	if node.Domain && r.domainsRendered() {
		return r.visitDropDomain(node)
	}
	return unsupportedFeaturef("DROP TYPE %s: user types are not rendered for Oracle", node.Name)
}

func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	r.notSupported("extensions", node.Name)
	return nil
}

func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	r.notSupported("DROP EXTENSION", node.Name)
	return nil
}

func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	create := "CREATE SEQUENCE"
	if node.IfNotExists {
		create += r.createGuard()
	}
	parts := []string{create, escapeQualifiedIdentifier(node.Name)}
	parts = append(parts, sequenceOptions(node.Start, node.Increment, node.MinValue, node.MaxValue, &node.Cycle)...)
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.Start != nil {
		// ALTER SEQUENCE ... START WITH is not how Oracle moves a live
		// counter, and emitting it would either be refused or reset something
		// the declaration did not ask to reset. Refusing is the answer that
		// cannot silently renumber a key column.
		return unsupportedFeaturef("ALTER SEQUENCE %s: changing the start counter is not rendered for Oracle", node.Name)
	}
	options := sequenceOptions(nil, node.Increment, node.MinValue, node.MaxValue, node.Cycle)
	if len(options) == 0 {
		return nil
	}
	r.w.WriteLinef("ALTER SEQUENCE %s %s;", escapeQualifiedIdentifier(node.Name), strings.Join(options, " "))
	return nil
}

func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP SEQUENCE%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

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

func (r *Renderer) VisitDropView(node *ast.DropViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP VIEW%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("CREATE MATERIALIZED VIEW %s AS", escapeQualifiedIdentifier(node.Name))
	r.w.WriteLine(strings.TrimSpace(node.Body))
	r.w.WriteLine(";")
	return nil
}

func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP MATERIALIZED VIEW%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

// VisitRefreshMaterializedView renders Oracle's refresh, which is a procedure
// call rather than a statement.
func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	if node.Concurrently {
		return unsupportedFeaturef("REFRESH MATERIALIZED VIEW %s: CONCURRENTLY is not supported", node.Name)
	}
	r.w.WriteLinef("BEGIN DBMS_MVIEW.REFRESH(%s); END;", escapeStringLiteral(node.Name))
	return nil
}

func (r *Renderer) VisitAlterMaterializedViewRefresh(node *ast.AlterMaterializedViewRefreshNode) error {
	return unsupportedFeaturef("ALTER MATERIALIZED VIEW %s: changing a refresh policy is not rendered for Oracle", node.Name)
}

// VisitCreateTrigger renders the trigger header; the body is PL/SQL the
// declaration supplies.
func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	create := "CREATE TRIGGER"
	if node.Replace {
		if !r.capabilities().Has(capability.CreateOrReplaceTrigger) {
			return unsupportedFeaturef("the target Oracle does not support CREATE OR REPLACE TRIGGER; trigger %q asks for it", node.Name)
		}
		create = "CREATE OR REPLACE TRIGGER"
	}
	forEach := strings.ToUpper(strings.TrimSpace(node.ForEach))
	if forEach == "" {
		forEach = "ROW"
	}
	if forEach != "ROW" {
		return unsupportedFeaturef("FOR EACH %s triggers are not supported", forEach)
	}
	body := strings.TrimSpace(node.Body)
	r.w.WriteLinef("%s %s %s %s ON %s FOR EACH ROW",
		create,
		escapeQualifiedIdentifier(node.Name),
		strings.TrimSpace(node.Timing),
		strings.TrimSpace(node.Event),
		escapeQualifiedIdentifier(node.Table),
	)
	r.w.WriteLine(body)
	return nil
}

func (r *Renderer) VisitDropTrigger(node *ast.DropTriggerNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// No guard: Oracle has no IF EXISTS on DROP TRIGGER on either measured
	// line, unlike the index, sequence, view and table drops above.
	r.w.WriteLinef("DROP TRIGGER %s;", escapeQualifiedIdentifier(node.Name))
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

// VisitCreateRole renders Oracle's CREATE ROLE, and refuses a declaration that
// describes a user rather than a role.
//
// Oracle's CREATE ROLE takes none of the attributes ast.CreateRoleNode carries
// from PostgreSQL. There, one statement makes a thing that can hold privileges
// AND a thing that can log in; here those are two objects, and the one that
// logs in is a USER. So a declaration carrying LOGIN, a password, or any of the
// PostgreSQL capability flags is refused rather than rendered: `CREATE ROLE app`
// would be accepted by the server and would not be what was declared, which is
// the failure this repository keeps finding -- a statement the engine accepts is
// not evidence it did what was asked.
//
// No IF NOT EXISTS guard, measured on 23.26.2.0.0: a second CREATE ROLE answers
// ORA-01921, and the clause is not accepted (stokaro/ptah#1920).
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	if attribute := oracleUserOnlyRoleAttribute(node); attribute != "" {
		return unsupportedFeaturef(
			"role %q declares %s, which in Oracle describes a USER rather than a ROLE; "+
				"CREATE ROLE would be accepted and would not create what was declared",
			node.Name, attribute)
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("CREATE ROLE %s;", escapeIdentifier(node.Name))
	return nil
}

// oracleUserOnlyRoleAttribute names the first attribute on the declaration that
// Oracle can only satisfy with a user, or "" when the role is a plain one.
func oracleUserOnlyRoleAttribute(node *ast.CreateRoleNode) string {
	switch {
	case node.Login:
		return "LOGIN"
	case node.Password != "":
		return "a password"
	case node.Superuser:
		return "SUPERUSER"
	case node.CreateDB:
		return "CREATEDB"
	case node.CreateRole:
		return "CREATEROLE"
	default:
		return ""
	}
}

// VisitDropRole renders DROP ROLE, unguarded.
//
// Measured on 23.26.2.0.0: dropping an absent role answers ORA-01919, and
// Oracle has no IF EXISTS on this statement -- the same shape DROP TRIGGER
// carries above.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("DROP ROLE %s;", escapeIdentifier(node.Name))
	return nil
}

// VisitAlterRole stays refused, and the reason is not that Oracle lacks the
// statement.
//
// Oracle has ALTER ROLE, and it changes how the role is AUTHENTICATED --
// IDENTIFIED BY, EXTERNALLY, GLOBALLY. It cannot change the capability flags
// ast.AlterRoleNode carries, because a role has none of them. Rendering it
// would answer a different question than the one asked.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	r.notSupported("ALTER ROLE", node.Name)
	return nil
}

// VisitGrantPrivilege renders both grant shapes Oracle has: an object privilege
// with ON, and a system privilege without it.
//
// WITH GRANT OPTION is refused rather than emitted, and the refusal is the
// engine's: measured on 23.26.2.0.0,
// `GRANT SELECT, INSERT ON t TO r WITH GRANT OPTION` answers
// `ORA-01926: A role cannot be granted a privilege with the WITH GRANT OPTION`.
// Emitting it would render a statement the server refuses, which is worse than
// refusing it here -- the plan would fail halfway through.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	if node.WithOption {
		return unsupportedFeaturef(
			"grant to role %q carries WITH GRANT OPTION, which Oracle refuses for a role "+
				"(ORA-01926); grant it to a user instead", node.Role)
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("GRANT %s%s TO %s;",
		strings.Join(node.Privileges, ", "),
		oracleGrantTarget(node.ObjectName),
		escapeIdentifier(node.Role))
	return nil
}

// VisitRevokePrivilege mirrors the grant, with the same two shapes.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("REVOKE %s%s FROM %s;",
		strings.Join(node.Privileges, ", "),
		oracleGrantTarget(node.ObjectName),
		escapeIdentifier(node.Role))
	return nil
}

// oracleGrantTarget renders the ON clause, or nothing for a system privilege.
//
// A system privilege such as CREATE SESSION names no object, and `GRANT CREATE
// SESSION ON  TO r` is a syntax error rather than a harmless extra space.
func oracleGrantTarget(object string) string {
	if strings.TrimSpace(object) == "" {
		return ""
	}
	return " ON " + escapeQualifiedIdentifier(object)
}

// VisitExtendedProperty names the property as skipped: an extended property is
// SQL Server's own object, and Oracle has no catalog to attach one to.
//
// The nearest Oracle construct is COMMENT ON, which Ptah already models as an
// object comment, and rendering a property as a comment would put a named
// value nobody can read back into the one slot the comment already owns.
//
// Skipped rather than refused, which is the difference between this and
// VisitAlterRole above. A refusal fails the whole render, and
// goschema.ExtendedProperty carries no dialect scope -- exactly as
// goschema.Synonym does not -- so refusing here would make one schema
// renderable on five targets and fatal on the sixth. Every other renderer
// writes this comment; Oracle answering differently would be the asymmetry,
// not the consistency.
func (r *Renderer) VisitExtendedProperty(node *ast.ExtendedPropertyNode) error {
	r.w.WriteLinef("-- ORACLE: extended property %q is not supported", node.Name)
	return nil
}

// VisitCreateSynonym renders Oracle's own object: a synonym is a native Oracle
// concept rather than a compatibility shim.
func (r *Renderer) VisitCreateSynonym(node *ast.CreateSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("CREATE SYNONYM %s FOR %s;",
		escapeQualifiedIdentifier(node.Name), escapeQualifiedIdentifier(node.Target))
	return nil
}

func (r *Renderer) VisitDropSynonym(node *ast.DropSynonymNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP SYNONYM%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	r.w.WriteLine(strings.TrimSpace(node.SQL))
	return nil
}

func (r *Renderer) notSupported(feature, name string) {
	r.w.WriteLinef("-- ORACLE: %s %q is not supported", feature, name)
}

// refuses reports whether the target declines the capability a statement needs,
// writing the named skip comment when it does.
//
// A comment rather than an error, because one schema is applied across several
// dialects: a declaration the target cannot host is named in the plan and the
// rest of the plan still runs. An error is kept for a declaration this target
// COULD host but only by writing something other than what was asked.
func (r *Renderer) refuses(key capability.Capability, kind, name string) bool {
	if r.capabilities().Has(key) {
		return false
	}
	r.notSupported(strings.ToUpper(kind), name)
	return true
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: oracle: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

package clickhouse

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
)

// VisitNode renders node, or reports why it cannot.
//
// The switch is this renderer's whole decision table. Every concrete node kind
// in [ptah.run/core/ast] appears in it, so what ClickHouse makes of a kind is
// readable in one place: the DDL it writes, the skip comment it writes instead,
// and the refusal that aborts the render.
// [ptah.run/internal/astrouteguard] reads the cases and fails the build when a
// kind is missing. That gate is the only place the question is asked: a visitor
// carrying one method gives the compiler nothing to check a dispatch table
// against, and the default arm below answers at run time, one render too late.
//
// What a handler decides is not part of the table. Several of them read the
// capability set first and write a named skip where the target withholds the
// key, so one arm here covers both outcomes and the arm says nothing about
// which one a given target gets.
//
//nolint:gocyclo // a dispatch table's arms are not branching complexity, and collapsing them is what would hide a missing kind
func (r *Renderer) VisitNode(node ast.Node) error {
	if nodedispatch.IsAbsent(node) {
		return r.nilNode()
	}

	switch n := node.(type) {
	// Tables, columns and the objects that hang off them.
	case *ast.CreateTableNode:
		return r.renderCreateTable(n)
	case *ast.AlterTableNode:
		return r.renderAlterTable(n)
	case *ast.DropTableNode:
		return r.renderDropTable(n)
	case *ast.ColumnNode:
		return r.renderColumnNode(n)
	case *ast.ConstraintNode:
		return r.renderConstraint(n)
	case *ast.IndexNode:
		return r.renderIndex(n)
	case *ast.DropIndexNode:
		return r.renderDropIndex(n)
	case *ast.CommentNode:
		return r.renderComment(n)
	case *ast.ObjectCommentNode:
		return r.renderObjectComment(n)

	// Schemas and databases. ClickHouse has one object for both, so the two
	// kinds reach two handlers that write the same statement.
	case *ast.CreateSchemaNode:
		return r.renderCreateSchema(n)
	case *ast.CreateDatabaseNode:
		return r.renderCreateDatabase(n)

	// User-defined types. An enumeration is an Enum8 or Enum16 column type
	// here, so the standalone type statements are named and skipped.
	case *ast.EnumNode:
		return r.renderEnum(n)
	case *ast.CreateTypeNode:
		return r.renderCreateType(n)
	case *ast.AlterTypeNode:
		return r.renderAlterType(n)
	case *ast.DropTypeNode:
		return r.renderDropType(n)

	// Views. A materialized view stores its result and is dropped with DROP
	// VIEW, so the four kinds do not share one spelling.
	case *ast.CreateViewNode:
		return r.renderCreateView(n)
	case *ast.DropViewNode:
		return r.renderDropView(n)
	case *ast.CreateMaterializedViewNode:
		return r.renderCreateMaterializedView(n)
	case *ast.DropMaterializedViewNode:
		return r.renderDropMaterializedView(n)
	case *ast.RefreshMaterializedViewNode:
		return r.renderRefreshMaterializedView(n)
	case *ast.AlterMaterializedViewRefreshNode:
		return r.renderAlterMaterializedViewRefresh(n)

	// Routines and triggers. ClickHouse has user-defined functions with a
	// syntax these nodes cannot express, and no triggers at all.
	case *ast.CreateFunctionNode:
		return r.renderCreateFunction(n)
	case *ast.DropFunctionNode:
		return r.renderDropFunction(n)
	case *ast.CreateTriggerNode:
		return r.renderCreateTrigger(n)
	case *ast.DropTriggerNode:
		return r.renderDropTrigger(n)

	// Sequences. There is no standalone sequence object, so each is named and
	// skipped.
	case *ast.CreateSequenceNode:
		return r.renderCreateSequence(n)
	case *ast.AlterSequenceNode:
		return r.renderAlterSequence(n)
	case *ast.DropSequenceNode:
		return r.renderDropSequence(n)

	// Roles and privileges. ClickHouse has both, with a syntax of its own, so
	// rbac.go renders them and refuses only what the server cannot represent.
	case *ast.CreateRoleNode:
		return r.renderCreateRole(n)
	case *ast.AlterRoleNode:
		return r.renderAlterRole(n)
	case *ast.DropRoleNode:
		return r.renderDropRole(n)
	case *ast.GrantPrivilegeNode:
		return r.renderGrantPrivilege(n)
	case *ast.RevokePrivilegeNode:
		return r.renderRevokePrivilege(n)
	case *ast.DefaultPrivilegeNode:
		return r.renderDefaultPrivilege(n)
	case *ast.RevokeDefaultPrivilegeNode:
		return r.renderRevokeDefaultPrivilege(n)

	// Row-level security. A row policy is a ClickHouse object; the table-level
	// switch the two RLS kinds describe is not.
	case *ast.CreatePolicyNode:
		return r.renderCreatePolicy(n)
	case *ast.DropPolicyNode:
		return r.renderDropPolicy(n)
	case *ast.AlterTableEnableRLSNode:
		return r.renderAlterTableEnableRLS(n)
	case *ast.AlterTableDisableRLSNode:
		return r.renderAlterTableDisableRLS(n)
	case *ast.AlterTableForceRLSNode:
		return r.renderAlterTableForceRLS(n)

	// Objects another engine owns. Each is named and skipped so the reader of
	// a render sees what the target left out.
	case *ast.ExtensionNode:
		return r.renderExtension(n)
	case *ast.DropExtensionNode:
		return r.renderDropExtension(n)
	case *ast.CreateSynonymNode:
		return r.renderCreateSynonym(n)
	case *ast.DropSynonymNode:
		return r.renderDropSynonym(n)
	case *ast.ExtendedPropertyNode:
		return r.renderExtendedProperty(n)
	case *ast.CreateHypertableNode:
		return r.renderCreateHypertable(n)
	case *ast.CreateContinuousAggregateNode:
		return r.renderCreateContinuousAggregate(n)
	case *ast.DropContinuousAggregateNode:
		return r.renderDropContinuousAggregate(n)

	// Data manipulation.
	case *ast.UpsertNode:
		return r.renderUpsert(n)

	// Literal SQL. A routine node carries a body this renderer does not parse,
	// so each one lowers to the raw-SQL handler with the statement it holds.
	case *ast.RawSQLNode:
		return r.renderRawSQL(n)
	case *ast.MySQLRoutineNode:
		return r.renderRawSQL(&ast.RawSQLNode{SQL: n.SQL})
	case *ast.OpaqueRoutineNode:
		return r.renderRawSQL(&ast.RawSQLNode{SQL: n.SQL})
	case *ast.PostgresDoBlockNode:
		return r.renderRawSQL(&ast.RawSQLNode{SQL: n.SQL})
	case *ast.PostgresRoutineNode:
		return r.renderRawSQL(&ast.RawSQLNode{SQL: n.SQL})
	case *ast.SQLServerRoutineNode:
		return r.renderRawSQL(&ast.RawSQLNode{SQL: n.SQL})

	case *ast.StatementList:
		return r.renderStatementList(n)

	// An operation and a type definition are parts of a statement, not
	// statements. Each is read out of the ALTER or the CREATE TYPE that carries
	// it, so one reaching this renderer alone names no table and no type and
	// there is nothing to render it against.
	case *ast.AddColumnOperation,
		*ast.DropColumnOperation,
		*ast.ModifyColumnOperation,
		*ast.RenameColumnOperation,
		*ast.AlterGeneratedColumnExpressionOperation,
		*ast.AlterColumnOperation,
		*ast.AddConstraintOperation,
		*ast.ValidateConstraintOperation,
		*ast.DropConstraintOperation,
		*ast.RenameConstraintOperation,
		*ast.AddIndexOperation,
		*ast.AddSkippingIndexOperation,
		*ast.RenameTableOperation,
		*ast.SetCommentOperation,
		*ast.SetConstraintCommentOperation,
		*ast.ModifyTTLOperation,
		*ast.SetRowTTLOperation,
		*ast.ResetRowTTLOperation,
		*ast.SetRowDeletionPolicyOperation,
		*ast.DropRowDeletionPolicyOperation,
		*ast.AddEnumValueOperation,
		*ast.RenameEnumValueOperation,
		*ast.RenameTypeOperation,
		*ast.CompositeAttributeOperation,
		*ast.DomainConstraintOperation,
		*ast.DomainDefaultOperation,
		*ast.DomainNotNullOperation,
		*ast.EnumTypeDef,
		*ast.DomainTypeDef,
		*ast.CompositeTypeDef,
		*ast.RangeTypeDef:
		return r.nodeNeedsParent(node)

	default:
		return r.unknownNode(node)
	}
}

// renderStatementList emits the list in order and stops at the first failure.
//
// Each statement goes back through [Renderer.VisitNode] rather than straight to
// a handler, so a statement that is itself a list, a nil, or a kind this
// renderer refuses gets the same answer inside a list as outside one.
func (r *Renderer) renderStatementList(list *ast.StatementList) error {
	for _, statement := range list.Statements {
		if err := r.VisitNode(statement); err != nil {
			return err
		}
	}
	return nil
}

// nilNode refuses a node that is not there.
func (r *Renderer) nilNode() error {
	return fmt.Errorf("%w: %s: the AST node is nil", ptaherr.ErrInvalidSchemaDiff, DialectName)
}

// nodeNeedsParent refuses a node that renders only as part of a larger
// statement, naming the kind that arrived alone.
func (r *Renderer) nodeNeedsParent(node ast.Node) error {
	return fmt.Errorf("%w: %s: %T renders as part of the statement that carries it, not on its own",
		ptaherr.ErrInvalidSchemaDiff, DialectName, node)
}

// unknownNode refuses a node kind the switch above does not name.
//
// It never returns nil. A kind that produced no output and no error would be
// indistinguishable from one this target deliberately skips, and the skips are
// the comments the handlers write.
func (r *Renderer) unknownNode(node ast.Node) error {
	return fmt.Errorf("%w: %s: %T has no handler in this renderer",
		ptaherr.ErrUnsupportedFeature, DialectName, node)
}

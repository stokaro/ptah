package sqlite

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
)

// VisitNode renders node.
//
// The switch is this renderer's whole decision table, and every concrete node
// kind appears in it. [ptah.run/internal/astrouteguard] reads the cases and
// fails the build when a kind is missing, which is the completeness a
// one-method [ast.Visitor] cannot ask the compiler for.
//
// A kind this target has no object for is routed all the same: it reaches a
// handler that names it in the output and records the omission, so the
// declaration the author wrote is reported rather than dropped.
func (r *Renderer) VisitNode(node ast.Node) error { //nolint:gocyclo // one case per AST node kind is the decision table, and splitting it would put the completeness the routing gate reads in two places
	if nodedispatch.IsAbsent(node) {
		return r.nilNode()
	}
	switch n := node.(type) {
	// A list is not a statement, so nothing is written for the list itself and
	// each statement decides its own output. The wrapper above this renderer
	// prepares a whole list before any of it renders; a list reaching here has
	// already been through that.
	case *ast.StatementList:
		return r.renderStatementList(n)

	case *ast.CreateTableNode:
		return r.renderCreateTable(n)
	case *ast.CreateSchemaNode:
		return r.renderCreateSchema(n)
	case *ast.CreateDatabaseNode:
		return r.renderCreateDatabase(n)
	case *ast.AlterTableNode:
		return r.renderAlterTable(n)
	case *ast.ColumnNode:
		return r.renderColumn(n)
	case *ast.ConstraintNode:
		return r.renderConstraint(n)
	case *ast.IndexNode:
		return r.renderIndex(n)
	case *ast.DropIndexNode:
		return r.renderDropIndex(n)
	case *ast.EnumNode:
		return r.renderEnum(n)
	case *ast.CreateTypeNode:
		return r.renderCreateType(n)
	case *ast.AlterTypeNode:
		return r.renderAlterType(n)
	case *ast.CommentNode:
		return r.renderComment(n)
	case *ast.DropTableNode:
		return r.renderDropTable(n)
	case *ast.DropTypeNode:
		return r.renderDropType(n)
	case *ast.ExtensionNode:
		return r.renderExtension(n)
	case *ast.DropExtensionNode:
		return r.renderDropExtension(n)
	case *ast.CreateFunctionNode:
		return r.renderCreateFunction(n)
	case *ast.DropFunctionNode:
		return r.renderDropFunction(n)
	case *ast.CreateSequenceNode:
		return r.renderCreateSequence(n)
	case *ast.AlterSequenceNode:
		return r.renderAlterSequence(n)
	case *ast.DropSequenceNode:
		return r.renderDropSequence(n)
	case *ast.CreateViewNode:
		return r.renderCreateView(n)
	case *ast.DropViewNode:
		return r.renderDropView(n)
	case *ast.CreateSynonymNode:
		return r.renderCreateSynonym(n)
	case *ast.CreateHypertableNode:
		return r.renderCreateHypertable(n)
	case *ast.CreateContinuousAggregateNode:
		return r.renderCreateContinuousAggregate(n)
	case *ast.DropContinuousAggregateNode:
		return r.renderDropContinuousAggregate(n)
	case *ast.DropSynonymNode:
		return r.renderDropSynonym(n)
	case *ast.ExtendedPropertyNode:
		return r.renderExtendedProperty(n)
	case *ast.CreateMaterializedViewNode:
		return r.renderCreateMaterializedView(n)
	case *ast.DropMaterializedViewNode:
		return r.renderDropMaterializedView(n)
	case *ast.AlterMaterializedViewRefreshNode:
		return r.renderAlterMaterializedViewRefresh(n)
	case *ast.RefreshMaterializedViewNode:
		return r.renderRefreshMaterializedView(n)
	case *ast.CreateTriggerNode:
		return r.renderCreateTrigger(n)
	case *ast.DropTriggerNode:
		return r.renderDropTrigger(n)
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
	case *ast.CreateRoleNode:
		return r.renderCreateRole(n)
	case *ast.DropRoleNode:
		return r.renderDropRole(n)
	case *ast.AlterRoleNode:
		return r.renderAlterRole(n)
	case *ast.GrantPrivilegeNode:
		return r.renderGrantPrivilege(n)
	case *ast.RevokePrivilegeNode:
		return r.renderRevokePrivilege(n)
	case *ast.DefaultPrivilegeNode:
		return r.renderDefaultPrivilege(n)
	case *ast.RevokeDefaultPrivilegeNode:
		return r.renderRevokeDefaultPrivilege(n)
	case *ast.RawSQLNode:
		return r.renderRawSQL(n)
	case *ast.UpsertNode:
		return r.renderUpsert(n)

	// A routine node keeps its dialect metadata for the parser and the readers,
	// and renders as the statement it wraps. The body is preserved verbatim, so
	// the raw-SQL handler is the whole rendering of it.
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

	// An alter operation, a type operation and a type definition are parts of a
	// statement rather than statements. None carries the object it applies to,
	// so the renderer that emits one reads the name out of the ALTER TABLE,
	// ALTER TYPE or CREATE TYPE around it. Reached alone there is nothing to
	// render it against.
	case *ast.AddColumnOperation,
		*ast.AddConstraintOperation,
		*ast.ValidateConstraintOperation,
		*ast.AddIndexOperation,
		*ast.AddSkippingIndexOperation,
		*ast.AlterGeneratedColumnExpressionOperation,
		*ast.DropColumnOperation,
		*ast.DropConstraintOperation,
		*ast.DropRowDeletionPolicyOperation,
		*ast.ModifyColumnOperation,
		*ast.ModifyTTLOperation,
		*ast.RenameColumnOperation,
		*ast.RenameConstraintOperation,
		*ast.RenameTableOperation,
		*ast.ResetRowTTLOperation,
		*ast.SetCommentOperation,
		*ast.SetRowDeletionPolicyOperation,
		*ast.SetRowTTLOperation,
		*ast.AddEnumValueOperation,
		*ast.CompositeAttributeOperation,
		*ast.DomainConstraintOperation,
		*ast.DomainDefaultOperation,
		*ast.DomainNotNullOperation,
		*ast.RenameEnumValueOperation,
		*ast.RenameTypeOperation,
		*ast.CompositeTypeDef,
		*ast.DomainTypeDef,
		*ast.EnumTypeDef,
		*ast.RangeTypeDef:
		return r.nodeNeedsParent(node)

	default:
		return r.unknownNode(node)
	}
}

// renderStatementList emits each statement in order and stops at the first
// refusal.
//
// Each statement goes back through [Renderer.VisitNode] rather than through its
// own Accept, so a nil element in the list is answered the same way a nil node
// handed to the renderer is, instead of dereferencing it.
func (r *Renderer) renderStatementList(list *ast.StatementList) error {
	for _, statement := range list.Statements {
		if err := r.VisitNode(statement); err != nil {
			return fmt.Errorf("error visiting statement: %w", err)
		}
	}
	return nil
}

// nodeNeedsParent refuses a node that is part of a statement rather than one.
//
// Answering with silence instead would write nothing and report nothing, which
// is what a deliberate skip looks like: the caller would read a successful
// render of an empty string and never learn the declaration went nowhere.
func (r *Renderer) nodeNeedsParent(node ast.Node) error {
	return unsupportedFeaturef("%T is rendered by the statement that carries it, not on its own", node)
}

// unknownNode refuses a node kind this renderer routes nowhere.
//
// The error names the concrete type, because the reader has to find the kind in
// order to add a case for it, and a renderer is the last place that can still
// say which node was dropped.
func (r *Renderer) unknownNode(node ast.Node) error {
	return unsupportedFeaturef("%T reaches no case in this renderer", node)
}

// nilNode is the answer to a node that is not there.
func (r *Renderer) nilNode() error {
	return &ptaherr.RenderError{
		Dialect: DialectName,
		Err:     ptaherr.ErrInvalidSchemaDiff,
		Message: DialectName + ": AST node is nil",
	}
}

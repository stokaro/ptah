package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
	"ptah.run/internal/ydbgap"
)

// defaultPrivilegeReason is why YDB refuses a default privilege.
const defaultPrivilegeReason = "YDB has no default privileges; a permission granted on a directory " +
	"is inherited by every object created in it"

// VisitNode renders node, or reports why YDB cannot hold it.
//
// The switch is this renderer's whole decision table: every concrete node kind
// in [ptah.run/core/ast] appears in it, and
// [ptah.run/internal/astrouteguard] fails the build when one is missing. A kind
// YDB has no statement for is refused here, by its capability key where one
// exists and by the phase that implements it where the family is planned.
// Nothing is answered with a skip comment: a comment lets an apply exit 0
// without the object.
//
//nolint:gocyclo // a dispatch table's arms are not branching complexity, and collapsing them is what would hide a missing kind
func (r *Renderer) VisitNode(node ast.Node) error {
	if nodedispatch.IsAbsent(node) {
		return fmt.Errorf("%w: %s: the AST node is nil", ptaherr.ErrInvalidSchemaDiff, DialectName)
	}

	switch n := node.(type) {
	// Tables, columns and indexes: what this renderer writes.
	case *ast.CreateTableNode:
		return r.renderCreateTable(n)
	case *ast.AlterTableNode:
		return r.renderAlterTable(n)
	case *ast.DropTableNode:
		return r.renderDropTable(n)
	case *ast.ColumnNode:
		return r.renderColumnNode(n)
	case *ast.ConstraintNode:
		return r.renderConstraintNode(n)
	case *ast.IndexNode:
		return r.renderIndex(n)
	case *ast.DropIndexNode:
		return r.renderDropIndex(n)
	case *ast.AlterIndexNode:
		// PostgreSQL's ALTER INDEX ... RENAME TO names no table, and YDB
		// renames an index through its table.
		return refuseFact("ALTER INDEX "+n.Name+" RENAME TO "+n.NewName,
			"YDB renames an index with ALTER TABLE ... RENAME INDEX, which needs the table this statement does not name")
	case *ast.CommentNode:
		return r.renderComment(n)
	case *ast.ObjectCommentNode:
		return refuseGap(ydbgap.Comments, "COMMENT ON "+string(n.Object)+" "+n.Name)

	// A Ptah schema is a directory on YDB, and no SQL creates one: a table
	// path names its directories and YDB creates them with the table
	// (measured: `CREATE DIRECTORY` is a parse error, and `CREATE TABLE
	// `dir/sub/t`` creates dir and sub). So a schema renders no statement.
	case *ast.CreateSchemaNode:
		return nil
	// No SQL creates a YDB database either: measured on 26.2.1.14 and
	// 25.1.4.7, `CREATE DATABASE` is a parse error. A cluster's databases
	// are its administrators', and a run that needs a database of its own
	// gets a dev realm instead (internal/ydbrealm).
	case *ast.CreateDatabaseNode:
		return refuseFact("CREATE DATABASE "+n.Name,
			"YDB has no CREATE DATABASE statement; a YDB database is created by the cluster's administrators")

	// User-defined types. YDB has none: CREATE TYPE and CREATE DOMAIN are
	// parse errors, and an enum is neither a column type nor a named type.
	case *ast.EnumNode:
		return r.keyed(capability.EnumCustomType, "enum type", "enum "+n.Name)
	case *ast.CreateTypeNode:
		return refuseKey(typeKey(n), typeSubject(n.Name))
	case *ast.AlterTypeNode:
		return refuseKey(capability.EnumCustomType, strings.TrimSpace("ALTER TYPE "+n.Name))
	case *ast.DropTypeNode:
		return refuseKey(capability.EnumCustomType, "DROP TYPE "+n.Name)

	// Views. A materialized view does not exist.
	case *ast.CreateViewNode:
		return r.renderCreateView(n)
	case *ast.DropViewNode:
		return r.renderDropView(n)
	case *ast.CreateMaterializedViewNode:
		return r.keyed(capability.MaterializedViews, "materialized view", "materialized view "+n.Name)
	case *ast.DropMaterializedViewNode:
		return r.keyed(capability.MaterializedViews, "materialized view", "DROP MATERIALIZED VIEW "+n.Name)
	case *ast.RefreshMaterializedViewNode:
		return r.keyed(capability.MaterializedViews, "materialized view", "REFRESH MATERIALIZED VIEW "+n.Name)
	case *ast.AlterMaterializedViewRefreshNode:
		return r.keyed(capability.MaterializedViews, "materialized view", "the refresh schedule of "+n.Name)

	// Routines and triggers do not exist.
	case *ast.CreateFunctionNode:
		return r.keyed(capability.Functions, "function", "function "+n.Name)
	case *ast.DropFunctionNode:
		return r.keyed(capability.Functions, "function", "DROP FUNCTION "+n.Name)
	case *ast.CreateTriggerNode:
		return r.keyed(capability.Triggers, "trigger", "trigger "+n.Name)
	case *ast.DropTriggerNode:
		return r.keyed(capability.Triggers, "trigger", "DROP TRIGGER "+n.Name)

	// A sequence exists only behind a Serial column, and ALTER SEQUENCE on
	// that one is the only sequence statement YDB has: CREATE SEQUENCE and
	// DROP SEQUENCE are parse errors on every line.
	case *ast.CreateSequenceNode:
		return r.keyed(capability.Sequences, "sequence", "sequence "+n.Name)
	case *ast.AlterSequenceNode:
		return r.keyed(capability.Sequences, "sequence", "ALTER SEQUENCE "+n.Name)
	case *ast.DropSequenceNode:
		return r.keyed(capability.Sequences, "sequence", "DROP SEQUENCE "+n.Name)
	case *ast.AlterSerialSequenceNode:
		return r.renderAlterSerialSequence(n)

	// Users, groups, memberships and permissions: YDB's own access model.
	// Default privileges do not exist, because a permission on a directory
	// is inherited by what is created in it.
	case *ast.CreateRoleNode:
		return r.renderCreateRole(n)
	case *ast.AlterRoleNode:
		return r.renderAlterRole(n)
	case *ast.DropRoleNode:
		return r.renderDropRole(n)
	case *ast.GrantRoleMembershipNode:
		return r.renderRoleMembership(n.Role, n.Member, n.Comment, "ADD USER",
			fmt.Sprintf("adding %s to group %s", n.Member, n.Role))
	case *ast.RevokeRoleMembershipNode:
		return r.renderRoleMembership(n.Role, n.Member, n.Comment, "DROP USER",
			fmt.Sprintf("dropping %s from group %s", n.Member, n.Role))
	case *ast.GrantPrivilegeNode:
		return r.renderGrantPrivilege(n)
	case *ast.RevokePrivilegeNode:
		return r.renderRevokePrivilege(n)
	case *ast.DefaultPrivilegeNode:
		return refuseFact("default privileges for "+n.Grantor, defaultPrivilegeReason)
	case *ast.RevokeDefaultPrivilegeNode:
		return refuseFact("revoked default privileges for "+n.Grantor, defaultPrivilegeReason)

	// Row-level security does not exist.
	case *ast.CreatePolicyNode:
		return r.keyed(capability.RowLevelSecurity, "row-level security", "policy "+n.Name)
	case *ast.DropPolicyNode:
		return r.keyed(capability.RowLevelSecurity, "row-level security", "DROP POLICY "+n.Name)
	case *ast.AlterTableEnableRLSNode:
		return r.keyed(capability.RowLevelSecurity, "row-level security", "row-level security on "+n.Table)
	case *ast.AlterTableDisableRLSNode:
		return r.keyed(capability.RowLevelSecurity, "row-level security", "row-level security on "+n.Table)
	case *ast.AlterTableForceRLSNode:
		return r.keyed(capability.RowLevelSecurity, "row-level security", "forced row-level security on "+n.Table)

	// Objects of other engines.
	case *ast.CreateSynonymNode:
		return refuseFact("synonym "+n.Name, "YDB has no synonyms")
	case *ast.DropSynonymNode:
		return refuseFact("DROP SYNONYM "+n.Name, "YDB has no synonyms")
	case *ast.ExtensionNode:
		return refuseFact("extension "+n.Name, "YDB has no extensions")
	case *ast.DropExtensionNode:
		return refuseFact("DROP EXTENSION "+n.Name, "YDB has no extensions")
	case *ast.ExtendedPropertyNode:
		return refuseFact("extended property "+n.Name, "extended properties are SQL Server's")
	case *ast.CreateHypertableNode:
		return r.keyed(capability.Hypertables, "hypertable", "hypertable "+n.Table)
	case *ast.CreateContinuousAggregateNode:
		return r.keyed(capability.ContinuousAggregates, "continuous aggregate", "continuous aggregate "+n.Name)
	case *ast.DropContinuousAggregateNode:
		return r.keyed(capability.ContinuousAggregates, "continuous aggregate", "DROP continuous aggregate "+n.Name)

	// An upsert node carries the match columns, update assignments and
	// predicates of a MERGE as SQL fragments. YDB's own upsert, UPSERT INTO,
	// matches on the primary key and writes the values it inserts, so it
	// holds none of them; the query builder writes that statement.
	case *ast.UpsertNode:
		return refuseFact("upsert into "+n.Table, "YDB's UPSERT INTO matches on the primary key and writes "+
			"the inserted values, so it holds no match columns, update assignments or predicates; "+
			"build the statement with core/query's UpsertInto")

	// Literal SQL is the author's own YQL and is written as it stands. A
	// routine body is another engine's code.
	case *ast.RawSQLNode:
		return r.renderRawSQL(n)
	case *ast.MySQLRoutineNode:
		return r.keyed(capability.Functions, "routine", "a MySQL routine")
	case *ast.OpaqueRoutineNode:
		return r.keyed(capability.Functions, "routine", "a routine")
	case *ast.PostgresDoBlockNode:
		return refuseFact("a PostgreSQL DO block", "YQL has no anonymous code block in DDL")
	case *ast.PostgresRoutineNode:
		return r.keyed(capability.Functions, "routine", "a PostgreSQL routine")
	case *ast.SQLServerRoutineNode:
		return r.keyed(capability.Functions, "routine", "a SQL Server routine")

	case *ast.StatementList:
		for _, statement := range n.Statements {
			if err := r.VisitNode(statement); err != nil {
				return err
			}
		}
		return nil

	// An operation and a type definition are parts of a statement. Each is
	// read out of the ALTER or the CREATE TYPE that carries it, so one
	// arriving alone names no table and no type.
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
		*ast.RenameIndexOperation,
		*ast.AlterIndexVisibilityOperation,
		*ast.SetIndexPartitioningOperation,
		*ast.AddChangefeedOperation,
		*ast.DropChangefeedOperation,
		*ast.AlterChangefeedTopicOperation,
		*ast.ReplaceIndexOperation,
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
		return fmt.Errorf("%w: %s: %T renders as part of the statement that carries it, not on its own",
			ptaherr.ErrInvalidSchemaDiff, DialectName, node)

	default:
		return fmt.Errorf("%w: %s: %T has no handler in this renderer",
			ptaherr.ErrUnsupportedFeature, DialectName, node)
	}
}

// typeKey is the capability a CREATE TYPE asks for, by the kind it declares.
func typeKey(node *ast.CreateTypeNode) capability.Capability {
	switch node.TypeDef.(type) {
	case *ast.DomainTypeDef:
		return capability.DomainTypes
	case *ast.CompositeTypeDef:
		return capability.CompositeTypes
	case *ast.RangeTypeDef:
		return capability.RangeTypes
	default:
		return capability.EnumCustomType
	}
}

// typeSubject names a type in a refusal: "type status", or "a type" where the
// statement names none, as the CREATE TYPE that checks a type definition
// arriving on its own does.
func typeSubject(name string) string {
	if name == "" {
		return "a type"
	}
	return "type " + name
}

// renderComment writes a planner's annotation as a YQL line comment. It is a
// note for the person reading the script and carries no declaration.
func (r *Renderer) renderComment(node *ast.CommentNode) error {
	if node.Text == "" {
		r.w.WriteLine("--")
		return nil
	}
	r.w.WriteLinef("-- %s", node.Text)
	return nil
}

// renderRawSQL writes the author's statement as it stands, terminated.
func (r *Renderer) renderRawSQL(node *ast.RawSQLNode) error {
	r.w.WriteLine(terminated(node.SQL))
	return nil
}

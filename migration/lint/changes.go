package lint

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/parser"
)

// SchemaChangeKind classifies the DDL effect of one semantic schema change.
type SchemaChangeKind string

const (
	// SchemaChangeAdd creates a schema object or adds an element to an existing
	// table (CREATE TABLE/INDEX/TYPE/…, ALTER TABLE … ADD COLUMN/CONSTRAINT).
	SchemaChangeAdd SchemaChangeKind = "add"
	// SchemaChangeModify alters an existing schema object in place (ALTER TABLE
	// … ALTER COLUMN, ALTER TYPE/SEQUENCE/ROLE, ENABLE/DISABLE ROW LEVEL
	// SECURITY, REFRESH MATERIALIZED VIEW, COMMENT ON …).
	SchemaChangeModify SchemaChangeKind = "modify"
	// SchemaChangeDrop removes a schema object or an element of a table (DROP
	// TABLE/INDEX/TYPE/…, ALTER TABLE … DROP COLUMN/CONSTRAINT).
	SchemaChangeDrop SchemaChangeKind = "drop"
	// SchemaChangeRename renames a table or a column (ALTER TABLE … RENAME …).
	SchemaChangeRename SchemaChangeKind = "rename"
)

// SchemaChange is one semantic schema change an up migration expresses. Changes
// are recovered from Ptah's dialect-aware SQL parser — the same parser the
// planner and dev-database replay rely on — rather than by counting statements
// or files. Parsing one statement can yield zero changes (a comment-only or
// operational statement such as INSERT/SELECT, or a statement outside Ptah's
// DDL grammar), exactly one (a single CREATE or DROP), or several (a
// multi-action ALTER TABLE, or a DROP TABLE naming several tables). A schema
// change count is therefore not interchangeable with a statement or file count.
type SchemaChange struct {
	// Version is the migration version that produced the change; zero when the
	// file name carries no recognizable version.
	Version int64 `json:"version"`
	// File is the migration file path exactly as findings report it (reporting
	// prefix included).
	File string `json:"file"`
	// StatementIndex is the zero-based index of the producing statement in the
	// owning file's Statements slice.
	StatementIndex int `json:"statement_index"`
	// Line is the 1-based line number of the producing statement's first token.
	Line int `json:"line,omitempty"`
	// Kind classifies the DDL effect.
	Kind SchemaChangeKind `json:"kind"`
	// Object is the primary schema object the change affects (the table, index,
	// type, column, … name). Best-effort: empty when the parser does not expose
	// a name for the construct.
	Object string `json:"object,omitempty"`
}

// extractSchemaChanges recovers the semantic schema changes an up migration
// expresses by parsing each of its statements with Ptah's dialect-aware SQL
// parser and classifying the resulting AST. Statements are parsed individually
// so a single construct the parser does not model (for example GRANT) cannot
// suppress the changes of its neighbors. Down migrations and files without
// parsed statements yield no changes.
func extractSchemaChanges(file *File, dialect string) []SchemaChange {
	if !file.IsUp || len(file.Statements) == 0 {
		return nil
	}
	var changes []SchemaChange
	for i := range file.Statements {
		changes = append(changes, statementSchemaChanges(file, file.Statements[i], dialect)...)
	}
	return changes
}

func statementSchemaChanges(file *File, stmt Statement, dialect string) []SchemaChange {
	var opts []parser.Option
	if strings.TrimSpace(dialect) != "" {
		opts = []parser.Option{parser.WithDialect(dialect)}
	}
	list, err := parser.NewParser(stmt.SQL, opts...).Parse()
	if err != nil || list == nil {
		// A statement Ptah's parser cannot model contributes no structural
		// change here. The dev-database replay independently validates that the
		// statement executes, so nothing is silently accepted; it is only
		// excluded from the semantic change count.
		return nil
	}
	var changes []SchemaChange
	for _, node := range list.Statements {
		changes = append(changes, nodeSchemaChanges(file, stmt, node)...)
	}
	return changes
}

func nodeSchemaChanges(file *File, stmt Statement, node ast.Node) []SchemaChange {
	change := func(kind SchemaChangeKind, object string) SchemaChange {
		return SchemaChange{
			Version:        file.Version,
			File:           file.Path,
			StatementIndex: stmt.Index,
			Line:           stmt.Line,
			Kind:           kind,
			Object:         object,
		}
	}
	// DROP TABLE and ALTER TABLE are the two constructs where one statement can
	// carry several changes; every other modeled node maps to exactly one.
	switch n := node.(type) {
	case *ast.DropTableNode:
		names := n.TableNames()
		out := make([]SchemaChange, 0, len(names))
		for _, name := range names {
			out = append(out, change(SchemaChangeDrop, name))
		}
		return out
	case *ast.AlterTableNode:
		out := make([]SchemaChange, 0, len(n.Operations))
		for _, op := range n.Operations {
			kind, object := alterOperationChange(n, op)
			out = append(out, change(kind, object))
		}
		return out
	}
	if object, ok := addNodeObject(node); ok {
		return []SchemaChange{change(SchemaChangeAdd, object)}
	}
	if object, ok := dropNodeObject(node); ok {
		return []SchemaChange{change(SchemaChangeDrop, object)}
	}
	if object, ok := modifyNodeObject(node); ok {
		return []SchemaChange{change(SchemaChangeModify, object)}
	}
	// Operational nodes (INSERT/SELECT wrappers, DO blocks, raw SQL) and any
	// construct Ptah does not model as a schema object contribute nothing.
	return nil
}

// addNodeObject reports the object a CREATE (or GRANT) node adds.
func addNodeObject(node ast.Node) (object string, ok bool) {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return n.Name, true
	case *ast.EnumNode:
		return n.Name, true
	case *ast.CreateTypeNode:
		return n.Name, true
	case *ast.IndexNode:
		return n.Name, true
	case *ast.ExtensionNode:
		return n.Name, true
	case *ast.CreateSequenceNode:
		return n.Name, true
	case *ast.CreateSchemaNode:
		return n.Name, true
	case *ast.CreateDatabaseNode:
		return n.Name, true
	case *ast.CreateViewNode:
		return n.Name, true
	case *ast.CreateMaterializedViewNode:
		return n.Name, true
	case *ast.CreateTriggerNode:
		return n.Name, true
	case *ast.CreateFunctionNode:
		return n.Name, true
	case *ast.CreatePolicyNode:
		return n.Name, true
	case *ast.CreateRoleNode:
		return n.Name, true
	case *ast.GrantPrivilegeNode:
		return n.ObjectName, true
	case *ast.OpaqueRoutineNode, *ast.MySQLRoutineNode,
		*ast.PostgresRoutineNode, *ast.SQLServerRoutineNode:
		// Dialect routine bodies (CREATE FUNCTION/PROCEDURE the parser preserves
		// verbatim) are opaque but are still object creations.
		return "", true
	default:
		return "", false
	}
}

// dropNodeObject reports the object a DROP (or REVOKE) node removes. DROP TABLE
// is handled by the caller because it can name several tables at once.
func dropNodeObject(node ast.Node) (object string, ok bool) {
	switch n := node.(type) {
	case *ast.DropTypeNode:
		return n.Name, true
	case *ast.DropIndexNode:
		return n.Name, true
	case *ast.DropExtensionNode:
		return n.Name, true
	case *ast.DropSequenceNode:
		return n.Name, true
	case *ast.DropViewNode:
		return n.Name, true
	case *ast.DropMaterializedViewNode:
		return n.Name, true
	case *ast.DropTriggerNode:
		return n.Name, true
	case *ast.DropFunctionNode:
		return n.Name, true
	case *ast.DropPolicyNode:
		return n.Name, true
	case *ast.DropRoleNode:
		return n.Name, true
	case *ast.RevokePrivilegeNode:
		return n.ObjectName, true
	default:
		return "", false
	}
}

// modifyNodeObject reports the object an in-place ALTER (or COMMENT ON) node
// mutates.
func modifyNodeObject(node ast.Node) (object string, ok bool) {
	switch n := node.(type) {
	case *ast.AlterTypeNode:
		return n.Name, true
	case *ast.AlterSequenceNode:
		return n.Name, true
	case *ast.RefreshMaterializedViewNode:
		return n.Name, true
	case *ast.AlterRoleNode:
		return n.Name, true
	case *ast.AlterTableEnableRLSNode:
		return n.Table, true
	case *ast.AlterTableDisableRLSNode:
		return n.Table, true
	case *ast.CommentNode:
		// A COMMENT ON … statement modifies an object's comment. Bare SQL
		// comments never reach here; the scanner drops them before parsing.
		return "", true
	default:
		return "", false
	}
}

func alterOperationChange(alter *ast.AlterTableNode, op ast.AlterOperation) (SchemaChangeKind, string) {
	switch o := op.(type) {
	case *ast.AddColumnOperation:
		return SchemaChangeAdd, columnName(o.Column)
	case *ast.DropColumnOperation:
		return SchemaChangeDrop, o.ColumnName
	case *ast.ModifyColumnOperation:
		return SchemaChangeModify, columnName(o.Column)
	case *ast.AlterGeneratedColumnExpressionOperation:
		return SchemaChangeModify, o.ColumnName
	case *ast.AddConstraintOperation:
		return SchemaChangeAdd, constraintName(o.Constraint)
	case *ast.DropConstraintOperation:
		return SchemaChangeDrop, o.ConstraintName
	case *ast.RenameColumnOperation:
		return SchemaChangeRename, o.OldName
	case *ast.RenameTableOperation:
		return SchemaChangeRename, alter.Name
	case *ast.AddSkippingIndexOperation:
		return SchemaChangeAdd, o.Name
	case *ast.ModifyTTLOperation:
		return SchemaChangeModify, alter.Name
	default:
		// Any other ALTER TABLE action still mutates the table.
		return SchemaChangeModify, alter.Name
	}
}

func columnName(column *ast.ColumnNode) string {
	if column == nil {
		return ""
	}
	return column.Name
}

func constraintName(constraint *ast.ConstraintNode) string {
	if constraint == nil {
		return ""
	}
	return constraint.Name
}

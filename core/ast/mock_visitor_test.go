package ast_test

// MockVisitor is test support shared by the package's black-box suites: an
// ast.Visitor that records the nodes it visits and can be switched to fail
// every visit for error-path testing.

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
)

// MockVisitor implements the Visitor interface for testing.
//
// It descends into a statement list itself. Accept hands a visitor the node it
// was called on and nothing else, so walking is the visitor's decision -- and a
// recorder that did not walk would see one list where the suite expects the
// statements inside it.
type MockVisitor struct {
	VisitedNodes []string
	ReturnError  bool
}

// VisitNode records the node and answers.
func (m *MockVisitor) VisitNode(node ast.Node) error {
	if list, ok := node.(*ast.StatementList); ok {
		return m.visitList(list)
	}
	m.VisitedNodes = append(m.VisitedNodes, mockNodeLabel(node))
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

// visitList walks the statements in order and stops at the first refusal.
func (m *MockVisitor) visitList(list *ast.StatementList) error {
	for _, statement := range list.Statements {
		if err := statement.Accept(m); err != nil {
			return fmt.Errorf("error visiting statement: %w", err)
		}
	}
	return nil
}

// mockNodeLabel is `Kind:Name`, the form the suite asserts on.
//
// The kind comes from the concrete type with the Node suffix removed, so a node
// kind added to the AST records itself without an edit here. The name is read
// per kind, because a name is a different field on each, and a kind whose name
// nothing asserts on records an empty one.
func mockNodeLabel(node ast.Node) string {
	return mockNodeKind(node) + ":" + mockNodeName(node)
}

func mockNodeKind(node ast.Node) string {
	name := fmt.Sprintf("%T", node)
	if index := strings.LastIndex(name, "."); index >= 0 {
		name = name[index+1:]
	}
	return strings.TrimSuffix(name, "Node")
}

func mockNodeName(node ast.Node) string {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return n.Name
	case *ast.AlterTableNode:
		return n.Name
	case *ast.ColumnNode:
		return n.Name
	case *ast.ConstraintNode:
		return n.Name
	case *ast.IndexNode:
		return n.Name
	case *ast.DropIndexNode:
		return n.Name
	case *ast.EnumNode:
		return n.Name
	case *ast.CreateTypeNode:
		return n.Name
	case *ast.AlterTypeNode:
		return n.Name
	case *ast.CreateSchemaNode:
		return n.Name
	case *ast.CreateDatabaseNode:
		return n.Name
	case *ast.CreateFunctionNode:
		return n.Name
	case *ast.CreatePolicyNode:
		return n.Name
	case *ast.CreateSynonymNode:
		return n.Name
	case *ast.DropSynonymNode:
		return n.Name
	case *ast.DropTableNode:
		return n.Name
	case *ast.DropTypeNode:
		return n.Name
	case *ast.CreateRoleNode:
		return n.Name
	case *ast.AlterRoleNode:
		return n.Name
	case *ast.DropRoleNode:
		return n.Name
	case *ast.CommentNode:
		return n.Text
	case *ast.UpsertNode:
		return n.Table
	case *ast.AlterTableEnableRLSNode:
		return n.Table
	// A grant names a role rather than an object: the role is what tells two
	// grants on one table apart.
	case *ast.GrantPrivilegeNode:
		return n.Role
	case *ast.RevokePrivilegeNode:
		return n.Role
	// An ALTER TABLE operation names what it acts on, which is a column or a
	// constraint that has no node of its own.
	case *ast.DropColumnOperation:
		return n.ColumnName
	case *ast.DropConstraintOperation:
		return n.ConstraintName
	// A routine carries its statement instead of a name, and the statement is
	// what a renderer emits.
	case *ast.RawSQLNode:
		return n.SQL
	case *ast.MySQLRoutineNode:
		return n.SQL
	case *ast.OpaqueRoutineNode:
		return n.SQL
	case *ast.PostgresDoBlockNode:
		return n.SQL
	case *ast.PostgresRoutineNode:
		return n.SQL
	case *ast.SQLServerRoutineNode:
		return n.SQL
	default:
		return ""
	}
}

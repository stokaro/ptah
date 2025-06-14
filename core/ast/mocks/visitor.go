package mocks

import (
	"errors"

	"github.com/stokaro/ptah/core/ast"
)

// MockVisitor implements the Visitor interface for testing
type MockVisitor struct {
	VisitedNodes []string
	ReturnError  bool
}

func (m *MockVisitor) VisitCreateTable(node *ast.CreateTableNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateTable:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitAlterTable(node *ast.AlterTableNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "AlterTable:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitColumn(node *ast.ColumnNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Column:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitConstraint(node *ast.ConstraintNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Constraint:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitIndex(node *ast.IndexNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Index:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropIndex(node *ast.DropIndexNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropIndex:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitEnum(node *ast.EnumNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Enum:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitComment(node *ast.CommentNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Comment:"+node.Text)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropTable(node *ast.DropTableNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropTable:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateType(node *ast.CreateTypeNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateType:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitAlterType(node *ast.AlterTypeNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "AlterType:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropType(node *ast.DropTypeNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropType:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

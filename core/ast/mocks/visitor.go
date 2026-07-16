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

func (m *MockVisitor) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateSchema:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateDatabase:"+node.Name)
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

func (m *MockVisitor) VisitExtension(node *ast.ExtensionNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "Extension:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropExtension(node *ast.DropExtensionNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropExtension:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateFunction:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreatePolicy:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "AlterTableEnableRLS:"+node.Table)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropFunction(node *ast.DropFunctionNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropFunction:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateView(node *ast.CreateViewNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateView:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropView(node *ast.DropViewNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropView:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateMaterializedView:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropMaterializedView:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "RefreshMaterializedView:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateTrigger:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropTrigger(node *ast.DropTriggerNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropTrigger:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropPolicy(node *ast.DropPolicyNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropPolicy:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "AlterTableDisableRLS:"+node.Table)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitCreateRole(node *ast.CreateRoleNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "CreateRole:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitDropRole(node *ast.DropRoleNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "DropRole:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitAlterRole(node *ast.AlterRoleNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "AlterRole:"+node.Name)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "GrantPrivilege:"+node.Role)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "RevokePrivilege:"+node.Role)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

func (m *MockVisitor) VisitRawSQL(node *ast.RawSQLNode) error {
	m.VisitedNodes = append(m.VisitedNodes, "RawSQL:"+node.SQL)
	if m.ReturnError {
		return errors.New("mock error")
	}
	return nil
}

package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/ast/mocks"
)

func TestCreateRoleNode(t *testing.T) {
	t.Run("NewCreateRole creates role with defaults", func(t *testing.T) {
		c := qt.New(t)
		role := ast.NewCreateRole("test_role")

		c.Assert(role.Name, qt.Equals, "test_role")
		c.Assert(role.Login, qt.Equals, false)
		c.Assert(role.Password, qt.Equals, "")
		c.Assert(role.Superuser, qt.Equals, false)
		c.Assert(role.CreateDB, qt.Equals, false)
		c.Assert(role.CreateRole, qt.Equals, false)
		c.Assert(role.Inherit, qt.Equals, true) // Default to true
		c.Assert(role.Replication, qt.Equals, false)
		c.Assert(role.Comment, qt.Equals, "")
	})

	t.Run("fluent API methods work correctly", func(t *testing.T) {
		c := qt.New(t)
		role := ast.NewCreateRole("app_user").
			SetLogin(true).
			SetPassword("encrypted_password").
			SetSuperuser(true).
			SetCreateDB(true).
			SetCreateRole(true).
			SetInherit(false).
			SetReplication(true).
			SetComment("Application user role")

		c.Assert(role.Name, qt.Equals, "app_user")
		c.Assert(role.Login, qt.Equals, true)
		c.Assert(role.Password, qt.Equals, "encrypted_password")
		c.Assert(role.Superuser, qt.Equals, true)
		c.Assert(role.CreateDB, qt.Equals, true)
		c.Assert(role.CreateRole, qt.Equals, true)
		c.Assert(role.Inherit, qt.Equals, false)
		c.Assert(role.Replication, qt.Equals, true)
		c.Assert(role.Comment, qt.Equals, "Application user role")
	})

	t.Run("Accept calls visitor correctly", func(t *testing.T) {
		c := qt.New(t)
		role := ast.NewCreateRole("test_role")
		visitor := &mocks.MockVisitor{}

		err := role.Accept(visitor)

		c.Assert(err, qt.IsNil)
		c.Assert(visitor.VisitedNodes, qt.Contains, "CreateRole:test_role")
	})

	t.Run("Accept propagates visitor errors", func(t *testing.T) {
		c := qt.New(t)
		role := ast.NewCreateRole("test_role")
		visitor := &mocks.MockVisitor{ReturnError: true}

		err := role.Accept(visitor)

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "mock error")
	})
}

func TestDropRoleNode(t *testing.T) {
	t.Run("NewDropRole creates role with defaults", func(t *testing.T) {
		c := qt.New(t)
		dropRole := ast.NewDropRole("test_role")

		c.Assert(dropRole.Name, qt.Equals, "test_role")
		c.Assert(dropRole.IfExists, qt.Equals, false)
		c.Assert(dropRole.Comment, qt.Equals, "")
	})

	t.Run("fluent API methods work correctly", func(t *testing.T) {
		c := qt.New(t)
		dropRole := ast.NewDropRole("old_role").
			SetIfExists().
			SetComment("Remove unused role")

		c.Assert(dropRole.Name, qt.Equals, "old_role")
		c.Assert(dropRole.IfExists, qt.Equals, true)
		c.Assert(dropRole.Comment, qt.Equals, "Remove unused role")
	})

	t.Run("Accept calls visitor correctly", func(t *testing.T) {
		c := qt.New(t)
		dropRole := ast.NewDropRole("test_role")
		visitor := &mocks.MockVisitor{}

		err := dropRole.Accept(visitor)

		c.Assert(err, qt.IsNil)
		c.Assert(visitor.VisitedNodes, qt.Contains, "DropRole:test_role")
	})

	t.Run("Accept propagates visitor errors", func(t *testing.T) {
		c := qt.New(t)
		dropRole := ast.NewDropRole("test_role")
		visitor := &mocks.MockVisitor{ReturnError: true}

		err := dropRole.Accept(visitor)

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "mock error")
	})
}

func TestAlterRoleNode(t *testing.T) {
	t.Run("NewAlterRole creates role with empty operations", func(t *testing.T) {
		c := qt.New(t)
		alterRole := ast.NewAlterRole("test_role")

		c.Assert(alterRole.Name, qt.Equals, "test_role")
		c.Assert(len(alterRole.Operations), qt.Equals, 0)
		c.Assert(alterRole.Comment, qt.Equals, "")
	})

	t.Run("AddOperation adds operations correctly", func(t *testing.T) {
		c := qt.New(t)
		alterRole := ast.NewAlterRole("test_role").
			AddOperation(ast.NewSetLoginOperation(true)).
			AddOperation(ast.NewSetPasswordOperation("new_password")).
			SetComment("Update role attributes")

		c.Assert(alterRole.Name, qt.Equals, "test_role")
		c.Assert(len(alterRole.Operations), qt.Equals, 2)
		c.Assert(alterRole.Comment, qt.Equals, "Update role attributes")

		// Check operation types
		c.Assert(alterRole.Operations[0].GetOperationType(), qt.Equals, "SET_LOGIN")
		c.Assert(alterRole.Operations[1].GetOperationType(), qt.Equals, "SET_PASSWORD")
	})

	t.Run("Accept calls visitor correctly", func(t *testing.T) {
		c := qt.New(t)
		alterRole := ast.NewAlterRole("test_role")
		visitor := &mocks.MockVisitor{}

		err := alterRole.Accept(visitor)

		c.Assert(err, qt.IsNil)
		c.Assert(visitor.VisitedNodes, qt.Contains, "AlterRole:test_role")
	})

	t.Run("Accept propagates visitor errors", func(t *testing.T) {
		c := qt.New(t)
		alterRole := ast.NewAlterRole("test_role")
		visitor := &mocks.MockVisitor{ReturnError: true}

		err := alterRole.Accept(visitor)

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "mock error")
	})
}

func TestRoleOperations(t *testing.T) {
	t.Run("SetPasswordOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetPasswordOperation("encrypted_password")

		c.Assert(op.GetOperationType(), qt.Equals, "SET_PASSWORD")
		c.Assert(op.Password, qt.Equals, "encrypted_password")
	})

	t.Run("SetLoginOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetLoginOperation(true)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_LOGIN")
		c.Assert(op.Login, qt.Equals, true)
	})

	t.Run("SetSuperuserOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetSuperuserOperation(true)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_SUPERUSER")
		c.Assert(op.Superuser, qt.Equals, true)
	})

	t.Run("SetCreateDBOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetCreateDBOperation(true)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_CREATEDB")
		c.Assert(op.CreateDB, qt.Equals, true)
	})

	t.Run("SetCreateRoleOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetCreateRoleOperation(true)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_CREATEROLE")
		c.Assert(op.CreateRole, qt.Equals, true)
	})

	t.Run("SetInheritOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetInheritOperation(false)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_INHERIT")
		c.Assert(op.Inherit, qt.Equals, false)
	})

	t.Run("SetReplicationOperation", func(t *testing.T) {
		c := qt.New(t)
		op := ast.NewSetReplicationOperation(true)

		c.Assert(op.GetOperationType(), qt.Equals, "SET_REPLICATION")
		c.Assert(op.Replication, qt.Equals, true)
	})
}

package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

func TestPostgreSQLRenderer_VisitCreateRole(t *testing.T) {
	t.Run("basic role creation", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		role := ast.NewCreateRole("test_role")
		sql, err := renderer.Render(role)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "CREATE ROLE test_role WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION;\n")
	})

	t.Run("role with login and password", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		role := ast.NewCreateRole("app_user").
			SetLogin(true).
			SetPassword("encrypted_password")
		sql, err := renderer.Render(role)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "CREATE ROLE app_user WITH LOGIN PASSWORD 'encrypted_password'")
		c.Assert(sql, qt.Contains, "NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION")
	})

	t.Run("superuser role", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		role := ast.NewCreateRole("admin_user").
			SetLogin(true).
			SetSuperuser(true)
		sql, err := renderer.Render(role)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "CREATE ROLE admin_user WITH LOGIN SUPERUSER")
		c.Assert(sql, qt.Contains, "NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION")
	})

	t.Run("role with all privileges", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		role := ast.NewCreateRole("power_user").
			SetLogin(true).
			SetSuperuser(true).
			SetCreateDB(true).
			SetCreateRole(true).
			SetReplication(true).
			SetInherit(false)
		sql, err := renderer.Render(role)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "CREATE ROLE power_user WITH LOGIN SUPERUSER CREATEDB CREATEROLE NOINHERIT REPLICATION")
	})

	t.Run("role with comment", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		role := ast.NewCreateRole("documented_role").
			SetComment("This is a test role")
		sql, err := renderer.Render(role)

		c.Assert(err, qt.IsNil)
		lines := strings.Split(strings.TrimSpace(sql), "\n")
		c.Assert(lines[0], qt.Equals, "-- This is a test role")
		c.Assert(lines[1], qt.Contains, "CREATE ROLE documented_role")
	})
}

func TestPostgreSQLRenderer_VisitDropRole(t *testing.T) {
	t.Run("basic role drop", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		dropRole := ast.NewDropRole("test_role")
		sql, err := renderer.Render(dropRole)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "DROP ROLE test_role;\n")
	})

	t.Run("drop role with IF EXISTS", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		dropRole := ast.NewDropRole("test_role").SetIfExists()
		sql, err := renderer.Render(dropRole)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "DROP ROLE IF EXISTS test_role;\n")
	})

	t.Run("drop role with comment", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		dropRole := ast.NewDropRole("test_role").
			SetComment("Remove unused role")
		sql, err := renderer.Render(dropRole)

		c.Assert(err, qt.IsNil)
		lines := strings.Split(strings.TrimSpace(sql), "\n")
		c.Assert(lines[0], qt.Equals, "-- Remove unused role")
		c.Assert(lines[1], qt.Equals, "DROP ROLE test_role;")
	})
}

func TestPostgreSQLRenderer_VisitAlterRole(t *testing.T) {
	t.Run("alter role with single operation", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		alterRole := ast.NewAlterRole("test_role").
			AddOperation(ast.NewSetLoginOperation(true))
		sql, err := renderer.Render(alterRole)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "ALTER ROLE test_role LOGIN;\n")
	})

	t.Run("alter role with multiple operations", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		alterRole := ast.NewAlterRole("test_role").
			AddOperation(ast.NewSetLoginOperation(true)).
			AddOperation(ast.NewSetPasswordOperation("md5a1b2c3d4e5f6789012345678901234")).
			AddOperation(ast.NewSetCreateDBOperation(true))
		sql, err := renderer.Render(alterRole)

		c.Assert(err, qt.IsNil)
		lines := strings.Split(strings.TrimSpace(sql), "\n")
		c.Assert(len(lines), qt.Equals, 3)
		c.Assert(lines[0], qt.Equals, "ALTER ROLE test_role LOGIN;")
		c.Assert(lines[1], qt.Equals, "ALTER ROLE test_role PASSWORD 'md5a1b2c3d4e5f6789012345678901234';")
		c.Assert(lines[2], qt.Equals, "ALTER ROLE test_role CREATEDB;")
	})

	t.Run("alter role with all operation types", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		alterRole := ast.NewAlterRole("test_role").
			AddOperation(ast.NewSetLoginOperation(false)).
			AddOperation(ast.NewSetPasswordOperation("SCRAM-SHA-256$4096:abcd1234$hash:signature")).
			AddOperation(ast.NewSetSuperuserOperation(true)).
			AddOperation(ast.NewSetCreateDBOperation(false)).
			AddOperation(ast.NewSetCreateRoleOperation(true)).
			AddOperation(ast.NewSetInheritOperation(false)).
			AddOperation(ast.NewSetReplicationOperation(true))
		sql, err := renderer.Render(alterRole)

		c.Assert(err, qt.IsNil)
		lines := strings.Split(strings.TrimSpace(sql), "\n")
		c.Assert(len(lines), qt.Equals, 7)
		c.Assert(lines[0], qt.Equals, "ALTER ROLE test_role NOLOGIN;")
		c.Assert(lines[1], qt.Equals, "ALTER ROLE test_role PASSWORD 'SCRAM-SHA-256$4096:abcd1234$hash:signature';")
		c.Assert(lines[2], qt.Equals, "ALTER ROLE test_role SUPERUSER;")
		c.Assert(lines[3], qt.Equals, "ALTER ROLE test_role NOCREATEDB;")
		c.Assert(lines[4], qt.Equals, "ALTER ROLE test_role CREATEROLE;")
		c.Assert(lines[5], qt.Equals, "ALTER ROLE test_role NOINHERIT;")
		c.Assert(lines[6], qt.Equals, "ALTER ROLE test_role REPLICATION;")
	})

	t.Run("alter role with comment", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		alterRole := ast.NewAlterRole("test_role").
			AddOperation(ast.NewSetLoginOperation(true)).
			SetComment("Enable login for test role")
		sql, err := renderer.Render(alterRole)

		c.Assert(err, qt.IsNil)
		lines := strings.Split(strings.TrimSpace(sql), "\n")
		c.Assert(lines[0], qt.Equals, "-- Enable login for test role")
		c.Assert(lines[1], qt.Equals, "ALTER ROLE test_role LOGIN;")
	})

	t.Run("alter role with no operations", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		alterRole := ast.NewAlterRole("test_role")
		sql, err := renderer.Render(alterRole)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "")
	})
}

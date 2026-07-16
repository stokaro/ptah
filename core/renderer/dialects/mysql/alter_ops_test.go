package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/mysql"
)

func renderMySQL(t *testing.T, nodes ...ast.Node) string {
	t.Helper()
	r := mysql.New()
	r.Reset()
	for _, n := range nodes {
		if err := n.Accept(r); err != nil {
			t.Fatalf("accept failed: %v", err)
		}
	}
	return r.Output()
}

// MySQL 8.0+ supports `ALTER TABLE x RENAME COLUMN old TO new`; the renderer
// emits it unconditionally and the runtime DB version is the user's problem
// (matches the existing dialect behaviour for AUTO_INCREMENT etc.).
func TestMySQL_AlterTable_RenameColumn(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.RenameColumnOperation{OldName: "email_old", NewName: "email"},
		},
	}
	out := renderMySQL(t, alter)
	c.Assert(out, qt.Contains, "ALTER TABLE users RENAME COLUMN email_old TO email;")
}

func TestMySQL_AlterTable_RenameTable(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "old_users",
		Operations: []ast.AlterOperation{
			&ast.RenameTableOperation{NewName: "users"},
		},
	}
	out := renderMySQL(t, alter)
	c.Assert(out, qt.Contains, "ALTER TABLE old_users RENAME TO users;")
}

func TestMySQL_CreateNamespaceStatements(t *testing.T) {
	c := qt.New(t)
	out := renderMySQL(t,
		&ast.CreateSchemaNode{Name: "`bc_test`", IfNotExists: true},
		&ast.CreateDatabaseNode{Name: "`atlantis`"},
	)

	c.Assert(out, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `bc_test`;")
	c.Assert(out, qt.Contains, "CREATE DATABASE `atlantis`;")
}

func TestMySQL_CreateTableSelectTail(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("t2").
		SetIfNotExists().
		SetOption("ENGINE", "heap").
		SetSelectBody("SELECT * FROM t1")

	out := renderMySQL(t, table)

	c.Assert(out, qt.Contains, "CREATE TABLE IF NOT EXISTS t2 ENGINE=heap SELECT * FROM t1;")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE IF NOT EXISTS t2 (")
}

func TestMySQL_AlterTable_ClickHouseOnlyOpsEmitComment(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.AddSkippingIndexOperation{Name: "idx_e_src", Expression: "source"},
			&ast.ModifyTTLOperation{Expression: "created_at + INTERVAL 30 DAY"},
		},
	}
	out := renderMySQL(t, alter)

	c.Assert(out, qt.Contains, "-- MYSQL: data-skipping indexes are ClickHouse-specific; ignored.")
	c.Assert(out, qt.Contains, "-- MYSQL: table TTL is ClickHouse-specific; ignored.")
	c.Assert(out, qt.Not(qt.Contains), "ADD INDEX")
	c.Assert(out, qt.Not(qt.Contains), "MODIFY TTL")
}

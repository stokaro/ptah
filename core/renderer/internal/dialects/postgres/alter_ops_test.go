package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/postgres"
)

// renderPG runs the postgres renderer against a list of nodes and returns
// the accumulated output, failing the test on any error. Used by the alter
// ops tests below.
func renderPG(t *testing.T, nodes ...ast.Node) string {
	t.Helper()
	r := postgres.New()
	r.Reset()
	for _, n := range nodes {
		if err := n.Accept(r); err != nil {
			t.Fatalf("accept failed: %v", err)
		}
	}
	return r.Output()
}

func TestPostgres_AlterTable_RenameColumn(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.RenameColumnOperation{OldName: "email_old", NewName: "email"},
		},
	}
	out := legacyPostgresSQL(renderPG(t, alter))
	c.Assert(out, qt.Contains, "ALTER TABLE users RENAME COLUMN email_old TO email;")
}

func TestPostgres_AlterTable_RenameTable(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "old_users",
		Operations: []ast.AlterOperation{
			&ast.RenameTableOperation{NewName: "users"},
		},
	}
	out := legacyPostgresSQL(renderPG(t, alter))
	c.Assert(out, qt.Contains, "ALTER TABLE old_users RENAME TO users;")
}

func TestPostgres_CreateSchema(t *testing.T) {
	c := qt.New(t)
	out := legacyPostgresSQL(renderPG(t, &ast.CreateSchemaNode{Name: "auth", IfNotExists: true, Comment: "Auth user's schema"}))
	c.Assert(out, qt.Contains, "CREATE SCHEMA IF NOT EXISTS auth;")
	c.Assert(out, qt.Contains, "COMMENT ON SCHEMA auth IS 'Auth user''s schema';")
}

func TestPostgres_CreateDatabase(t *testing.T) {
	c := qt.New(t)
	out := legacyPostgresSQL(renderPG(t, &ast.CreateDatabaseNode{Name: "appdb"}))
	c.Assert(out, qt.Contains, "CREATE DATABASE appdb;")
}

func TestPostgres_CreateDatabaseIfNotExistsUnsupported(t *testing.T) {
	c := qt.New(t)
	r := postgres.New()

	err := (&ast.CreateDatabaseNode{Name: "appdb", IfNotExists: true}).Accept(r)

	c.Assert(err, qt.ErrorMatches, "create database if not exists is not supported in PostgreSQL")
}

func TestPostgres_CreateTableSelectWithTypedColumnsUnsupported(t *testing.T) {
	c := qt.New(t)
	r := postgres.New()
	table := ast.NewCreateTable("copied_users").
		AddColumn(ast.NewColumn("id", "INTEGER")).
		SetSelectBody("SELECT id FROM users")

	err := table.Accept(r)

	c.Assert(err, qt.ErrorMatches, "postgres: create table as select with explicit column definitions is not supported")
}

// AddSkippingIndex and ModifyTTL are ClickHouse-only; postgres emits an
// explanatory comment and otherwise treats the operation as a no-op.
func TestPostgres_AlterTable_ClickHouseOnlyOpsEmitComment(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.AddSkippingIndexOperation{Name: "idx_e_src", Expression: "source"},
			&ast.ModifyTTLOperation{Expression: "created_at + INTERVAL '30 days'"},
		},
	}
	out := renderPG(t, alter)

	c.Assert(out, qt.Contains, "-- POSTGRES: data-skipping indexes are ClickHouse-specific; ignored.")
	c.Assert(out, qt.Contains, "-- POSTGRES: table TTL is ClickHouse-specific; ignored.")
	// No executable ALTER statement should have been emitted by these branches.
	c.Assert(out, qt.Not(qt.Contains), "ADD INDEX",
		qt.Commentf("postgres must not emit ADD INDEX for an AddSkippingIndexOperation; got: %q", out))
	c.Assert(out, qt.Not(qt.Contains), "MODIFY TTL",
		qt.Commentf("postgres must not emit MODIFY TTL for a ModifyTTLOperation; got: %q", out))
}

func TestPostgres_AlterTable_SetGeneratedExpression(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AlterGeneratedColumnExpressionOperation{
				ColumnName: "slug",
				Expression: "lower(name)",
			},
		},
	}

	out := renderPG(t, alter)

	c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "slug" SET EXPRESSION AS (lower(name));`)
}

func TestPostgres_AlterTable_SetGeneratedExpressionUnsupported(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.Postgres16(), platform.Postgres)
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AlterGeneratedColumnExpressionOperation{
				ColumnName: "slug",
				Expression: "lower(name)",
			},
		},
	}

	err := alter.Accept(renderer)

	c.Assert(err, qt.IsNil)
	c.Assert(renderer.Output(), qt.Contains, `ALTER COLUMN SET EXPRESSION requires PostgreSQL 17+`)
	c.Assert(renderer.Output(), qt.Not(qt.Contains), `SET EXPRESSION AS`)
}

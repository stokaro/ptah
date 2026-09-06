package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/postgres"
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
	// The refusal has to name the key a preset is composed from, because that
	// is what an operator can act on: Postgres16() is one of several ways to
	// reach this branch, and the others carry no version that explains it.
	c.Assert(renderer.Output(), qt.Contains, "ALTER COLUMN SET EXPRESSION requires target capability "+string(capability.AlterGeneratedColumnExpression))
	c.Assert(renderer.Output(), qt.Not(qt.Contains), `SET EXPRESSION AS`)
}

// TestVisitCreateSchema_SpannerKeepsTheSchemaAndSaysWhatItLeftOut covers
// stokaro/ptah#2651.
//
// The renderer emitted `COMMENT ON SCHEMA` for the whole PostgreSQL family with
// no capability gate. Measured on the Cloud Spanner emulator behind PGAdapter
// 0.55.2: the `CREATE SCHEMA` before it is accepted and the comment answers
// `Unknown statement`. `ptah schema render` had always produced it, so an
// operator applying that output by hand met the refusal; #2626 put it on the
// plan, which is what made it a failing `ptah migrations up`.
//
// The schema is still created, because that is what the author asked for, and
// the line naming what was left out is what keeps this from being a comment
// quietly deleted from somebody's DDL.
func TestVisitCreateSchema_SpannerKeepsTheSchemaAndSaysWhatItLeftOut(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
	r.Reset()

	node := &ast.CreateSchemaNode{Name: "app", Comment: "the schema"}
	c.Assert(node.Accept(r), qt.IsNil)

	out := r.Output()
	c.Assert(out, qt.Contains, `CREATE SCHEMA "app";`)
	c.Assert(out, qt.Not(qt.Contains), "COMMENT ON SCHEMA")
	c.Assert(out, qt.Contains, "schema comment app is not supported by this target; skipped.")
}

// TestVisitCreateSchema_PostgresStillCommentsTheSchema is the control.
//
// Every assertion above is satisfied by a renderer that stopped emitting the
// statement for everyone, which would silently drop a comment three of the four
// family members accept and read back -- measured on PostgreSQL 17,
// CockroachDB v24.1.33 and YugabyteDB 2024.1.3.0.
func TestVisitCreateSchema_PostgresStillCommentsTheSchema(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
	r.Reset()

	node := &ast.CreateSchemaNode{Name: "app", Comment: "the schema"}
	c.Assert(node.Accept(r), qt.IsNil)

	out := r.Output()
	c.Assert(out, qt.Contains, `CREATE SCHEMA "app";`)
	c.Assert(out, qt.Contains, `COMMENT ON SCHEMA "app" IS 'the schema';`)
	c.Assert(out, qt.Not(qt.Contains), "skipped")
}

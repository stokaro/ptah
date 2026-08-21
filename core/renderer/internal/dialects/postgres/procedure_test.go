package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

// TestPostgreSQLRenderer_Procedure covers the statements a procedure takes and
// the one clause it must never carry.
//
// A procedure is the same catalog object as a function with its return type
// removed, which is why one node carries both. The grammar is where they part:
// `CREATE PROCEDURE p() RETURNS int` does not parse, and `DROP FUNCTION` aimed
// at a procedure is refused by name (stokaro/ptah#1722).
func TestPostgreSQLRenderer_Procedure(t *testing.T) {
	tests := []struct {
		name        string
		node        ast.Node
		want        string
		wantMissing string
	}{
		{
			name: "a procedure carries no RETURNS, even when the node holds one",
			node: ast.NewCreateFunction("bump").
				SetKind("procedure").
				SetParameters("n integer").
				SetReturns("integer").
				SetLanguage("sql").
				SetBody("SELECT n"),
			want:        `CREATE OR REPLACE PROCEDURE "bump"(n integer)`,
			wantMissing: "RETURNS",
		},
		{
			name: "a function still carries its RETURNS",
			node: ast.NewCreateFunction("addone").
				SetParameters("n integer").
				SetReturns("integer").
				SetLanguage("sql").
				SetBody("SELECT n + 1"),
			want:        `CREATE OR REPLACE FUNCTION "addone"(n integer) RETURNS integer`,
			wantMissing: "PROCEDURE",
		},
		{
			name:        "a procedure is dropped with its own verb",
			node:        ast.NewDropFunction("bump").SetKind("procedure").SetIfExists(),
			want:        `DROP PROCEDURE IF EXISTS "bump";`,
			wantMissing: "DROP FUNCTION",
		},
		{
			name:        "a function is still dropped with DROP FUNCTION",
			node:        ast.NewDropFunction("addone").SetIfExists(),
			want:        `DROP FUNCTION IF EXISTS "addone";`,
			wantMissing: "PROCEDURE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.New()

			sql, err := renderer.Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), test.wantMissing)
		})
	}
}

// TestPostgreSQLRenderer_Procedure_DropsWithoutAnEmptySignature pins the drop
// that used to remove nothing.
//
// `DROP FUNCTION f()` names the ZERO-ARGUMENT overload specifically, so a drop
// of a routine that takes arguments matched nothing -- and with IF EXISTS in
// front of it, reported success having removed nothing at all. The bare name is
// ambiguous only when the server holds two of them, which is a louder failure
// than the silent one.
func TestPostgreSQLRenderer_Procedure_DropsWithoutAnEmptySignature(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	sql, err := renderer.Render(ast.NewDropFunction("bump").SetKind("procedure").SetIfExists())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "()")
}

// TestPostgreSQLRenderer_Procedure_NamesTheSkipOnATargetWithout keeps the
// procedure behind its own key rather than the function's.
//
// Spanner's PostgreSQL interface takes neither, and the two keys are separate
// because the answers differ elsewhere: SQL Server hosts functions and no Ptah
// path reads a procedure back there.
func TestPostgreSQLRenderer_Procedure_NamesTheSkipOnATargetWithout(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)

	sql, err := renderer.Render(ast.NewCreateFunction("bump").
		SetKind("procedure").
		SetLanguage("sql").
		SetBody("SELECT 1"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "CREATE OR REPLACE PROCEDURE")
	c.Assert(sql, qt.Contains, "procedure bump is not supported by this target; skipped.")
}

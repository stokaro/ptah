package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// checkedTable declares one column CHECK without a name and one with a name.
func checkedTable() *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
		{Name: "id", Type: "BIGINT", Primary: true},
		{Name: "s", Type: "TEXT", Nullable: true, Check: "s <> ''"},
		{Name: "u", Type: "TEXT", Nullable: true, Check: "u <> ''", CheckName: "u_named"},
	}}
}

// addCheckedColumn adds a column carrying a CHECK without a name.
func addCheckedColumn() *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
		&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "v", Type: "TEXT", Nullable: true, Check: "v <> ''"}},
	}}
}

// TestRenderer_ColumnChecksOnSpanner pins how Spanner gets a column CHECK the
// comparison can find again. An unnamed check is written under the name the
// comparison looks for, because the server would name it
// CK_<table>_<hash>_<n>. A column added later takes its check as a constraint
// of its own, because the server refuses a CHECK inside ADD COLUMN. A check
// that names itself keeps its name.
func TestRenderer_ColumnChecksOnSpanner(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
	r.Reset()

	c.Assert(checkedTable().Accept(r), qt.IsNil)
	c.Assert(addCheckedColumn().Accept(r), qt.IsNil)

	out := r.Output()
	c.Assert(out, qt.Contains, `  "s" TEXT CONSTRAINT "t_s_check" CHECK (s <> ''),`)
	c.Assert(out, qt.Contains, `  "u" TEXT CONSTRAINT "u_named" CHECK (u <> '')`)
	c.Assert(out, qt.Contains, "ALTER TABLE \"t\" ADD COLUMN \"v\" TEXT;\nALTER TABLE \"t\" ADD CONSTRAINT \"t_v_check\" CHECK (v <> '');")
}

// TestRenderer_ColumnChecksOnPostgres is the control: PostgreSQL names an
// unnamed check by the convention the comparison predicts and accepts a CHECK
// inside ADD COLUMN, so both stay as declared.
func TestRenderer_ColumnChecksOnPostgres(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
	r.Reset()

	c.Assert(checkedTable().Accept(r), qt.IsNil)
	c.Assert(addCheckedColumn().Accept(r), qt.IsNil)

	out := r.Output()
	c.Assert(out, qt.Contains, `  "s" TEXT CHECK (s <> ''),`)
	c.Assert(out, qt.Contains, `  "u" TEXT CONSTRAINT "u_named" CHECK (u <> '')`)
	c.Assert(out, qt.Contains, `ALTER TABLE "t" ADD COLUMN "v" TEXT CHECK (v <> '');`)
}

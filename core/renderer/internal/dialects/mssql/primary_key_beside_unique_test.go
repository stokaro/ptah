package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/mssql"
)

// TestAddColumnRefusesPrimaryKeyBesideUnique covers the second entry point the
// refusal guards, and it is not the same statement as the CREATE TABLE one.
//
// `renderColumn` serves both `CREATE TABLE` and `ALTER TABLE ... ADD`, so a
// guard placed there covers a column arriving through either. A test for one
// says nothing about the other: the guard could as easily have been written
// into VisitCreateTable, where the added column would keep reaching the server
// as a statement it refuses.
func TestAddColumnRefusesPrimaryKeyBesideUnique(t *testing.T) {
	c := qt.New(t)

	column := ast.NewColumn("a", "INT")
	column.Primary = true
	column.Unique = true
	alter := &ast.AlterTableNode{
		Name:       "t",
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
	}

	_, err := mssql.New().Render(alter)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "PRIMARY KEY and UNIQUE")
}

// TestAddColumnKeepsUniqueWithoutAPrimaryKey is the control on that entry
// point: a column carrying UNIQUE alone is what SQL Server accepts, and it
// still renders.
func TestAddColumnKeepsUniqueWithoutAPrimaryKey(t *testing.T) {
	c := qt.New(t)

	column := ast.NewColumn("a", "INT")
	column.Unique = true
	alter := &ast.AlterTableNode{
		Name:       "t",
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
	}

	rendered, err := mssql.New().Render(alter)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "UNIQUE")
}

// TestAddColumnKeepsAPrimaryKeyWithoutUnique is the other half of the control,
// and it is here because the tree caught this mutant and this file did not.
//
// A guard written as `if !column.Primary` refuses every primary key column on
// SQL Server. Four unrelated renderer tests go red on that, so the tree is
// covered -- but a reader auditing this rule should not have to find them, and
// a future narrowing of those tests would take the coverage with it.
func TestAddColumnKeepsAPrimaryKeyWithoutUnique(t *testing.T) {
	c := qt.New(t)

	column := ast.NewColumn("a", "INT")
	column.Primary = true
	alter := &ast.AlterTableNode{
		Name:       "t",
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
	}

	rendered, err := mssql.New().Render(alter)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "PRIMARY KEY")
}

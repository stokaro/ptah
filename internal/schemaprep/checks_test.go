package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func TestTableCheckConstraints(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	table := schemamodel.Table{StructName: "Product", Schema: "store", Name: "products", Checks: []string{
		"price > 0",
		"stock >= 0",
		"published_at IS NOT NULL",
	}}
	declared := []schemamodel.Constraint{
		{StructName: "Product", Name: "products_check", Type: "UNIQUE"},
		{Table: "store.products", Name: "stock_nonnegative", Type: "CHECK", CheckExpression: "stock >= 0"},
	}

	got := schemaprep.TableCheckConstraints(table, nil, declared, "postgres")
	c.Assert(got, qt.DeepEquals, []schemamodel.Constraint{
		{StructName: "Product", Name: "products_check1", Type: "CHECK", Table: "store.products", CheckExpression: "price > 0"},
		{StructName: "Product", Name: "products_check2", Type: "CHECK", Table: "store.products", CheckExpression: "published_at IS NOT NULL"},
	})
}

// columnCheckFields declares table e with a column CHECK over two columns, one
// over the other two, and one over its own column, none of them named.
func columnCheckFields() []schemamodel.Field {
	return []schemamodel.Field{
		{StructName: "E", Name: "a", Type: "INTEGER", Check: "a IS NULL OR b IS NOT NULL"},
		{StructName: "E", Name: "b", Type: "INTEGER", Check: "b IS NULL OR a IS NOT NULL"},
		{StructName: "E", Name: "c", Type: "INTEGER", Check: "c > 0"},
		{StructName: "E", Name: "d", Type: "INTEGER", Check: "d > 0", CheckName: "e_d_positive"},
		{StructName: "E", Name: "n", Type: "INTEGER"},
		{StructName: "Other", Name: "a", Type: "INTEGER", Check: "a > 0"},
	}
}

// TestColumnCheckNames names the CHECK each column of a table carries. The
// PostgreSQL names were read back from pg_constraint on PostgreSQL 18.6 after
// `CREATE TABLE e (a INTEGER CHECK (a IS NULL OR b IS NOT NULL), b INTEGER
// CHECK (b IS NULL OR a IS NOT NULL), c INTEGER CHECK (c > 0), ...)`, which is
// the table the renderer writes for these fields (stokaro/ptah#3750). Every
// other target keeps `<table>_<column>_check`.
func TestColumnCheckNames(t *testing.T) {
	tests := []struct {
		dialect string
		want    map[string]string
	}{
		{
			dialect: "postgres",
			want:    map[string]string{"a": "e_check", "b": "e_check1", "c": "e_c_check", "d": "e_d_positive"},
		},
		{
			dialect: "mysql",
			want:    map[string]string{"a": "e_a_check", "b": "e_b_check", "c": "e_c_check", "d": "e_d_positive"},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{StructName: "E", Name: "e"}

			got := schemaprep.ColumnCheckNames(table, columnCheckFields(), nil, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestColumnCheckNames_ColumnNameIsTaken numbers a column's CHECK past the name
// an earlier column gives its own. Measured on PostgreSQL 18.6, `CREATE TABLE e
// (d INTEGER CONSTRAINT e_check CHECK (d > 0), a INTEGER CHECK (a IS NULL OR b
// IS NOT NULL), b INTEGER)` names the second `e_check1`.
func TestColumnCheckNames_ColumnNameIsTaken(t *testing.T) {
	c := qt.New(t)
	table := schemamodel.Table{StructName: "E", Name: "e"}
	fields := []schemamodel.Field{
		{StructName: "E", Name: "d", Type: "INTEGER", Check: "d > 0", CheckName: "e_check"},
		{StructName: "E", Name: "a", Type: "INTEGER", Check: "a IS NULL OR b IS NOT NULL"},
		{StructName: "E", Name: "b", Type: "INTEGER"},
	}

	got := schemaprep.ColumnCheckNames(table, fields, nil, "postgres")

	c.Assert(got, qt.DeepEquals, map[string]string{"d": "e_check", "a": "e_check1"})
}

// TestColumnCheckNames_DeclaredNameIsTaken numbers a column's CHECK past a name
// the table declares for a constraint of its own.
func TestColumnCheckNames_DeclaredNameIsTaken(t *testing.T) {
	c := qt.New(t)
	table := schemamodel.Table{StructName: "E", Name: "e"}
	declared := []schemamodel.Constraint{{StructName: "E", Name: "e_c_check", Type: "UNIQUE", Columns: []string{"c"}}}

	got := schemaprep.ColumnCheckNames(table, columnCheckFields(), declared, "postgres")

	c.Assert(got["c"], qt.Equals, "e_c_check1")
}

// TestTableCheckConstraints_NumberedPastColumnChecks names a `checks` entry past
// the name a column's CHECK takes. PostgreSQL creates the column's CHECK first,
// as `f_check` for `c > a`, and an entry named `f_check` beside it is refused
// with `check constraint "f_check" already exists`; measured on 18.6, the
// entry named `f_check1` is accepted. MySQL takes `<table>_chk_<n>`, which no
// entry name collides with.
func TestTableCheckConstraints_NumberedPastColumnChecks(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "postgres", want: "f_check1"},
		{dialect: "mysql", want: "f_check"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{StructName: "F", Name: "f", Checks: []string{"a > 0"}}
			fields := []schemamodel.Field{
				{StructName: "F", Name: "a", Type: "INTEGER"},
				{StructName: "F", Name: "c", Type: "INTEGER", Check: "c > a"},
			}

			got := schemaprep.TableCheckConstraints(table, fields, nil, test.dialect)

			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Name, qt.Equals, test.want)
		})
	}
}

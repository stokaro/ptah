package identifiervalidation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/internal/identifiervalidation"
)

// TestTableConstraints_UnnamedPrimaryKeyAndCheck lists the constraints of a
// table whose primary key and column CHECK nobody named. SQL Server takes the
// names Ptah writes for them; a server that names such a constraint itself
// lists neither, because no name the plan writes can collide.
func TestTableConstraints_UnnamedPrimaryKeyAndCheck(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		dialect string
		want    []identifiervalidation.Hosted
	}{
		{
			name:    "SQL Server",
			table:   "t1",
			dialect: "sqlserver",
			want:    []identifiervalidation.Hosted{{Name: "t1_pkey", Table: "dbo.t1"}, {Name: "t1_n_check", Table: "dbo.t1"}},
		},
		{
			name:    "SQL Server, a table named with its schema",
			table:   "sales.t1",
			dialect: "sqlserver",
			want:    []identifiervalidation.Hosted{{Name: "t1_pkey", Table: "dbo.t1"}, {Name: "t1_n_check", Table: "dbo.t1"}},
		},
		{name: "MySQL", table: "t1", dialect: "mysql"},
		{name: "PostgreSQL", table: "t1", dialect: "postgres"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fields := []schemamodel.Field{
				{StructName: test.table, Name: "id", Primary: true},
				{StructName: test.table, Name: "n", Check: "n > 0"},
			}

			got := identifiervalidation.TableConstraints("dbo.t1", schemamodel.Table{Name: test.table}, fields, nil, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestTableConstraints_NamedPrimaryKeyAndCheck is the control: on SQL Server a
// declared name is listed as it is written.
func TestTableConstraints_NamedPrimaryKeyAndCheck(t *testing.T) {
	c := qt.New(t)
	fields := []schemamodel.Field{
		{StructName: "t1", Name: "id", Primary: true},
		{StructName: "t1", Name: "n", Check: "n > 0", CheckName: "ck_n"},
	}

	got := identifiervalidation.TableConstraints("dbo.t1", schemamodel.Table{Name: "t1", PrimaryKeyName: "pk_t1"}, fields, nil, "sqlserver")

	c.Assert(got, qt.DeepEquals, []identifiervalidation.Hosted{{Name: "pk_t1", Table: "dbo.t1"}, {Name: "ck_n", Table: "dbo.t1"}})
}

// TestTableConstraints_PrimaryKeyAsATableConstraint reads a primary key a
// created table declares as one of its constraints: a named one is listed
// under its name alone, and an unnamed one under the name Ptah writes.
func TestTableConstraints_PrimaryKeyAsATableConstraint(t *testing.T) {
	tests := []struct {
		name       string
		constraint string
		want       []identifiervalidation.Hosted
	}{
		{name: "named", constraint: "pk_t1", want: []identifiervalidation.Hosted{{Name: "pk_t1", Table: "dbo.t1"}}},
		{name: "unnamed", want: []identifiervalidation.Hosted{{Name: "t1_pkey", Table: "dbo.t1"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			constraints := []schemamodel.Constraint{{Type: "PRIMARY KEY", Name: test.constraint, Table: "t1", Columns: []string{"id"}}}

			got := identifiervalidation.TableConstraints("dbo.t1", schemamodel.Table{Name: "t1"}, nil, constraints, "sqlserver")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestHeldDerivedConstraintNames lists the names a re-created table would
// take for the held primary key and column CHECK, on SQL Server alone.
func TestHeldDerivedConstraintNames(t *testing.T) {
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "dbo", Name: "MixedTable", Columns: []catalog.Column{{Name: "id", IsPrimaryKey: true}, {Name: "n"}}},
			{Schema: "dbo", Name: "keyless", Columns: []catalog.Column{{Name: "id"}}},
		},
		Constraints: []catalog.Constraint{
			{Schema: "dbo", TableName: "orders", Name: "PK__orders__3213E83F", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			{Schema: "dbo", TableName: "MixedTable", Name: "CK__MixedTabl__n__1234", Type: "CHECK", ColumnNames: []string{"n"}},
			{Schema: "dbo", TableName: "keyless", Name: "uq_keyless_id", Type: "UNIQUE", ColumnNames: []string{"id"}},
		},
	}
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "SQL Server", dialect: "sqlserver", want: []string{"MixedTable_pkey", "orders_pkey", "MixedTable_n_check"}},
		{name: "PostgreSQL", dialect: "postgres"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := identifiervalidation.HeldDerivedConstraintNames(database, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestConvertTable_CarriesThePrimaryKeyName is the hop where the name was lost.
//
// Everything else about the constraint was carried across -- its columns, its
// parts, its INCLUDE list -- and the one field that identifies it was not. A
// declaration naming its key therefore reached the renderer with nothing to
// render, so the inline collapse was a second symptom rather than the cause
// (stokaro/ptah#2180).
func TestConvertTable_CarriesThePrimaryKeyName(t *testing.T) {
	tests := []struct {
		name       string
		constraint *ast.ConstraintNode
		want       string
	}{
		{
			name:       "a named key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: "c_pk", Columns: []string{"b"}},
			want:       "c_pk",
		},
		{
			name:       "a named composite key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: "c_comp_pk", Columns: []string{"a", "b"}},
			want:       "c_comp_pk",
		},
		{
			// The control: a key with no name must not acquire one.
			name:       "an unnamed key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"b"}},
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := &ast.CreateTableNode{
				Name: "t",
				Columns: []*ast.ColumnNode{
					{Name: "a", Type: "INTEGER"},
					{Name: "b", Type: "INTEGER"},
				},
				Constraints: []*ast.ConstraintNode{test.constraint},
			}

			converted := sqlschema.ToTable(table, "")

			c.Assert(converted.PrimaryKeyName, qt.Equals, test.want)
			// The columns still come across, so a conversion that carried the
			// name and dropped everything else would fail here.
			c.Assert(converted.PrimaryKey, qt.DeepEquals, test.constraint.Columns)
		})
	}
}

// A primary key's name is read as the server stores it, like the name of any
// other constraint: a quoted name without its quotes and an unquoted one
// folded where the engine folds it. The model then holds the name a read of
// the database reports, so a plan against the database the file describes
// renames no key.
func TestRead_PrimaryKeyNameAsStored_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, want string
	}{
		{
			dialect: platform.Postgres,
			name:    "a quoted name pg_dump writes",
			sql: `CREATE TABLE public."Order" ("Id" bigint NOT NULL);
ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_pkey" PRIMARY KEY ("Id");`,
			want: "Order_pkey",
		},
		{
			dialect: platform.Postgres,
			name:    "a quoted name in CREATE TABLE",
			sql:     `CREATE TABLE "Order" ("Id" bigint NOT NULL, CONSTRAINT "Order_Key" PRIMARY KEY ("Id"));`,
			want:    "Order_Key",
		},
		{
			dialect: platform.Postgres,
			name:    "a quoted name on the column",
			sql:     `CREATE TABLE "Order" ("Id" bigint CONSTRAINT "Order_Key" PRIMARY KEY);`,
			want:    "Order_Key",
		},
		{
			dialect: platform.Postgres,
			name:    "an unquoted name, folded",
			sql:     `CREATE TABLE orders (id bigint, CONSTRAINT Orders_PK PRIMARY KEY (id));`,
			want:    "orders_pk",
		},
		{
			dialect: platform.Postgres,
			name:    "an unquoted name ALTER TABLE adds, folded",
			sql:     "CREATE TABLE orders (id bigint NOT NULL);\nALTER TABLE orders ADD CONSTRAINT Orders_PK PRIMARY KEY (id);",
			want:    "orders_pk",
		},
		{
			dialect: platform.Postgres,
			name:    "a quoted name a later statement renames",
			sql: `CREATE TABLE "Order" ("Id" bigint NOT NULL);
ALTER TABLE ONLY "Order" ADD CONSTRAINT "Order_pkey" PRIMARY KEY ("Id");
ALTER TABLE "Order" RENAME CONSTRAINT "Order_pkey" TO "Order_Key";`,
			want: "Order_Key",
		},
		{
			dialect: platform.SQLServer,
			name:    "a bracketed name on SQL Server, as written",
			sql:     "CREATE TABLE orders (id int NOT NULL, CONSTRAINT [Orders_PK] PRIMARY KEY (id));",
			want:    "Orders_PK",
		},
		{
			dialect: platform.MySQL,
			name:    "a backquoted name on MySQL",
			sql:     "CREATE TABLE orders (id int NOT NULL, CONSTRAINT `Orders_PK` PRIMARY KEY (id));",
			want:    "Orders_PK",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].PrimaryKeyName, qt.Equals, test.want)
		})
	}
}

// DROP CONSTRAINT finds a key a quoted name declared, by the same name.
func TestRead_DropQuotedPrimaryKeyName_HappyPath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(`CREATE TABLE "Order" ("Id" bigint NOT NULL);
ALTER TABLE ONLY "Order" ADD CONSTRAINT "Order_pkey" PRIMARY KEY ("Id");
ALTER TABLE "Order" DROP CONSTRAINT "Order_pkey";`), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].PrimaryKeyName, qt.Equals, "")
	c.Assert(database.Tables[0].PrimaryKey, qt.IsNil)
}

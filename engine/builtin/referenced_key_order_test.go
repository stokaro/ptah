package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// referencedKeyDatabase is children(p_a, p_b) referencing parents(a, b), with change
// giving parents its key.
func referencedKeyDatabase(change func(*schemamodel.Database)) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Parent", Name: "parents"}, {StructName: "Child", Name: "children"}},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "a", Type: "INTEGER"},
			{StructName: "Parent", Name: "b", Type: "INTEGER"},
			{StructName: "Child", Name: "p_a", Type: "INTEGER"},
			{StructName: "Child", Name: "p_b", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Name: "fk_children_parents", Type: "FOREIGN KEY", Columns: []string{"p_a", "p_b"},
			ForeignTable: "parents", ForeignColumns: []string{"a", "b"},
		}},
	}
	change(database)
	return database
}

// referencedKeyInOtherOrder are the ways of holding a key over a and b in the other
// order than the foreign key references them.
var referencedKeyInOtherOrder = []struct {
	name   string
	change func(*schemamodel.Database)
}{
	{name: "a table's primary key", change: func(d *schemamodel.Database) { d.Tables[0].PrimaryKey = []string{"b", "a"} }},
	{name: "the primary key its fields declare", change: func(d *schemamodel.Database) {
		d.Fields[0], d.Fields[1] = d.Fields[1], d.Fields[0]
		d.Fields[0].Primary, d.Fields[1].Primary = true, true
	}},
	{name: "a UNIQUE constraint", change: func(d *schemamodel.Database) {
		d.Constraints = append(d.Constraints, schemamodel.Constraint{StructName: "Parent", Name: "parents_b_a", Type: "UNIQUE", Columns: []string{"b", "a"}})
	}},
	{name: "a unique index", change: func(d *schemamodel.Database) {
		d.Indexes = []schemamodel.Index{{StructName: "Parent", Name: "parents_b_a", Fields: []string{"b", "a"}, Unique: true}}
	}},
}

// TestReferencedKeyOrder_HappyPath takes the order PostgreSQL and CockroachDB take.
// Both match a foreign key to a unique key over the same columns in any order.
func TestReferencedKeyOrder_HappyPath(t *testing.T) {
	for _, dialect := range []string{"postgres", "cockroachdb"} {
		for _, test := range referencedKeyInOtherOrder {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := builtin.GetOrderedCreateStatements(referencedKeyDatabase(test.change), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(strings.Join(statements, "\n"), qt.Contains, "fk_children_parents")
			})
		}
	}
}

// TestReferencedKeyOrder_FailurePath is what stays refused: a server that matches
// the key's order, where MySQL answers with Error 6125 and MariaDB with errno 150,
// and on any server a key over other columns.
func TestReferencedKeyOrder_FailurePath(t *testing.T) {
	const refused = ` requires referenced columns a, b on table "parents" to be .*`
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(referencedKeyDatabase(func(d *schemamodel.Database) {
				d.Tables[0].PrimaryKey = []string{"b", "a"}
			}), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, dialect+refused)
			c.Assert(statements, qt.IsNil)
		})
	}
	tests := []struct {
		name string
		key  []string
	}{
		{name: "a key over one of the columns", key: []string{"b"}},
		{name: "a key over more columns", key: []string{"b", "a", "c"}},
		{name: "a key over a column twice", key: []string{"a", "a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(referencedKeyDatabase(func(d *schemamodel.Database) {
				d.Fields = append(d.Fields, schemamodel.Field{StructName: "Parent", Name: "c", Type: "INTEGER"})
				d.Tables[0].PrimaryKey = test.key
			}), "postgres")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "postgres"+refused)
			c.Assert(statements, qt.IsNil)
		})
	}
}

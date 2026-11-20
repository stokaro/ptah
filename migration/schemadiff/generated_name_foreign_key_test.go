package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// keyedParents declares APP.PARENTS with the columns named as its primary key,
// which the referenced columns of a foreign key have to be on Oracle.
func keyedParents(columns ...string) *schemamodel.Database {
	parents := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "parents", Name: "parents"}}}
	for _, column := range columns {
		parents.Fields = append(parents.Fields, schemamodel.Field{StructName: "parents", Name: column, Type: "NUMBER", Primary: true})
	}
	return parents
}

// heldParents is the read of APP.PARENTS keyed by the columns named.
func heldParents(columns ...string) (catalog.Table, catalog.Constraint) {
	table := catalog.Table{Name: "PARENTS", Schema: "APP", Type: "TABLE"}
	var names []string
	for _, column := range columns {
		table.Columns = append(table.Columns, catalog.Column{
			Name: column, DataType: "NUMBER", UDTName: "NUMBER", IsNullable: "NO", IsPrimaryKey: true,
		})
		names = append(names, column)
	}
	return table, catalog.Constraint{
		Name: "PARENTS_PK", TableName: "PARENTS", Schema: "APP", Type: "PRIMARY KEY",
		ColumnName: names[0], ColumnNames: names,
	}
}

// kidsDeclaring declares APP.KIDS and APP.PARENTS with a table-level foreign
// key from kids (x, y) to parents (a, b), named name, or unnamed when name is
// empty.
func kidsDeclaring(name, onDelete string) *schemamodel.Database {
	desired := declaredTable("kids", "x", "y")
	parents := keyedParents("a", "b")
	desired.Tables = append(desired.Tables, parents.Tables...)
	desired.Fields = append(desired.Fields, parents.Fields...)
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "kids", Name: name, Type: "FOREIGN KEY", Table: "kids",
		Columns: []string{"x", "y"}, ForeignTable: "parents", ForeignColumns: []string{"a", "b"}, OnDelete: onDelete,
	}}
	return desired
}

// kidsDeclaringInline declares the same tables with a single-column key on the
// field, which is how a declaration carries one.
func kidsDeclaringInline(name string) *schemamodel.Database {
	desired := declaredTable("kids", "x")
	parents := keyedParents("a")
	desired.Tables = append(desired.Tables, parents.Tables...)
	desired.Fields = append(desired.Fields, parents.Fields...)
	for i := range desired.Fields {
		if desired.Fields[i].StructName == "kids" && desired.Fields[i].Name == "x" {
			desired.Fields[i].Foreign = "parents(a)"
			desired.Fields[i].ForeignKeyName = name
		}
	}
	return desired
}

// kidsDeclaringTwoInline declares APP.KIDS with a key from x and another from
// y, both to parents (a), each carried on its field and unnamed.
func kidsDeclaringTwoInline() *schemamodel.Database {
	desired := declaredTable("kids", "x", "y")
	parents := keyedParents("a")
	desired.Tables = append(desired.Tables, parents.Tables...)
	desired.Fields = append(desired.Fields, parents.Fields...)
	for i := range desired.Fields {
		if desired.Fields[i].StructName == "kids" && (desired.Fields[i].Name == "x" || desired.Fields[i].Name == "y") {
			desired.Fields[i].Foreign = "parents(a)"
		}
	}
	return desired
}

// kidsHolding is the read of APP.KIDS holding the foreign key key, which it
// completes with the table and the reference to APP.PARENTS.
func kidsHolding(key catalog.Constraint) *catalog.Database {
	parents, parentKey := heldParents(slices.Sorted(slices.Values(key.ForeignColumns))...)
	key.TableName, key.Schema, key.Type = "KIDS", "APP", "FOREIGN KEY"
	key.ColumnName = key.ColumnNames[0]
	key.ForeignTable = new("PARENTS")
	key.ForeignSchema = "APP"
	key.ForeignColumn = new(key.ForeignColumns[0])
	if key.DeleteRule == nil {
		key.DeleteRule = new("NO ACTION")
	}
	return &catalog.Database{
		StoredNames: true,
		Tables:      []catalog.Table{heldTable("KIDS", "X", "Y"), parents},
		Constraints: []catalog.Constraint{key, parentKey},
	}
}

// serverNamed is the foreign key Oracle created for a declaration that named
// none, over (x, y) and (a, b).
func serverNamed() catalog.Constraint {
	return catalog.Constraint{
		Name: "SYS_C009299", GeneratedName: true,
		ColumnNames: []string{"X", "Y"}, ForeignColumns: []string{"A", "B"},
	}
}

// TestCompare_GeneratedNameForeignKey_HappyPath compares an unnamed declared
// foreign key with the key Oracle created for it, which the server named
// SYS_C009299. The declaration takes the name Ptah derives, fk_kids_x_y, and
// the two are one key: nothing is planned. Keyed by name alone, the plan adds
// fk_kids_x_y, which Oracle refuses with ORA-02275 while SYS_C009299 stands,
// and drops SYS_C009299.
func TestCompare_GeneratedNameForeignKey_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared *schemamodel.Database
		held     catalog.Constraint
	}{
		{
			name:     "a composite key declared in the table",
			declared: kidsDeclaring("", ""),
			held:     serverNamed(),
		},
		{
			name:     "a composite key declared in the table with the delete rule the server reports",
			declared: kidsDeclaring("", "NO ACTION"),
			held:     serverNamed(),
		},
		{
			name:     "a single-column key declared on the field",
			declared: kidsDeclaringInline(""),
			held: catalog.Constraint{
				Name: "SYS_C009300", GeneratedName: true, ColumnNames: []string{"X"}, ForeignColumns: []string{"A"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.declared, kidsHolding(test.held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_GeneratedNameForeignKey_Replaced holds the controls where the
// declared key is another key, so the comparison adds it and drops the one the
// server named. A key the author named is not paired with a server-named one,
// a key over other columns or another table is another key, and a key whose
// definition differs is replaced by the one the declaration states.
func TestCompare_GeneratedNameForeignKey_Replaced(t *testing.T) {
	tests := []struct {
		name     string
		declared *schemamodel.Database
		held     catalog.Constraint
		added    []string
		removed  []string
	}{
		{
			name:     "a name the author wrote",
			declared: kidsDeclaring("fk_mine", ""),
			held:     serverNamed(),
			added:    []string{"fk_mine"},
			removed:  []string{"SYS_C009299"},
		},
		{
			name:     "a name the author wrote on the field",
			declared: kidsDeclaringInline("fk_mine"),
			held: catalog.Constraint{
				Name: "SYS_C009300", GeneratedName: true, ColumnNames: []string{"X"}, ForeignColumns: []string{"A"},
			},
			added:   []string{"fk_mine"},
			removed: []string{"SYS_C009300"},
		},
		{
			name:     "a delete rule the declaration changes",
			declared: kidsDeclaring("", "CASCADE"),
			held:     serverNamed(),
			added:    []string{"fk_kids_x_y"},
			removed:  []string{"SYS_C009299"},
		},
		{
			name:     "other local columns",
			declared: kidsDeclaring("", ""),
			held: catalog.Constraint{
				Name: "SYS_C009299", GeneratedName: true, ColumnNames: []string{"Y", "X"}, ForeignColumns: []string{"A", "B"},
			},
			added:   []string{"fk_kids_x_y"},
			removed: []string{"SYS_C009299"},
		},
		{
			name:     "other referenced columns",
			declared: kidsDeclaring("", ""),
			held: catalog.Constraint{
				Name: "SYS_C009299", GeneratedName: true, ColumnNames: []string{"X", "Y"}, ForeignColumns: []string{"B", "A"},
			},
			added:   []string{"fk_kids_x_y"},
			removed: []string{"SYS_C009299"},
		},
		{
			name:     "a key the read does not mark as server-named",
			declared: kidsDeclaring("", ""),
			held: catalog.Constraint{
				Name: "SYS_C009299", ColumnNames: []string{"X", "Y"}, ForeignColumns: []string{"A", "B"},
			},
			added:   []string{"fk_kids_x_y"},
			removed: []string{"SYS_C009299"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.declared, kidsHolding(test.held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.removed)
		})
	}
}

// TestCompare_GeneratedNameForeignKey_EachKeyFindsItsOwn compares two unnamed
// declared keys with the two keys the server named, listed in the other order.
// Each declaration takes the key over its own column, whatever the names sort
// as, so nothing is planned. Paired without regard to the reference, the first
// declaration takes the first key and both are replaced.
func TestCompare_GeneratedNameForeignKey_EachKeyFindsItsOwn(t *testing.T) {
	c := qt.New(t)
	desired := kidsDeclaringTwoInline()
	held := kidsHolding(catalog.Constraint{
		Name: "SYS_C009301", GeneratedName: true, ColumnNames: []string{"Y"}, ForeignColumns: []string{"A"},
	})
	other := held.Constraints[0]
	other.Name, other.ColumnNames, other.ColumnName = "SYS_C009302", []string{"X"}, "X"
	held.Constraints = append(held.Constraints, other)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, held, oracleRead(), nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

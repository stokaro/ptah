package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// pairsWithUnique declares APP.PAIRS with columns a and b and a UNIQUE over
// columns, named name, or unnamed when name is empty.
func pairsWithUnique(name string, columns ...string) *schemamodel.Database {
	desired := declaredTable("pairs", "a", "b")
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "pairs", Name: name, Type: "UNIQUE", Table: "pairs", Columns: columns,
	}}
	return desired
}

// pairsHolding is the read of APP.PAIRS holding one UNIQUE.
func pairsHolding(key catalog.Constraint) *catalog.Database {
	key.TableName, key.Schema, key.Type = "PAIRS", "APP", "UNIQUE"
	key.ColumnName = key.ColumnNames[0]
	return &catalog.Database{StoredNames: true, Tables: []catalog.Table{heldTable("PAIRS", "A", "B")}, Constraints: []catalog.Constraint{key}}
}

// TestCompare_GeneratedNameUnique_HappyPath compares an unnamed declared
// UNIQUE over two columns with the key Oracle created for it, which the server
// named SYS_C008753. A key over several columns has no column flag to carry
// it, so it is read as a constraint, and the two are one key: nothing is
// planned. Keyed by name alone, the declared key is added again, which Oracle
// refuses with ORA-02261.
func TestCompare_GeneratedNameUnique_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), pairsWithUnique("", "a", "b"),
		pairsHolding(catalog.Constraint{Name: "SYS_C008753", GeneratedName: true, ColumnNames: []string{"A", "B"}}),
		oracleRead(), nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestCompare_GeneratedNameUnique_NotPaired holds the controls: a key over
// other columns, or over the same columns in another order, is another key; a
// key the author named is not paired with one the server named; and a key
// the read does not mark as server-named is compared by its name.
func TestCompare_GeneratedNameUnique_NotPaired(t *testing.T) {
	tests := []struct {
		name     string
		declared *schemamodel.Database
		held     catalog.Constraint
		added    []string
		removed  []string
	}{
		{
			name:     "another column list",
			declared: pairsWithUnique("", "a", "id"),
			held:     catalog.Constraint{Name: "SYS_C008753", GeneratedName: true, ColumnNames: []string{"A", "B"}},
			added:    []string{""},
			removed:  []string{"SYS_C008753"},
		},
		{
			name:     "another order",
			declared: pairsWithUnique("", "b", "a"),
			held:     catalog.Constraint{Name: "SYS_C008753", GeneratedName: true, ColumnNames: []string{"A", "B"}},
			added:    []string{""},
			removed:  []string{"SYS_C008753"},
		},
		{
			name:     "a declared name",
			declared: pairsWithUnique("pairs_ab", "a", "b"),
			held:     catalog.Constraint{Name: "SYS_C008753", GeneratedName: true, ColumnNames: []string{"A", "B"}},
			added:    []string{"pairs_ab"},
			removed:  []string{"SYS_C008753"},
		},
		{
			name:     "a name the author chose",
			declared: pairsWithUnique("", "a", "b"),
			held:     catalog.Constraint{Name: "SYS_C008753", ColumnNames: []string{"A", "B"}},
			added:    []string{""},
			removed:  []string{"SYS_C008753"},
		},
		{
			name:     "a deferred key",
			declared: pairsWithUnique("", "a", "b"),
			held:     catalog.Constraint{Name: "SYS_C008753", GeneratedName: true, Deferrable: true, Initially: "deferred", ColumnNames: []string{"A", "B"}},
			added:    []string{""},
			removed:  []string{"SYS_C008753"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.declared, pairsHolding(test.held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.removed)
		})
	}
}

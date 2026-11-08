package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
)

// The constraints of the named columns are their CHECKs and foreign keys,
// in declaration order, each under the name the comparison pairs it by. An
// unnamed CHECK takes the name the target's server gives it, which can depend
// on the columns before it: MySQL numbers the table's unnamed CHECKs, and the
// column kept, which is not asked for, holds the first number. A column the
// declaration does not hold, and a column of another table, give nothing.
func TestColumnConstraints_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		wantCheck string
	}{
		{name: "PostgreSQL", target: "postgres", wantCheck: "items_score_check"},
		{name: "MySQL", target: "mysql", wantCheck: "items_chk_2"},
		{name: "an engine without a descriptor", target: "fixture", wantCheck: "items_score_check"},
	}
	declaration := schemacapture.TableDeclaration{
		Table: schemamodel.Table{Name: "items", StructName: "Item"},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Item", Type: "INTEGER", Primary: true},
			{Name: "kept", StructName: "Item", Type: "INTEGER", Check: "kept > 0"},
			{Name: "score", StructName: "Item", Type: "INTEGER", Check: "score >= 0"},
			{Name: "owner_id", StructName: "Item", Type: "INTEGER", Foreign: "owners(id)", OnDelete: "CASCADE"},
			{Name: "label", StructName: "Item", Type: "TEXT", Check: "label <> ''", CheckName: "items_label_named"},
			{Name: "score", StructName: "Other", Type: "INTEGER", Check: "score < 0"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := goschematodb.ColumnConstraints(declaration, []string{"score", "owner_id", "label", "missing"}, test.target)

			c.Assert(got, qt.HasLen, 3)
			c.Assert(got[0].Name, qt.Equals, test.wantCheck)
			c.Assert(got[0].Type, qt.Equals, "CHECK")
			c.Assert(got[0].TableName, qt.Equals, "items")
			c.Assert(*got[0].CheckClause, qt.Equals, "score >= 0")
			c.Assert(got[0].ColumnNames, qt.DeepEquals, []string{"score"})
			c.Assert(got[1].Name, qt.Equals, "fk_items_owner_id")
			c.Assert(got[1].Type, qt.Equals, "FOREIGN KEY")
			c.Assert(got[1].ColumnNames, qt.DeepEquals, []string{"owner_id"})
			c.Assert(*got[1].ForeignTable, qt.Equals, "owners")
			c.Assert(got[1].ForeignColumns, qt.DeepEquals, []string{"id"})
			c.Assert(*got[1].DeleteRule, qt.Equals, "CASCADE")
			c.Assert(got[2].Name, qt.Equals, "items_label_named")
			c.Assert(*got[2].CheckClause, qt.Equals, "label <> ''")
		})
	}
}

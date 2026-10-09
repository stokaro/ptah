package schemacapture_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

func TestDeclareTableKeepsExplicitConstraintOwnership(t *testing.T) {
	for _, structName := range []string{"", "Shared"} {
		t.Run("struct="+structName, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{Schema: "app", Name: "events", StructName: structName}
			source := &schemamodel.Database{Tables: []schemamodel.Table{table}, Constraints: []schemamodel.Constraint{
				{Name: "foreign_key", Type: "PRIMARY KEY", Table: "other", StructName: structName, Columns: []string{"other_id"}},
				{Name: "own_key", Type: "PRIMARY KEY", Table: "app.events", StructName: structName, Columns: []string{"id"}},
			}}
			capture := schemacapture.DeclareTable(source, table, identifier.ForDialect("clickhouse"))
			c.Assert(capture.Constraints, qt.HasLen, 1)
			c.Assert(capture.Constraints[0].Name, qt.Equals, "own_key")
			capture.Constraints[0].Columns[0] = "changed"
			c.Assert(source.Constraints[1].Columns, qt.DeepEquals, []string{"id"})
		})
	}
}

func TestConstraintsForUsesStructOnlyWithoutExplicitTable(t *testing.T) {
	c := qt.New(t)
	source := []schemamodel.Constraint{
		{Name: "by_struct", StructName: "Event"},
		{Name: "foreign", StructName: "Event", Table: "other"},
		{Name: "by_table", StructName: "Other", Table: "events"},
		{Name: "unowned"},
	}
	result := schemacapture.ConstraintsFor(source, schemamodel.Table{Name: "events", StructName: "Event"})
	c.Assert(result, qt.DeepEquals, []schemamodel.Constraint{source[0], source[2]})
}

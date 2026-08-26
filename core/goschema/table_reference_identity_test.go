package goschema_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemamodel"
)

func TestParseSource_TableDirectivePreservesReferenceIdentity(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantSchema string
		wantTable  string
	}{
		{
			name:       "qualified",
			annotation: `name="tenant.data"`,
			wantSchema: "tenant",
			wantTable:  "data",
		},
		{
			name:       "bracket quoted literal dot",
			annotation: `name="[tenant.data]"`,
			wantTable:  "tenant.data",
		},
		{
			name:       "backtick quoted literal dot",
			annotation: "name=\"`tenant.data`\"",
			wantTable:  "tenant.data",
		},
		{
			name:       "double quoted literal dot",
			annotation: `name="\"tenant.data\""`,
			wantTable:  "tenant.data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source := fmt.Sprintf(
				"package models\n\n//ptah:schema:table %s\ntype Item struct{}\n",
				tt.annotation,
			)

			database, err := goschema.ParseSource("models.go", source)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].Schema, qt.Equals, tt.wantSchema)
			c.Assert(database.Tables[0].Name, qt.Equals, tt.wantTable)
		})
	}
}

func TestFinalize_ForeignKeysPreserveReferenceIdentity(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
			{StructName: "Child", Name: "literal_id", Type: "INTEGER", Foreign: `"tenant.data"(id)`},
			{StructName: "Child", Name: "qualified_id", Type: "INTEGER", Foreign: "tenant.data(id)"},
		},
	}

	schemamodel.Finalize(database)

	c.Assert(database.Dependencies["children"], qt.DeepEquals, []string{`"tenant.data"`, "tenant.data"})
}

func TestFinalize_ExplicitTableReferencesOverrideStructAssociation(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Literal",
			Name:       "qualified_lookup",
			TableName:  "tenant.data",
			Fields:     []string{"id"},
		}},
		Constraints: []schemamodel.Constraint{{
			StructName:      "Literal",
			Name:            "qualified_check",
			Type:            "CHECK",
			Table:           "tenant.data",
			CheckExpression: "id > 0",
		}},
		Triggers: []schemamodel.Trigger{{
			StructName: "Literal",
			Name:       "qualified_trigger",
			Table:      "tenant.data",
			Body:       "SELECT 1",
		}},
	}

	schemamodel.Finalize(database)

	c.Assert(database.Indexes[0].TableName, qt.Equals, "tenant.data")
	c.Assert(database.Constraints[0].Table, qt.Equals, "tenant.data")
	c.Assert(database.Triggers[0].Table, qt.Equals, "tenant.data")
}

func TestQualifiedSchemaObjectNamesPreserveReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert(schemamodel.Domain{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(schemamodel.Domain{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(schemamodel.CompositeType{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(schemamodel.CompositeType{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(schemamodel.Range{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(schemamodel.Range{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(schemamodel.Sequence{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(schemamodel.Sequence{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
}

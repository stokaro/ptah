package schemaprojection_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
)

func TestCreationColumnsRequireCompleteOrderedFacts(t *testing.T) {
	subject := objectidentity.NewBuilder(identifier.Semantics{}).Table("records")
	request := schemaprojection.TableCreationRequest{Tables: []schemaprojection.TableCreationInput{{Subject: subject, Declaration: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "records"}, Fields: []schemamodel.Field{{Name: "id", Type: "BIGINT", Nullable: true}, {Name: "value", Type: "TEXT"}}}}}}
	result := schemaprojection.TableCreationResult{Complete: true, Tables: []schemaprojection.TableCreation{{Subject: subject, ColumnsPrepared: true, Columns: []catalog.Column{{Name: "id", ColumnType: "Int64", IsNullable: "NO"}, {Name: "value", DataType: "TEXT", IsNullable: "YES", ColumnDefault: new("typed()")}}}}}
	c := qt.New(t)
	accepted, err := schemaprojection.AcceptTableCreations(request, result)
	c.Assert(err, qt.IsNil)
	*accepted.Tables[0].Columns[1].ColumnDefault = "changed"
	c.Assert(*result.Tables[0].Columns[1].ColumnDefault, qt.Equals, "typed()")
	c.Assert(request.Tables[0].Declaration.Fields[0].Nullable, qt.IsTrue)
	for _, row := range []struct {
		name   string
		mutate func(*schemaprojection.TableCreation)
	}{
		{"missing", func(v *schemaprojection.TableCreation) { v.Columns = v.Columns[:1] }},
		{"order", func(v *schemaprojection.TableCreation) { v.Columns[0], v.Columns[1] = v.Columns[1], v.Columns[0] }},
		{"unknown nullability", func(v *schemaprojection.TableCreation) { v.Columns[0].IsNullable = "" }},
		{"unknown type", func(v *schemaprojection.TableCreation) { v.Columns[0].ColumnType = "" }},
		{"unprepared", func(v *schemaprojection.TableCreation) { v.ColumnsPrepared = false }},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			changed := result.Clone()
			row.mutate(&changed.Tables[0])
			actual, err := schemaprojection.AcceptTableCreations(request, changed)
			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(actual.Tables, qt.IsNil)
		})
	}
}

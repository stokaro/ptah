package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

func unvalidatedCheckDatabase(notValid bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "pos", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_pos_positive", Type: "CHECK",
			CheckExpression: "pos > 0", NotValid: notValid,
		}},
	}
}

// A constraint the server has not validated has no annotation spelling, so the
// export refuses it rather than write one that reads back validated
// (stokaro/ptah#3853).
func TestRender_NotValidConstraint_FailurePath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(unvalidatedCheckDatabase(true), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorMatches, `CHECK "slots_pos_positive" is NOT VALID, which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

// The control: the validated CHECK is written.
func TestRender_NotValidConstraint_HappyPath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(unvalidatedCheckDatabase(false), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
}

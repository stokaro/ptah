package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

func deferrableUniqueDatabase(deferrable bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "pos", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_pos_key", Type: "UNIQUE",
			Columns: []string{"pos"}, Deferrable: deferrable,
		}},
	}
}

// A key that defers its check has no annotation spelling, so the export
// refuses it rather than write a key that checks at once.
func TestRender_DeferrableKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(deferrableUniqueDatabase(true), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorMatches, `UNIQUE "slots_pos_key" defers its check, which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

// The control: the key that does not defer is written.
func TestRender_DeferrableKey_HappyPath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(deferrableUniqueDatabase(false), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
}

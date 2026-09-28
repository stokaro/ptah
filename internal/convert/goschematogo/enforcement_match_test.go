package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

func matchDatabase(match string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "C", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "C", Name: "p_id", Type: "INTEGER", Nullable: true, Foreign: "p(id)", ForeignKeyMatch: match},
		},
	}
}

// A foreign key's MATCH type has no annotation spelling, so the export refuses
// the key rather than write a MATCH SIMPLE one (stokaro/ptah#3853).
func TestRender_EnforcementAndMatch_FailurePath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(matchDatabase("FULL"), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorMatches, `the foreign key of column "p_id" is MATCH FULL, which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

// The control: the key without a MATCH type is written.
func TestRender_EnforcementAndMatch_HappyPath(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(matchDatabase(""), goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
}

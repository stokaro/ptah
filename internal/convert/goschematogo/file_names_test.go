package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// Each table is written to a file of its own. A table's file name lower-cases
// its qualified name, so "Docs" and docs derive one, and a table called enums
// derives the name of the enum file. Written under one name, the later file
// replaces the earlier on disk and a table disappears from the output
// (stokaro/ptah#3647).
func TestRender_PerTableFileNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		database schemamodel.Database
		want     []string
	}{
		{
			name: "two tables whose qualified names lower-case alike",
			database: schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "Docs", Schema: "app", StructName: "Docs"},
					{Name: "docs", Schema: "app", StructName: "Docs2"},
				},
				Fields: []schemamodel.Field{
					{StructName: "Docs", Name: "a", Type: "integer", FieldName: "A"},
					{StructName: "Docs2", Name: "b", Type: "integer", FieldName: "B"},
				},
			},
			want: []string{"app_docs.go", "app_docs2.go"},
		},
		{
			name: "a table named after the enum file",
			database: schemamodel.Database{
				Enums:  []schemamodel.Enum{{Name: "mood", Values: []string{"ok"}}},
				Tables: []schemamodel.Table{{Name: "enums", StructName: "Enums"}},
				Fields: []schemamodel.Field{{StructName: "Enums", Name: "a", Type: "integer", FieldName: "A"}},
			},
			want: []string{"enums.go", "enums2.go"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(&test.database, goschematogo.Options{PerTable: true})

			c.Assert(err, qt.IsNil)
			c.Assert(fileNames(files), qt.DeepEquals, test.want)
		})
	}
}

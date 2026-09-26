package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// An enum's comment survives an export to Go annotations and back
// (stokaro/ptah#3646).
func TestRender_EnumCommentRoundTrip(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Enums: []schemamodel.Enum{
		{Name: "mood", Values: []string{"ok"}, Comment: "how it went"},
	}}

	files, err := goschematogo.Render(database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	parsed, err := goschema.ParseSource("schema.go", source.String())

	c.Assert(err, qt.IsNil, qt.Commentf("the exported source does not parse:\n%s", source.String()))
	c.Assert(parsed.Enums, qt.HasLen, 1)
	c.Assert(parsed.Enums[0].Comment, qt.Equals, "how it went")
}

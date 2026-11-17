package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// An exported role writes its settings on its directive, and the directive
// reads back as the declaration it was written from.
func TestRender_RoleSettingsReadBack(t *testing.T) {
	c := qt.New(t)
	settings := []pgrole.Setting{
		{Name: "App.Tenant", Value: "'a;b'"},
		{Name: "search_path", Value: "'$user', public"},
		{Name: "statement_timeout", Value: "'30s'"},
		{Name: "work_mem", Reset: true},
	}
	db := &schemamodel.Database{Roles: []schemamodel.Role{{StructName: "AppReader", Name: "app_reader", Login: true, Inherit: true,
		Facets: must.Must(pgrole.WithSettings(schemaext.Facets{}, settings))}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	c.Assert(annotationLine(c, source.String(), "ptah:schema:role"), qt.Contains,
		`settings="\"App\".\"Tenant\" = 'a;b'; \"search_path\" = '$user', public; \"statement_timeout\" = '30s'" reset_settings="\"work_mem\""`)

	read, err := goschema.ParseSource(builtintest.Annotations(), "roles.go", source.String())
	c.Assert(err, qt.IsNil)
	c.Assert(read.Roles, qt.HasLen, 1)
	got, _, err := pgrole.SettingsOf(read.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, settings)
}

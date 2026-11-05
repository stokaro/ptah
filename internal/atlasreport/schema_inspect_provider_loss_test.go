package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// The JSON document of an inspection through an executable provider names
// what it leaves out: no Atlas edition has a driver for an engine only a
// provider serves, so no Atlas document's silence is departed from. The same
// read of a built-in engine Atlas has a driver for reports nothing.
func TestSchemaInspectJSON_ReportsWhatAProviderEngineLeavesOut(t *testing.T) {
	db := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "user"}}, Grants: []schemamodel.Grant{{}}, Views: []schemamodel.View{{Name: "v"}}}
	for _, test := range []struct {
		name string
		info catalog.ServerInfo
		want string
	}{
		{"an engine a provider serves", catalog.ServerInfo{Dialect: "provided", Provider: catalog.ProviderIdentity{ID: "example.org/provided"}},
			"warning: JSON schema inspection leaves out grants (1)\n" +
				"warning: JSON schema inspection leaves out roles (1)\n" +
				"warning: JSON schema inspection leaves out views (1)\n"},
		{"a built-in engine with an Atlas driver", catalog.ServerInfo{Dialect: "postgres"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			report := newInspectReport(c, db, &catalog.Database{}, test.info, &diagnostics, atlasreport.SchemaInspectReportOptions{})

			_, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.String(), qt.Equals, test.want)
		})
	}
}

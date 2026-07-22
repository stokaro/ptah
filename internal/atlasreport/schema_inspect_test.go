package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasreport"
)

func TestNormalizeSchemaInspectFormat_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "default", format: "", want: "{{ $.MarshalHCL }}"},
		{name: "hcl", format: "hcl", want: "{{ $.MarshalHCL }}"},
		{name: "sql", format: "sql", want: "{{ sql . }}"},
		{name: "json", format: "json", want: "{{ json . }}"},
		{name: "custom", format: "{{ json . }}", want: "{{ json . }}"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasreport.NormalizeSchemaInspectFormat(test.format)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestRenderSchemaInspectFormat_JSONTemplate(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaInspectReport(
		&goschema.Database{},
		&types.DBSchema{
			Tables: []types.DBTable{
				{
					Name:   "users",
					Schema: "main",
					Columns: []types.DBColumn{
						{Name: "id", DataType: "integer", IsNullable: "NO"},
					},
				},
			},
		},
		types.DBInfo{Dialect: "sqlite", Schema: "main"},
		nil,
	)

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `"schemas":[{"name":"main"`)
	c.Assert(rendered, qt.Contains, `"tables":[{"name":"users"`)
	c.Assert(rendered, qt.Contains, `"columns":[{"name":"id","type":"integer"`)
}

func TestValidateSchemaInspectTemplate_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaInspectTemplate(`{{ unknown . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "unknown" not defined.*`)
}

func TestRenderSchemaInspectFormat_UnsupportedFileTemplateFunction(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaInspectReport(
		&goschema.Database{},
		&types.DBSchema{},
		types.DBInfo{Dialect: "sqlite", Schema: "main"},
		nil,
	)

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ split . }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*atlas schema inspect accepts split/write templates, but Ptah does not implement their behavior yet`)
	c.Assert(rendered, qt.Equals, "")
}

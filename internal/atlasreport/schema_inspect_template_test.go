package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasreport"
)

// publicSchemaReport is the report of a PostgreSQL database holding one empty
// schema.
func publicSchemaReport(c *qt.C) *atlasreport.SchemaInspectReport {
	c.Helper()
	db := excludeTable()
	db.Constraints = nil
	return newInspectReport(c,
		db, &catalog.Database{Schemas: []catalog.Schema{{Name: "public"}}},
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil,
		atlasreport.SchemaInspectReportOptions{},
	)
}

// TestSchemaInspectTemplate_HappyPath renders the members the pinned community
// binary v1.3.0 hands a `schema inspect --format` template, each measured
// there on PostgreSQL 18.
func TestSchemaInspectTemplate_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "the realm lists the schemas",
			format: `{{ len .Realm.Schemas }}`,
			want:   "1",
		},
		{
			name:   "MarshalSQL writes the schema",
			format: `{{ printf "%.12s" .MarshalSQL }}`,
			want:   `CREATE TABLE`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := publicSchemaReport(c)

			got, err := atlasreport.RenderSchemaInspect(test.format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Text, qt.Equals, test.want)
		})
	}
}

// TestSchemaInspectTemplate_FailurePath refuses the members the community
// binary's report does not carry: the realm is `.Realm` alone there, and the
// report has no DBML method. Ptah writes DBML through the `dbml` helper.
func TestSchemaInspectTemplate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr string
	}{
		{
			name:    "the realm is not also Schema",
			format:  `{{ .Schema }}`,
			wantErr: `template: format:1:3: executing "format" at <.Schema>: can't evaluate field Schema in type \*atlasreport.SchemaInspectReport`,
		},
		{
			name:    "the report has no MarshalDBML",
			format:  `{{ .MarshalDBML }}`,
			wantErr: `template: format:1:3: executing "format" at <.MarshalDBML>: can't evaluate field MarshalDBML in type \*atlasreport.SchemaInspectReport`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := publicSchemaReport(c)

			got, err := atlasreport.RenderSchemaInspect(test.format, report)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.Text, qt.Equals, "")
		})
	}
}

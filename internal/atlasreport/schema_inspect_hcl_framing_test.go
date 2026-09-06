package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/atlasreport"
)

func TestCompatibilityHCLFraming_EmptySQLiteExactBytes(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "default", format: ""},
		{name: "hcl name", format: "hcl"},
		{name: "hcl helper", format: `{{ hcl . }}`},
		{name: "MarshalHCL method", format: `{{ $.MarshalHCL }}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			format, err := atlasreport.NormalizeSchemaInspectFormat(test.format)
			c.Assert(err, qt.IsNil)

			output, err := atlasreport.RenderSchemaInspect(format, emptySQLiteInspectReport(true))

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, "schema \"main\" {\n}\n")
		})
	}
}

func TestCompatibilityHCLFraming_PopulatedDocumentKeepsOnlyOneMarkerOccurrence(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaInspectReport(
		&schemamodel.Database{
			Schemas: []schemamodel.Schema{{
				Name:    "main",
				Comment: atlashclrender.GeneratedCodeMarker,
			}},
			Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: "main"}},
			Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.SQLite, Schema: "main"},
		nil,
		atlasreport.SchemaInspectReportOptions{
			DescribeSchemas:         true,
			CompatibilityHCLFraming: true,
		},
	)

	hcl, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(hcl, atlashclrender.GeneratedCodeMarker), qt.IsFalse)
	c.Assert(strings.Count(hcl, atlashclrender.GeneratedCodeMarker), qt.Equals, 1,
		qt.Commentf("the marker text inside the schema comment must survive"))
	c.Assert(hcl, qt.Contains, `table "users"`)
	c.Assert(strings.HasSuffix(hcl, "\n"), qt.IsTrue)
	c.Assert(strings.HasSuffix(hcl, "\n\n"), qt.IsFalse)
}

func TestCompatibilityHCLFraming_PreservesPostgreSQLCoverageDirectives(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaInspectReport(
		&schemamodel.Database{
			Schemas:    []schemamodel.Schema{{Name: "public"}},
			Tables:     []schemamodel.Table{{StructName: "User", Name: "users", Schema: "public"}},
			Fields:     []schemamodel.Field{{StructName: "User", Name: "id", Type: "bigint"}},
			Extensions: []schemamodel.Extension{{Name: "pgcrypto"}},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
		nil,
		atlasreport.SchemaInspectReportOptions{
			OmitAtlasRefusedBlocks:  true,
			DescribeSchemas:         true,
			CompatibilityHCLFraming: true,
		},
	)

	hcl, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(hcl, "// ptah:not-described extension reason=suppressed provenance=defaulted\n"), qt.IsTrue)
	c.Assert(hcl, qt.Not(qt.Contains), atlashclrender.GeneratedCodeMarker)
	covered, err := coverage.DecodeHeader(hcl)
	c.Assert(err, qt.IsNil)
	c.Assert(covered, qt.DeepEquals, suppressedBlocks(
		coverage.Extension,
		coverage.Policy,
		coverage.Sequence,
	))
}

func TestCompatibilityHCLFraming_IsIndependentOfBlockPolicy(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaInspectReport(
		&schemamodel.Database{
			Schemas:    []schemamodel.Schema{{Name: "public"}},
			Extensions: []schemamodel.Extension{{Name: "pgcrypto"}},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
		nil,
		atlasreport.SchemaInspectReportOptions{
			OmitAtlasRefusedBlocks:  false,
			DescribeSchemas:         true,
			CompatibilityHCLFraming: true,
		},
	)

	hcl, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(hcl, qt.Contains, `extension "pgcrypto"`)
	c.Assert(hcl, qt.Not(qt.Contains), "ptah:not-described")
	c.Assert(hcl, qt.Not(qt.Contains), atlashclrender.GeneratedCodeMarker)
	c.Assert(strings.HasSuffix(hcl, "\n"), qt.IsTrue)
	c.Assert(strings.HasSuffix(hcl, "\n\n"), qt.IsFalse)
}

func TestCompatibilityHCLFraming_DoesNotChangeJSONOrSQL(t *testing.T) {
	formats := []string{`{{ json . }}`, `{{ sql . }}`}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			c := qt.New(t)
			native, err := atlasreport.RenderSchemaInspect(format, emptySQLiteInspectReport(false))
			c.Assert(err, qt.IsNil)
			compat, err := atlasreport.RenderSchemaInspect(format, emptySQLiteInspectReport(true))
			c.Assert(err, qt.IsNil)

			c.Assert(compat, qt.DeepEquals, native)
		})
	}
}

func TestCompatibilityHCLFraming_NativeDocumentIsByteIdentical(t *testing.T) {
	c := qt.New(t)

	hcl, err := emptySQLiteInspectReport(false).MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(hcl, qt.Equals,
		atlashclrender.GeneratedCodeMarker+"\n\nschema \"main\" {\n}\n\n")
}

func emptySQLiteInspectReport(compatibilityHCLFraming bool) *atlasreport.SchemaInspectReport {
	return atlasreport.NewSchemaInspectReport(
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "main"}}},
		&catalog.Database{Schemas: []catalog.Schema{{Name: "main"}}},
		catalog.ServerInfo{Dialect: platform.SQLite, Schema: "main"},
		nil,
		atlasreport.SchemaInspectReportOptions{
			DescribeSchemas:         true,
			CompatibilityHCLFraming: compatibilityHCLFraming,
		},
	)
}

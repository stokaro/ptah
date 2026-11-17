package atlas

// White-box testing required: this test isolates the compatibility-only
// template adapter before schema inspection crosses filesystem and database
// boundaries. End-to-end coverage lives under integration/sqlitecmd.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

func TestAtlasSchemaInspectCompatibilityFormat(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "empty stays empty"},
		{name: "bare SQL is literal", format: "sql", want: `{{ "sql" }}`},
		{name: "bare JSON is literal", format: "json", want: `{{ "json" }}`},
		{name: "bare HCL is literal", format: "hcl", want: `{{ "hcl" }}`},
		{name: "bare DBML is literal", format: "dbml", want: `{{ "dbml" }}`},
		{name: "wrapped SQL keeps bytes", format: " sql ", want: `{{ " sql " }}`},
		{name: "wrapped JSON keeps bytes", format: " json ", want: `{{ " json " }}`},
		{name: "wrapped HCL keeps bytes", format: " hcl ", want: `{{ " hcl " }}`},
		{name: "wrapped DBML keeps bytes", format: " dbml ", want: `{{ " dbml " }}`},
		{name: "explicit SQL helper is unchanged", format: `{{ sql . }}`, want: `{{ sql . }}`},
		{name: "explicit JSON helper is unchanged", format: `{{ json . }}`, want: `{{ json . }}`},
		{name: "explicit HCL helper is unchanged", format: `{{ hcl . }}`, want: `{{ hcl . }}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasreport.TemplateExtensionsEnvVar, "0")

			got, err := atlasSchemaInspectCompatibilityFormat(test.format)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestAtlasSchemaInspectCompatibilityFormat_Extended reads bare dbml as the
// DBML document once atlasreport.TemplateExtensionsEnvVar is on, and leaves
// the other shorthands literal.
func TestAtlasSchemaInspectCompatibilityFormat_Extended(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "bare DBML is the document", format: "dbml", want: "dbml"},
		{name: "bare SQL stays literal", format: "sql", want: `{{ "sql" }}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")

			got, err := atlasSchemaInspectCompatibilityFormat(test.format)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestAtlasSchemaInspectCompatibilityFormat_FailurePath refuses a malformed
// value of the variable whatever the format is.
func TestAtlasSchemaInspectCompatibilityFormat_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.TemplateExtensionsEnvVar, "yes")

	got, err := atlasSchemaInspectCompatibilityFormat("")

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_ATLAS_TEMPLATE_EXTENSIONS`)
	c.Assert(got, qt.Equals, "")
}

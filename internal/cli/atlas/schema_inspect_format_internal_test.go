package atlas

// White-box testing required: this test isolates the compatibility-only
// template adapter before schema inspection crosses filesystem and database
// boundaries. End-to-end coverage lives under integration/sqlitecmd.

import (
	"testing"

	qt "github.com/frankban/quicktest"
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
		{name: "wrapped SQL keeps bytes", format: " sql ", want: `{{ " sql " }}`},
		{name: "wrapped JSON keeps bytes", format: " json ", want: `{{ " json " }}`},
		{name: "wrapped HCL keeps bytes", format: " hcl ", want: `{{ " hcl " }}`},
		{name: "explicit SQL helper is unchanged", format: `{{ sql . }}`, want: `{{ sql . }}`},
		{name: "explicit JSON helper is unchanged", format: `{{ json . }}`, want: `{{ json . }}`},
		{name: "explicit HCL helper is unchanged", format: `{{ hcl . }}`, want: `{{ hcl . }}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasSchemaInspectCompatibilityFormat(test.format)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

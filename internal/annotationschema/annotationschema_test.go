package annotationschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/annotationschema"
	"ptah.run/internal/tableref"
)

func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name, directive string
		values, want    map[string]string
	}{
		{
			name:      "the schema of an object, quoted with a capital",
			directive: "ptah:schema:domain",
			values:    map[string]string{"name": "d", "schema": `"App"`},
			want:      map[string]string{"name": "d", "schema": "App"},
		},
		{
			name:      "the schema a grant is on, quoted with a space",
			directive: "ptah:schema:grant",
			values:    map[string]string{"on_schema": `" app"`},
			want:      map[string]string{"on_schema": " app"},
		},
		{
			name:      "a bare schema with the space inside a quote around it",
			directive: "ptah:schema:table",
			values:    map[string]string{"name": " t", "schema": ` "a b" `},
			want:      map[string]string{"name": " t", "schema": "a b"},
		},
		{
			name:      "the name of a schema directive",
			directive: "ptah:schema:schema",
			values:    map[string]string{"name": "[App]"},
			want:      map[string]string{"name": "App"},
		},
		{
			name:      "the name of another directive is left as written",
			directive: "ptah:schema:sequence",
			values:    map[string]string{"name": `"App"`},
			want:      map[string]string{"name": `"App"`},
		},
		{
			name:      "white space alone",
			directive: "ptah:schema:sequence",
			values:    map[string]string{"schema": "  "},
			want:      map[string]string{"schema": ""},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := annotationschema.Read(test.directive, test.values)

			c.Assert(err, qt.IsNil)
			c.Assert(test.values, qt.DeepEquals, test.want)
		})
	}
}

func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name, directive string
		values          map[string]string
		want            string
	}{
		{
			name:      "a bare schema that ends with a space",
			directive: "ptah:schema:table",
			values:    map[string]string{"schema": "app "},
			want: `schema attribute "app " begins or ends with white space, which a bare name loses: ` +
				`write it as one quoted identifier, "app ", or remove the space`,
		},
		{
			name:      "a bare schema directive name that begins with a space",
			directive: "ptah:schema:schema",
			values:    map[string]string{"name": " app"},
			want: `name attribute " app" begins or ends with white space, which a bare name loses: ` +
				`write it as one quoted identifier, " app", or remove the space`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := annotationschema.Read(test.directive, test.values)

			c.Assert(err, qt.ErrorIs, tableref.ErrSpacedBareName)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

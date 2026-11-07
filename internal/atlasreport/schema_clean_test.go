package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/schemaclean"
)

func TestSchemaCleanFormatRendersJSONWithRedactedURL(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver: "sqlite",
		URL:    "sqlite://user:secret@db.sqlite?password=hidden&token=private",
		DryRun: true,
		Plan: schemaclean.Plan{
			Objects: []schemaclean.Object{{Type: "table", Name: "users"}},
			Changes: []schemaclean.Change{{Type: "table", Name: "users", Cmd: `DROP TABLE IF EXISTS "users"`}},
		},
	})

	var out bytes.Buffer
	err := atlasreport.WriteSchemaClean(&out, "{{ json . }}", report)

	c.Assert(err, qt.IsNil)
	got := make(map[string]any)
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	gotURL := got["Env"].(map[string]any)["URL"].(map[string]any)
	c.Assert(gotURL["Scheme"], qt.Equals, "sqlite")
	c.Assert(gotURL["User"], qt.DeepEquals, make(map[string]any))
	c.Assert(gotURL["Host"], qt.Equals, "db.sqlite")
	c.Assert(gotURL["RawQuery"], qt.Equals, "password=xxxxx&token=xxxxx")
	c.Assert(gotURL["Schema"], qt.Equals, "main")
	c.Assert(got["DryRun"], qt.IsTrue)
	c.Assert(got["Applied"], qt.IsFalse)
	c.Assert(got["Changes"].([]any)[0].(map[string]any)["Cmd"], qt.Equals, `DROP TABLE IF EXISTS "users"`)
}

// The Atlas-shaped document names the URL with no credentials, for a URL net/url
// reads differently from how it was meant too, and for a parameter an engine
// names its secret in.
func TestSchemaCleanFormatHidesEverySecretOfTheURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "a / in a password", url: "postgres://app:12345/Xk9Lm@db.internal/shop"},
		{name: "a # in a password", url: "postgres://app:Zq7Kp@Xk9Lm#Vp8Qr@db.internal/shop"},
		{name: "a key", url: "sqlite://x.db?_key=Xk9Lm"},
		{name: "a password parameter", url: "sqlite://x.db?_auth_pass=Xk9Lm"},
		{name: "an auth token", url: "libsql://db.internal?authToken=Xk9Lm"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{Driver: "postgres", URL: test.url})

			var out bytes.Buffer
			err := atlasreport.WriteSchemaClean(&out, "{{ json . }}", report)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Not(qt.Contains), "Xk9Lm")
			c.Assert(out.String(), qt.Not(qt.Contains), "Zq7Kp")
			c.Assert(out.String(), qt.Not(qt.Contains), "Vp8Qr")
		})
	}
}

func TestSchemaCleanFormatRendersCustomTemplate(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver: "sqlite",
		Plan: schemaclean.Plan{
			Changes: []schemaclean.Change{{Type: "table", Name: "users", Cmd: `DROP TABLE IF EXISTS "users"`}},
		},
	})

	var out bytes.Buffer
	err := atlasreport.WriteSchemaClean(&out, `{{ .Env.Driver }}|{{ len .Changes }}|{{ (index .Changes 0).Cmd }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `sqlite|1|DROP TABLE IF EXISTS "users"`)
}

func TestSchemaCleanFormatPreservesFunctionParameters(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver: "postgres",
		Plan: schemaclean.Plan{
			Objects: []schemaclean.Object{
				{Type: "function", Schema: "app", Name: "normalize", Parameters: "integer"},
			},
			Changes: []schemaclean.Change{
				{
					Type:       "function",
					Schema:     "app",
					Name:       "normalize",
					Parameters: "integer",
					Cmd:        `DROP FUNCTION IF EXISTS "app"."normalize"(integer) CASCADE`,
				},
			},
		},
	})

	c.Assert(report.Objects[0].Parameters, qt.Equals, "integer")
	c.Assert(report.Changes[0].Parameters, qt.Equals, "integer")
}

func TestSchemaCleanFormatRendersURLAsStringInCustomTemplate(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver: "sqlite",
		URL:    "sqlite://user:secret@db.sqlite?password=hidden&token=private",
	})

	var out bytes.Buffer
	err := atlasreport.WriteSchemaClean(&out, "{{ .Env.URL }}", report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite://user@db.sqlite?password=xxxxx&token=xxxxx")
}

func TestSchemaCleanFormatRejectsInvalidTemplate(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaCleanTemplate("{{ if }}")

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
}

func TestSchemaCleanFormatRejectsExecutionErrors(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaCleanTemplate("{{ .DoesNotExist }}")

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*`)
}

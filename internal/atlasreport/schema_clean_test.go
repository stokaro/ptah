package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/schemaclean"
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
	got := map[string]any{}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	gotURL := got["Env"].(map[string]any)["URL"].(map[string]any)
	c.Assert(gotURL["Scheme"], qt.Equals, "sqlite")
	c.Assert(gotURL["User"], qt.DeepEquals, map[string]any{})
	c.Assert(gotURL["Host"], qt.Equals, "db.sqlite")
	c.Assert(gotURL["RawQuery"], qt.Equals, "password=xxxxx&token=xxxxx")
	c.Assert(gotURL["Schema"], qt.Equals, "main")
	c.Assert(got["DryRun"], qt.Equals, true)
	c.Assert(got["Applied"], qt.Equals, false)
	c.Assert(got["Changes"].([]any)[0].(map[string]any)["Cmd"], qt.Equals, `DROP TABLE IF EXISTS "users"`)
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

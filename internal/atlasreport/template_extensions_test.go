package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

// templateValidators is each compat verb's --format parse, by the name the
// community binary gives the verb.
var templateValidators = map[string]func(string) error{
	"migrate diff":   atlasreport.ValidateMigrateDiffTemplate,
	"schema diff":    atlasreport.ValidateSchemaDiffTemplate,
	"schema apply":   atlasreport.ValidateSchemaApplyTemplate,
	"migrate lint":   atlasreport.ValidateMigrateLintTemplate,
	"migrate status": atlasreport.ValidateMigrateStatusTemplate,
	"schema inspect": atlasreport.ValidateCompatSchemaInspectTemplate,
}

// helperCall is a template calling helper under a branch that never runs, so
// that parsing it asks whether the verb defines the helper and nothing else.
// The community binary v1.3.0 was measured with the same template.
func helperCall(helper string) string {
	return "{{ if false }}{{ " + helper + " }}{{ end }}"
}

// TestTemplateHelpers_HappyPath parses, on each verb, a helper the pinned
// community binary v1.3.0 defines there.
func TestTemplateHelpers_HappyPath(t *testing.T) {
	tests := []struct {
		verb   string
		helper string
	}{
		{verb: "migrate diff", helper: "sql"},
		{verb: "schema diff", helper: "sql"},
		{verb: "schema apply", helper: "indent_ln"},
		{verb: "schema apply", helper: "json_merge"},
		{verb: "schema apply", helper: "upper"},
		{verb: "migrate lint", helper: "add"},
		{verb: "migrate lint", helper: "json"},
		{verb: "migrate status", helper: "json_merge"},
		{verb: "migrate status", helper: "redBgWhiteFg"},
		{verb: "schema inspect", helper: "sql"},
		{verb: "schema inspect", helper: "mermaid"},
	}
	for _, test := range tests {
		t.Run(test.verb+" "+test.helper, func(t *testing.T) {
			c := qt.New(t)

			err := templateValidators[test.verb](helperCall(test.helper))

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestTemplateHelpers_FailurePath refuses, on each verb, a helper Ptah has and
// the pinned community binary v1.3.0 does not define there, in that binary's
// words: measured on PostgreSQL 18, each of these templates exits 1 there with
// the same error.
func TestTemplateHelpers_FailurePath(t *testing.T) {
	tests := []struct {
		verb    string
		helper  string
		wantErr string
	}{
		{verb: "migrate diff", helper: "json", wantErr: `parse format: template: format:1: function "json" not defined`},
		{verb: "schema diff", helper: "json", wantErr: `parse log format: template: format:1: function "json" not defined`},
		{verb: "schema apply", helper: "sql", wantErr: `parse log format: template: format:1: function "sql" not defined`},
		{verb: "migrate lint", helper: "indent_ln", wantErr: `parse format: template: format:1: function "indent_ln" not defined`},
		{verb: "migrate lint", helper: "json_merge", wantErr: `parse format: template: format:1: function "json_merge" not defined`},
		{verb: "migrate lint", helper: "upper", wantErr: `parse format: template: format:1: function "upper" not defined`},
		{verb: "migrate status", helper: "add", wantErr: `parse format: template: format:1: function "add" not defined`},
		{verb: "migrate status", helper: "indent_ln", wantErr: `parse format: template: format:1: function "indent_ln" not defined`},
		{verb: "migrate status", helper: "upper", wantErr: `parse format: template: format:1: function "upper" not defined`},
		{verb: "schema inspect", helper: "dbml", wantErr: `parse log format: template: format:1: function "dbml" not defined`},
	}
	for _, test := range tests {
		t.Run(test.verb+" "+test.helper, func(t *testing.T) {
			c := qt.New(t)

			err := templateValidators[test.verb](helperCall(test.helper))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestTemplateExtensions_HappyPath parses every helper the failure path above
// refuses once atlasreport.TemplateExtensionsEnvVar opens Ptah's own surface:
// compatibility withholds no capability, it only stops offering one by default.
func TestTemplateExtensions_HappyPath(t *testing.T) {
	tests := []struct {
		verb   string
		helper string
	}{
		{verb: "migrate diff", helper: "json"},
		{verb: "schema diff", helper: "json"},
		{verb: "schema apply", helper: "sql"},
		{verb: "migrate lint", helper: "indent_ln"},
		{verb: "migrate lint", helper: "json_merge"},
		{verb: "migrate lint", helper: "upper"},
		{verb: "migrate status", helper: "add"},
		{verb: "migrate status", helper: "indent_ln"},
		{verb: "migrate status", helper: "upper"},
		{verb: "schema inspect", helper: "dbml"},
	}
	for _, test := range tests {
		t.Run(test.verb+" "+test.helper, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")

			err := templateValidators[test.verb](helperCall(test.helper))

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestTemplateExtensions_FailurePath refuses a malformed value on every verb
// the variable governs, with a template that would parse either way, so the
// refusal cannot come from the template.
func TestTemplateExtensions_FailurePath(t *testing.T) {
	verbs := []string{"migrate diff", "schema diff", "schema apply", "migrate lint", "migrate status", "schema inspect"}
	for _, verb := range verbs {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasreport.TemplateExtensionsEnvVar, "yes-please")

			err := templateValidators[verb]("ok")

			c.Assert(err, qt.ErrorMatches, `(?s).*PTAH_ATLAS_TEMPLATE_EXTENSIONS.*`)
		})
	}
}

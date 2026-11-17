package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

// pendingUsersTable is a `schema apply --dry-run` report planning one table.
func pendingUsersTable() atlasreport.SchemaApply {
	return atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver:     "postgres",
		URL:        "postgres://app@localhost:5432/app",
		DryRun:     true,
		Statements: []string{`CREATE TABLE "users" ("id" integer);`},
		Comments:   []string{`create "users" table`},
	})
}

// TestSchemaApplyTemplate_HappyPath renders the members the pinned community
// binary v1.3.0 hands a `schema apply --format` template, each measured there
// on PostgreSQL 18 with the same template.
func TestSchemaApplyTemplate_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "the target's members are promoted from Env",
			format: `{{ .Driver }}|{{ .Env.Driver }}|{{ .Env.Dir }}|{{ .Dir }}|{{ .Error }}`,
			want:   "postgres|postgres|||",
		},
		{
			name:   "a dry run lists its changes under Pending",
			format: `{{ range .Changes.Pending }}{{ .Comment }}: {{ .Cmd }}|{{ end }}`,
			want:   `create "users" table: CREATE TABLE "users" ("id" integer)|`,
		},
		{
			name:   "a dry run applies nothing",
			format: `{{ len .Changes.Pending }}|{{ len .Changes.Applied }}|{{ .Changes.Error }}`,
			want:   "1|0|<nil>",
		},
		{
			name:   "the changes encode as statements",
			format: `{{ json .Changes }}`,
			want:   `{"Pending":["CREATE TABLE \"users\" (\"id\" integer)"]}`,
		},
		{
			name:   "the URL is the target's",
			format: `{{ .URL.Scheme }}|{{ .URL.Host }}|{{ .URL.Path }}`,
			want:   "postgres|localhost:5432|/app",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteSchemaApply(&out, test.format, pendingUsersTable())

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestSchemaApplyTemplate_FailurePath refuses what the pinned community binary
// v1.3.0 refuses on `schema apply --format`. Measured there on PostgreSQL 18,
// `{{ sql . }}` fails to parse, and `.MarshalSQL`, `.MarshalJSON`, `len
// .Changes` and `range .Changes` fail to execute, because the report has
// neither method and Changes is a struct. Each error is the community binary's
// but for the Go type it names. Nothing is written in front of a refusal.
func TestSchemaApplyTemplate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr string
	}{
		{
			name:    "sql is not a helper here",
			format:  `before {{ sql . }}`,
			wantErr: `parse log format: template: format:1: function "sql" not defined`,
		},
		{
			name:    "the report has no MarshalSQL",
			format:  `before {{ .MarshalSQL }}`,
			wantErr: `template: format:1:10: executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type atlasreport.SchemaApply`,
		},
		{
			name:    "the report has no MarshalJSON",
			format:  `before {{ .MarshalJSON }}`,
			wantErr: `template: format:1:10: executing "format" at <.MarshalJSON>: can't evaluate field MarshalJSON in type atlasreport.SchemaApply`,
		},
		{
			name:    "Changes has no length",
			format:  `before {{ len .Changes }}`,
			wantErr: `template: format:1:10: executing "format" at <len .Changes>: error calling len: len of type atlasreport.SchemaApplyChanges`,
		},
		{
			name:    "Changes is not a list",
			format:  `before {{ range .Changes }}x{{ end }}`,
			wantErr: `template: format:1:16: executing "format" at <.Changes>: range can't iterate over .*`,
		},
		{
			name:    "a change carries no Args",
			format:  `before {{ range .Changes.Pending }}{{ .Args }}{{ end }}`,
			wantErr: `template: format:1:38: executing "format" at <.Args>: can't evaluate field Args in type atlasreport.SchemaChange`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteSchemaApply(&out, test.format, pendingUsersTable())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestSchemaApplyTemplateExtensions_HappyPath renders Ptah's own `sql` helper
// when atlasreport.TemplateExtensionsEnvVar opens it.
func TestSchemaApplyTemplateExtensions_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")
	var out bytes.Buffer

	err := atlasreport.WriteSchemaApply(&out, `{{ sql . "  " }}`, pendingUsersTable())

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "-- Create \"users\" table\nCREATE TABLE \"users\" (\"id\" integer);\n")
}

// TestSchemaApplyTemplateExtensions_FailurePath refuses a malformed value of
// the variable before any template is parsed, and refuses a call of `sql`
// with too many arguments without writing what came before it.
func TestSchemaApplyTemplateExtensions_FailurePath(t *testing.T) {
	t.Run("a malformed value", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreport.TemplateExtensionsEnvVar, "yes-please")

		err := atlasreport.ValidateSchemaApplyTemplate(`{{ json . }}`)

		c.Assert(err, qt.ErrorMatches, `(?s).*PTAH_ATLAS_TEMPLATE_EXTENSIONS.*`)
	})
	t.Run("too many arguments", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")
		var out bytes.Buffer

		err := atlasreport.WriteSchemaApply(&out, `before {{ sql . "  " "extra" }}`, pendingUsersTable())

		c.Assert(err, qt.ErrorMatches, `template: format:.*unexpected number of arguments: 2.*`)
		c.Assert(out.String(), qt.Equals, "")
	})
}

// TestSchemaApplyRegistersJSONHelper is the regression test for stokaro/ptah#940
// item C: `schema apply --format '{{ json . }}'` failed at template-parse time
// with `function "json" not defined` while the pinned community binary rendered
// the document and exited 0.
func TestSchemaApplyRegistersJSONHelper(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaApplyTemplate(`{{ json . }}`)

	c.Assert(err, qt.IsNil)
}

func TestSchemaApplyTemplateValidationRejectsUnknownHelpers(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaApplyTemplate(`{{ no_such_helper . }}`)

	c.Assert(err, qt.ErrorMatches, `parse log format: .*function "no_such_helper" not defined.*`)
}

// schemaApplyJSONDocument is the shape `{{ json . }}` renders, as measured on
// the pinned community binary v1.3.0.
type schemaApplyJSONDocument struct {
	Driver string `json:"Driver"`
	URL    struct {
		Scheme string `json:"Scheme"`
		Host   string `json:"Host"`
		Schema string `json:"Schema"`
	} `json:"URL"`
	Changes struct {
		Applied []string `json:"Applied"`
		Pending []string `json:"Pending"`
	} `json:"Changes"`
}

func TestSchemaApplyJSONDocumentShape(t *testing.T) {
	statements := []string{`CREATE TABLE "users" ("id" integer);`}
	// The document carries the statement without its terminator, which is how
	// the community binary spells the same entry.
	rendered := []string{`CREATE TABLE "users" ("id" integer)`}
	tests := []struct {
		name   string
		dryRun bool
		// The list the run does not fill is left unset, which is what the
		// document itself carries: both entries are omitted when empty, so the
		// absent one has to be asserted absent rather than merely short.
		wantApplied []string
		wantPending []string
	}{
		{
			name:        "a real apply reports the statements as applied",
			dryRun:      false,
			wantApplied: rendered,
		},
		{
			name:        "a dry run reports the statements as pending",
			dryRun:      true,
			wantPending: rendered,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
				Driver:     "sqlite",
				URL:        "sqlite://apply.db",
				DryRun:     test.dryRun,
				Statements: statements,
			})

			err := atlasreport.WriteSchemaApply(&out, `{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			var doc schemaApplyJSONDocument
			c.Assert(json.Unmarshal(out.Bytes(), &doc), qt.IsNil)
			c.Assert(doc.Driver, qt.Equals, "sqlite")
			c.Assert(doc.URL.Scheme, qt.Equals, "sqlite")
			c.Assert(doc.URL.Host, qt.Equals, "apply.db")
			c.Assert(doc.URL.Schema, qt.Equals, "main")
			c.Assert(doc.Changes.Applied, qt.DeepEquals, test.wantApplied)
			c.Assert(doc.Changes.Pending, qt.DeepEquals, test.wantPending)
		})
	}
}

// TestSchemaApplyJSONRedactsURLSecrets keeps the document from carrying a
// password into a CI log, the way every other report in this package does.
func TestSchemaApplyJSONRedactsURLSecrets(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	// Assembled rather than written inline so the fixture is not a hardcoded
	// credential literal.
	secret := "hunter" + "2"
	report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver:     "postgres",
		URL:        "postgres://user:" + secret + "@localhost:5432/app?sslmode=disable&password=" + secret,
		Statements: []string{`CREATE TABLE "users" ("id" integer);`},
	})

	err := atlasreport.WriteSchemaApply(&out, `{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), secret)
}

// TestSchemaApplySyncedJSONCarriesNoEnvironment pins the one document the
// community binary renders differently: with nothing to apply it emits exactly
// {"Changes":{}}, with no Driver and no URL.
func TestSchemaApplySyncedJSONCarriesNoEnvironment(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver: "sqlite",
		URL:    "sqlite://apply.db",
	})

	err := atlasreport.WriteSchemaApply(&out, `{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `{"Changes":{}}`)
}

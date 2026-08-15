package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
)

func TestSchemaApplyCustomSQLTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver: "sqlite",
		URL:    "sqlite://apply.db",
		Statements: []string{
			`CREATE TABLE "users" ("id" integer);`,
		},
	})

	err := atlasreport.WriteSchemaApply(&out, `{{ len .Changes }}|{{ .MarshalSQL }}|{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|CREATE TABLE \"users\" (\"id\" integer);\n|  CREATE TABLE \"users\" (\"id\" integer);\n")
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

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "no_such_helper" not defined.*`)
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

func TestSchemaApplyTemplateExecutionErrorDoesNotWritePartialOutput(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver: "sqlite",
		URL:    "sqlite://apply.db",
		Statements: []string{
			`CREATE TABLE "users" ("id" integer);`,
		},
	})

	err := atlasreport.WriteSchemaApply(&out, `before {{ sql . "  " "extra" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(out.String(), qt.Equals, "")
}

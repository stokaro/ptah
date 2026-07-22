package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
)

func TestSchemaDiffDefaultFormatReportsSyncedSchemas(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaDiff(nil, nil, nil)

	err := atlasreport.WriteSchemaDiff(&out, atlasreport.NormalizeSchemaDiffFormat(""), report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Schemas are synced, no changes to be made.\n")
}

func TestSchemaDiffCustomSQLTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaDiff(&out, `{{ len .Changes }}|{{ .MarshalSQL }}|{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|CREATE TABLE \"users\" (\"id\" integer);\n|  CREATE TABLE \"users\" (\"id\" integer);\n")
}

func TestSchemaDiffTemplateValidationRejectsUnknownHelpers(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaDiffTemplate(`{{ json . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "json" not defined.*`)
}

func TestSchemaDiffTemplateExecutionErrorDoesNotWritePartialOutput(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaDiff(&out, `before {{ sql . "  " "extra" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(out.String(), qt.Equals, "")
}

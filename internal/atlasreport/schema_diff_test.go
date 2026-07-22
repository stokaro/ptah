package atlasreport

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSchemaDiffDefaultFormatReportsSyncedSchemas(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := NewSchemaDiff(nil, nil, nil)

	err := WriteSchemaDiff(&out, NormalizeSchemaDiffFormat(""), report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Schemas are synced, no changes to be made.\n")
}

func TestSchemaDiffCustomSQLTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := WriteSchemaDiff(&out, `{{ len .Changes }}|{{ .MarshalSQL }}|{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|CREATE TABLE \"users\" (\"id\" integer);\n|  CREATE TABLE \"users\" (\"id\" integer);\n")
}

func TestSchemaDiffTemplateValidationRejectsUnknownHelpers(t *testing.T) {
	c := qt.New(t)

	err := ValidateSchemaDiffTemplate(`{{ json . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "json" not defined.*`)
}

func TestSchemaDiffTemplateExecutionErrorDoesNotWritePartialOutput(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := WriteSchemaDiff(&out, `before {{ sql . "  " "extra" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(out.String(), qt.Equals, "")
}

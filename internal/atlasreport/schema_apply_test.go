package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
)

func TestSchemaApplyCustomSQLTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaApply([]string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaApply(&out, `{{ len .Changes }}|{{ .MarshalSQL }}|{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|CREATE TABLE \"users\" (\"id\" integer);\n|  CREATE TABLE \"users\" (\"id\" integer);\n")
}

func TestSchemaApplyTemplateValidationRejectsUnknownHelpers(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaApplyTemplate(`{{ json . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "json" not defined.*`)
}

func TestSchemaApplyTemplateExecutionErrorDoesNotWritePartialOutput(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	report := atlasreport.NewSchemaApply([]string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaApply(&out, `before {{ sql . "  " "extra" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
	c.Assert(out.String(), qt.Equals, "")
}

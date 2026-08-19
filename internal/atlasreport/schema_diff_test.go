package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
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

func TestNormalizeMigrateDiffFormat_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasreport.NormalizeMigrateDiffFormat(""), qt.Equals, `{{ sql . "  " }}`)
	c.Assert(atlasreport.NormalizeMigrateDiffFormat(`{{ sql . "" }}`), qt.Equals, `{{ sql . "" }}`)
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

// TestSchemaDiffTemplateHelpers_OpenTheSharedSet covers stokaro/ptah#1705.
//
// `schema diff` registers `sql` alone, which keeps ptah-compat from rendering a
// template the pinned community binary refuses. The other half of that policy
// is that compatibility must not withhold a capability Ptah has, and Ptah has
// the document -- `schema apply` and `schema inspect` already render it. So the
// fuller set lives behind a variable rather than behind a new flag, and the
// command and flag inventory is unchanged either way.
//
// TestSchemaDiffTemplateValidationRejectsUnknownHelpers above is the other half
// of this pair: with the variable absent, the same template is still refused.
func TestSchemaDiffTemplateHelpers_OpenTheSharedSet(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.SchemaDiffTemplateHelpersEnvVar, "1")
	var out bytes.Buffer
	report := atlasreport.NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaDiff(&out, `{{ json .Changes }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `[{"Cmd":"CREATE TABLE \"users\" (\"id\" integer)"}]`)
}

// TestSchemaDiffTemplateHelpers_KeepSQL is the non-interference control: the
// helper this verb has always had must survive the wider registration. The
// shared set carries no `sql`, so a registration that replaced the map instead
// of adding to it would break every existing template while making the new one
// work.
func TestSchemaDiffTemplateHelpers_KeepSQL(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.SchemaDiffTemplateHelpersEnvVar, "1")
	var out bytes.Buffer
	report := atlasreport.NewSchemaDiff(nil, nil, []string{
		`CREATE TABLE "users" ("id" integer);`,
	})

	err := atlasreport.WriteSchemaDiff(&out, `{{ sql . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "CREATE TABLE \"users\" (\"id\" integer);\n")
}

// TestSchemaDiffTemplateHelpers_RefuseAMalformedValue pins where a bad spelling
// is answered: by the command that would have used the variable, naming the
// template it did not parse, rather than lying dormant until a later run.
func TestSchemaDiffTemplateHelpers_RefuseAMalformedValue(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.SchemaDiffTemplateHelpersEnvVar, "yes-please")

	err := atlasreport.ValidateSchemaDiffTemplate(`{{ sql . }}`)

	c.Assert(err, qt.ErrorMatches, `(?s).*PTAH_SCHEMA_DIFF_TEMPLATE_HELPERS.*`)
}

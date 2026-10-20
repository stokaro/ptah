package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/migration/schemadiff"
)

// languageProcedure is a procedure declared in HCL, which has no attribute for
// a language: the body is in the language of the engine the schema targets.
const languageProcedure = `
schema "app" {}
procedure "touch" {
  schema = schema.app
  as     = "BEGIN UPDATE t SET c = 1; END"
}
`

// TestCompareWithDialect_AnUndeclaredLanguageIsTheEnginesOwn pins that a
// routine whose declaration names no language takes the language of the
// engine it targets.
//
// The model filled an omitted language in as PostgreSQL's plpgsql on every
// dialect. A MySQL, MariaDB or SQL Server server reports its routines in SQL,
// and an Oracle server in PL/SQL, so the same procedure compared as
// `language: sql -> plpgsql` on every run, and a plan replaced a routine
// nobody had changed. On PostgreSQL the omitted
// language is plpgsql, and the procedure compares equal to the one the server
// reports.
func TestCompareWithDialect_AnUndeclaredLanguageIsTheEnginesOwn(t *testing.T) {
	for _, test := range []struct {
		dialect  string
		language string
	}{
		{dialect: "mysql", language: "sql"},
		{dialect: "mariadb", language: "sql"},
		{dialect: "sqlserver", language: "sql"},
		{dialect: "oracle", language: "plsql"},
		{dialect: "postgres", language: "plpgsql"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))
			current := &catalog.Database{Functions: []catalog.Function{{
				Name: "touch", Schema: "app", Kind: "procedure", Language: test.language,
				Security: desired.Functions[0].Security, Volatility: desired.Functions[0].Volatility,
				Body: "BEGIN UPDATE t SET c = 1; END",
			}}}

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_ADocumentStandingForADatabaseLeavesTheLanguageOut
// covers the other side of the fold. A schema document converted to stand for
// the database, as `schema diff` between two files does, can leave a
// routine's language out as well, and it is the target's there too.
func TestCompareWithDialect_ADocumentStandingForADatabaseLeavesTheLanguageOut(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))
	desired.Functions[0].Language = "plpgsql"
	current := &catalog.Database{Functions: []catalog.Function{{
		Name: "touch", Schema: "app", Kind: "procedure",
		Security: desired.Functions[0].Security, Volatility: desired.Functions[0].Volatility,
		Body: "BEGIN UPDATE t SET c = 1; END",
	}}}

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
}

// TestCompareWithDialect_TheDiffKeepsTheLanguageAsWritten pins that the fold
// is for the comparison alone. A routine that changed carries its declaration
// as written, with no language, so the planner renders what the author wrote
// (stokaro/ptah#2315).
func TestCompareWithDialect_TheDiffKeepsTheLanguageAsWritten(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))
	current := &catalog.Database{Functions: []catalog.Function{{
		Name: "touch", Schema: "app", Kind: "procedure", Language: "sql",
		Security: desired.Functions[0].Security, Volatility: desired.Functions[0].Volatility,
		Body: "BEGIN UPDATE t SET c = 2; END",
	}}}

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "mysql", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Changes, qt.DeepEquals, map[string]string{
		"body": "BEGIN UPDATE t SET c = 2; END -> BEGIN UPDATE t SET c = 1; END",
	})
	c.Assert(diff.FunctionsModified[0].Desired.Language, qt.Equals, "")
}

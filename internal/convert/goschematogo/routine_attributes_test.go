package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// attributedFunction declares every routine attribute an export has to keep.
func attributedFunction() schemamodel.Function {
	return schemamodel.Function{
		StructName: "Fns", Name: "same_tenant", Parameters: "t uuid", Returns: "boolean",
		Language: "sql", Volatility: "STABLE", Security: "DEFINER",
		Leakproof: true, Parallel: "SAFE", Strict: true,
		Settings: []string{"search_path=pg_catalog"},
		Body:     "SELECT t = current_setting('app.tenant')::uuid",
	}
}

// The planner attributes survive an export to Go annotations and back
// (stokaro/ptah#3630). They were written by nothing, so a function read back
// from the export was neither LEAKPROOF nor STRICT, and ran in no parallel
// plan.
func TestRender_RoutineAttributesRoundTrip(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{attributedFunction()}}

	files, err := goschematogo.Render(database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	parsed, err := goschema.ParseSource("schema.go", source.String())

	c.Assert(err, qt.IsNil, qt.Commentf("the exported source does not parse:\n%s", source.String()))
	c.Assert(parsed.Functions, qt.HasLen, 1)
	function := parsed.Functions[0]
	c.Assert([]any{function.Leakproof, function.Parallel, function.Strict}, qt.DeepEquals, []any{true, "SAFE", true})
	c.Assert(function.Settings, qt.DeepEquals, []string{"search_path=pg_catalog"})
}

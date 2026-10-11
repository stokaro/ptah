package hcldiag_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"

	"ptah.run/internal/hcldiag"
)

// evaluate evaluates expression against a context holding the named
// functions, and returns the diagnostics Stable makes of the result.
func evaluate(c *qt.C, expression string, functions ...string) hcl.Diagnostics {
	c.Helper()
	parsed, diags := hclsyntax.ParseExpression([]byte(expression), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	c.Assert(diags.HasErrors(), qt.IsFalse)
	table := make(map[string]function.Function, len(functions))
	for _, name := range functions {
		table[name] = stdlib.UpperFunc
	}
	_, diags = parsed.Value(&hcl.EvalContext{Functions: table})
	return hcldiag.Stable(diags)
}

// TestStable_NamesOneSuggestionOnEveryRun evaluates the same unknown call many
// times, against a table where two functions are close enough for the
// library to suggest either, and gets the nearest every time. The library's
// own walk of the map changed its answer between runs.
func TestStable_NamesOneSuggestionOnEveryRun(t *testing.T) {
	c := qt.New(t)
	for range 200 {
		diags := evaluate(c, "now()", "log", "pow", "max", "min", "abs", "lower", "upper", "trim")

		c.Assert(diags, qt.HasLen, 1)
		c.Assert(diags[0].Summary, qt.Equals, "Call to unknown function")
		c.Assert(diags[0].Detail, qt.Equals, `There is no function named "now". Did you mean "pow"?`)
	}
}

// TestStable_Suggestion holds the choice to the library's threshold and
// wording: the nearest function, the first in name order among equally near
// ones, and none beyond two edits.
func TestStable_Suggestion(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		functions  []string
		want       string
	}{
		{name: "the nearest", expression: "lowr()", functions: []string{"low", "lower", "flower"},
			want: `There is no function named "lowr". Did you mean "low"?`},
		{name: "the first of equally near ones", expression: "nod()", functions: []string{"nox", "nob", "noc"},
			want: `There is no function named "nod". Did you mean "nob"?`},
		{name: "none close enough", expression: "frobnicate()", functions: []string{"lower", "upper"},
			want: `There is no function named "frobnicate".`},
		{name: "a namespaced call", expression: "core::uper()", functions: []string{"core::upper", "core::lower", "other::upper"},
			want: `There is no function named "uper" in namespace core::.`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diags := evaluate(c, test.expression, test.functions...)

			c.Assert(diags, qt.HasLen, 1)
			c.Assert(diags[0].Detail, qt.Equals, test.want)
		})
	}
}

// TestStable_LeavesOtherDiagnosticsAlone returns a diagnostic that is not a
// call to an unknown function as the library wrote it, and nothing for nothing.
func TestStable_LeavesOtherDiagnosticsAlone(t *testing.T) {
	c := qt.New(t)
	other := &hcl.Diagnostic{Severity: hcl.DiagError, Summary: "Unknown variable", Detail: "There is no variable named \"x\"."}

	stable := hcldiag.Stable(hcl.Diagnostics{other})

	c.Assert(stable, qt.DeepEquals, hcl.Diagnostics{other})
	c.Assert(hcldiag.Stable(nil), qt.HasLen, 0)
}

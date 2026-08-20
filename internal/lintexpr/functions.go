package lintexpr

import (
	"strings"

	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// ruleFunctions is the shared project set plus the string tests a rule needs.
//
// # Why an overlay rather than an addition to the shared set
//
// The shared set is what `atlas.hcl` and schema files are evaluated against,
// and its contents are a compatibility surface. Adding a name there would claim
// that an expression using it evaluates in a project file, which is a claim
// about a contract this repository does not own. So the name is added HERE,
// where the scope is already Ptah's own (`statement`, `file`, `dialect`) and no
// such claim is implied.
//
// The overlay direction is [atlashcl.WithProjectBoundFunctions]: the bound side
// wins, so this can never silently shadow a shared name with different
// semantics -- and it deliberately shadows none.
func ruleFunctions() map[string]function.Function {
	return atlashcl.WithProjectBoundFunctions(
		atlashcl.ProjectFunctions(),
		map[string]function.Function{"strcontains": strContainsFunc},
	)
}

// strContainsFunc is substring containment.
//
// `contains` in the shared set tests LIST membership, which is right for
// `statement.words` and wrong for `statement.sql`. Without a separate name the
// most ordinary rule anybody writes -- does this statement mention X -- would
// have to be spelled `length(regexall("x", statement.sql)) > 0`, and the
// obvious spelling would fail once per statement at evaluation time.
//
// The name is the one HCL's own ecosystem uses for this, so a rule author who
// knows `contains` from elsewhere finds the string form where they expect it.
var strContainsFunc = function.New(&function.Spec{
	Params: []function.Parameter{
		{Name: "haystack", Type: cty.String},
		{Name: "needle", Type: cty.String},
	},
	Type: function.StaticReturnType(cty.Bool),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		return cty.BoolVal(strings.Contains(args[0].AsString(), args[1].AsString())), nil
	},
})

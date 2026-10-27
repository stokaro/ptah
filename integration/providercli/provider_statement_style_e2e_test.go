//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/planner"
)

// TestProviderStatementStyle_ExecutableOwnerHonorsExactAndAnnotated sends the
// common planner's style through an independently built provider process. The
// fixture owns the operation and chooses its guard before rendering it.
func TestProviderStatementStyle_ExecutableOwnerHonorsExactAndAnnotated(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=statement-style"})
	request := providerPlanningRequest(c, runtime)
	request.Changes[0].Value = planningChange(c, runtime, "planned", `{"action":"drop"}`,
		provider.PlanningPolicyFeature, provider.PlanningStatementStyleFeature)
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &request.Identifiers, FeatureChanges: request.Changes}

	for _, test := range []struct {
		name  string
		style planext.StatementStyle
		want  string
	}{
		{name: "annotated", style: planext.StatementStyleAnnotated, want: `DROP TABLE IF EXISTS "planned";`},
		{name: "exact", style: planext.StatementStyleExact, want: `DROP TABLE "planned";`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{
				Capabilities: request.Capabilities, Statements: test.style,
			})
			c.Assert(err, qt.IsNil)
			rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{test.want})
		})
	}
}

func TestProviderStatementStyle_ExactRefusesOlderExecutable(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	request.Changes[0].Value = planningChange(c, runtime, "planned", `{"action":"drop"}`)
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &request.Identifiers, FeatureChanges: request.Changes}

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{
		Capabilities: request.Capabilities, Statements: planext.StatementStyleExact,
	})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(nodes, qt.IsNil)
}

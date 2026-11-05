package providerhost

// White-box testing required: the order of the host's common steps is the
// order of its private placeholders in the planning request, which a provider
// claims in place, and nothing outside the adapter sees the placeholders. The
// provider live tests cover what an engine receives.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// stepClaimer claims every common step that has a footprint, each with an
// operation named for the action of the step's last effect, so the nodes a
// plan returns spell the order of the host's steps.
type stepClaimer struct{ *engine.Runtime }

func (stepClaimer) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/fixture"}
	for _, common := range request.CommonSteps {
		if len(common.Effects) == 0 {
			continue
		}
		last := common.Effects[len(common.Effects)-1]
		id := plangraph.StepID{Owner: contribution.Owner, Name: common.ID.Name}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: string(last.Action) + " " + last.Subject.Name.Source}},
			Effects:     common.Effects,
			Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "Claims a step"}})
		result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common.ID}, Replacement: id})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// A plan that creates a table and drops an index of a table it keeps creates
// the table first where index names are scoped to their table, as the YDB
// engine built into Ptah at 74de0b65b planned it, and drops the index first
// where a schema holds the names, so the new table may take the dropped
// index's name.
func TestOwnedLifecycleIndexDropFollowsTheTableCreations_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name      string
		namespace identifier.IndexNamespace
		want      []string
	}{
		{"index names scoped to a table", identifier.IndexNamespaceTable, []string{"create added", "drop records_old"}},
		{"index names shared by a schema", identifier.IndexNamespaceSchema, []string{"drop records_old", "create added"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			diff := preparedRecords()
			rules := *diff.IdentifierSemantics
			rules.IndexNamespace = row.namespace
			diff.IdentifierSemantics = &rules
			diff.TablesAdded = difftypes.TableChanges{{Name: "main.added", Table: schemamodel.Table{Name: "added", Schema: "main"}}}
			diff.IndexesRemoved = []difftypes.IndexRef{{Name: "records_old", TableName: "main.records"}}

			nodes, err := indexPlanner().GenerateMigrationAST(c.Context(), stepClaimer{Runtime: base}, diff)

			c.Assert(err, qt.IsNil)
			var order []string
			for _, node := range nodes {
				statement, isStatement := node.(*ast.ExtensionStatement)
				c.Assert(isStatement, qt.IsTrue)
				operation, isOperation := statement.Payload.(*creationOperation)
				c.Assert(isOperation, qt.IsTrue)
				order = append(order, operation.name)
			}
			c.Assert(order, qt.DeepEquals, row.want)
		})
	}
}

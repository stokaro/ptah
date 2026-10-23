//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/migration/planner"
)

func ownedDropDiff() *difftypes.SchemaDiff {
	rules := comparisonRules()
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesRemoved: difftypes.TableRemovals{{
		Name:    "main.records",
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}},
	}}}
}

func TestProviderOwnedLifecyclePlansTableRemoval_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned"})
	options := planext.Options{Capabilities: unwrappedOperationTarget()}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, ownedDropDiff(), "fixture", options)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(ast.OwnerTransactions(nodes[0]), qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: options.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`DROP TABLE "records";`})
}

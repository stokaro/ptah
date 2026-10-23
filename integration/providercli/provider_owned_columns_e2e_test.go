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
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

func ownedColumnsDiff(added, removed []schemamodel.Field) *difftypes.SchemaDiff {
	rules := comparisonRules()
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesModified: []difftypes.TableDiff{{
		TableName:      "main.records",
		Desired:        schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "records", Schema: "main"}, Fields: added},
		Current:        schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}},
		ColumnsAdded:   added,
		ColumnsRemoved: removed,
	}}}
}

func TestProviderOwnedLifecyclePlansColumnChanges_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name           string
		added, removed []schemamodel.Field
		want           []string
	}{
		{"add column", []schemamodel.Field{{Name: "added", Type: "TEXT", Nullable: true}}, nil,
			[]string{`ALTER TABLE "records" ADD COLUMN "added" TEXT;`}},
		{"drop column", nil, []schemamodel.Field{{Name: "gone", Type: "TEXT", Nullable: true}},
			[]string{`ALTER TABLE "records" DROP COLUMN "gone";`}},
		{"add and drop", []schemamodel.Field{{Name: "added", Type: "INTEGER"}}, []schemamodel.Field{{Name: "gone", Type: "TEXT", Nullable: true}},
			[]string{`ALTER TABLE "records" ADD COLUMN "added" INTEGER NOT NULL;`, `ALTER TABLE "records" DROP COLUMN "gone";`}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned"})
			options := planext.Options{Capabilities: unwrappedOperationTarget()}
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, ownedColumnsDiff(row.added, row.removed), "fixture", options)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, len(row.want))
			for _, node := range nodes {
				c.Assert(ast.OwnerTransactions(node), qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
			}
			rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: options.Capabilities, Nodes: nodes})
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, row.want)
		})
	}
}

func TestProviderOwnedLifecycleRefusesColumnItDoesNotClaim_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned"})
	added := []schemamodel.Field{{Name: "added", Type: "TEXT", Nullable: true, Comment: "the fixture claims plain columns only"}}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, ownedColumnsDiff(added, nil), "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}

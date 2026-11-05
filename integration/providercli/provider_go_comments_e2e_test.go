//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/goschema"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/migration/planner"
)

// commentedGoSource declares a table and its key column, each with a comment
// written the common way.
const commentedGoSource = `package p

//ptah:schema:table name="commented" comment="People who sign in"
type Commented struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" comment="Key"
	ID int
}
`

// A comment written in a Go source is the comment of its table or column and
// no platform property. The fixture owns the table lifecycle and refuses the
// table "commented" unless its declaration carries both comments, so each
// rendered statement shows the comments reached it.
func TestProviderReceivesTheCommentsOfAGoSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=declarations"})
	declared, err := goschema.ParseSource(runtime.Annotations(), "commented.go", commentedGoSource)
	c.Assert(err, qt.IsNil)
	want := []string{`CREATE TABLE "commented" ("id" INTEGER NOT NULL PRIMARY KEY);`}

	t.Run("schema render", func(t *testing.T) {
		c := qt.New(t)
		rendered, err := runtime.RenderSchema(c.Context(), renderer.SchemaRequest{Target: "fixture", Capabilities: unwrappedOperationTarget(),
			Identifiers: identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
				IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable},
			Schema: &declared})
		c.Assert(err, qt.IsNil)
		c.Assert(rendered.Statements, qt.DeepEquals, want)
	})
	t.Run("schema apply", func(t *testing.T) {
		c := qt.New(t)
		// The fixture prepares no tables, so the creation is the one a
		// comparison against an empty database records.
		rules := comparisonRules()
		diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
			TablesAdded: difftypes.TableChanges{{Name: "main.commented", Table: declared.Tables[0], Fields: declared.Fields}}}
		options := planext.Options{Capabilities: unwrappedOperationTarget()}
		nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", options)
		c.Assert(err, qt.IsNil)
		rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: options.Capabilities, Nodes: nodes})
		c.Assert(err, qt.IsNil)
		c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, want)
	})
}

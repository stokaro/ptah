//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/planner"
)

// ownedCommentsCreation creates main.users with a comment on the table, on a
// column and on one of its two indexes.
func ownedCommentsCreation() *difftypes.SchemaDiff {
	rules := comparisonRules()
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded: difftypes.TableChanges{{Name: "main.users", Table: schemamodel.Table{Name: "users", Schema: "main", Comment: "People who sign in"},
			Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", Comment: "Key"}, {Name: "label", Type: "TEXT", Nullable: true}}}},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "main.users", Index: schemamodel.Index{Name: "users_by_label", Fields: []string{"label"}, Comment: "Lookup by name's"}},
			{TableName: "main.users", Index: schemamodel.Index{Name: "users_by_id", Fields: []string{"label", "id"}}},
		},
	}
}

// ownedCommentChanges keeps main.users and changes the comments of two of its
// indexes, one of them to none.
func ownedCommentChanges() *difftypes.SchemaDiff {
	rules := comparisonRules()
	subject := objectidentity.NewBuilder(rules).TableParts("main", "users")
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{{Subject: subject,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "users", Schema: "main"}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "users", Schema: "main"}}}}},
		IndexCommentsChanged: []difftypes.IndexCommentChange{
			{TableName: "main.users", Name: "users_by_id", Current: "by id"},
			{TableName: "main.users", Name: "users_by_label", Current: "before", Desired: "after"},
		},
	}
}

// renderedPlan plans diff through the fixture in mode and renders the plan.
func renderedPlan(c *qt.C, mode string, diff *difftypes.SchemaDiff) ([]string, error) {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=" + mode})
	options := planext.Options{Capabilities: unwrappedOperationTarget()}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", options)
	if err != nil {
		return nil, err
	}
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: options.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	return must.Must(rendered.SQLFragments()), nil
}

// A provider that owns the table lifecycle and carries comments on the
// common-object wire receives the comments of a table the plan creates, of
// its columns and of its indexes as steps after its own creation, and
// renders each.
func TestProviderOwnedLifecycleSetsCreatedComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned-comments", ownedCommentsCreation())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY);`,
		`COMMENT ON TABLE "users" IS 'People who sign in';`,
		`COMMENT ON COLUMN "users"."id" IS 'Key';`,
		`COMMENT ON INDEX "users"."users_by_label" IS 'Lookup by name''s';`,
	})
}

// The comments of a kept table's indexes change with a step each, one of them
// to none.
func TestProviderOwnedLifecycleChangesIndexComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned-comments", ownedCommentChanges())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`COMMENT ON INDEX "users"."users_by_id" IS NULL;`,
		`COMMENT ON INDEX "users"."users_by_label" IS 'after';`,
	})
}

// A provider that owns the table lifecycle without the comment wire gets no
// comment step: it receives the comments in the declaration.
func TestProviderOwnedLifecycleWithoutTheCommentWire_HappyPath(t *testing.T) {
	c := qt.New(t)
	creation := ownedCommentsCreation()
	creation.IndexesAdded = nil
	statements, err := renderedPlan(c, "declarations", creation)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY);`})
}

// Without the comment wire, an index comment change has no step to be made
// with and stays refused.
func TestProviderOwnedLifecycleWithoutTheCommentWire_FailurePath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned", ownedCommentChanges())
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexCommentsChanged`)
	c.Assert(statements, qt.IsNil)
}

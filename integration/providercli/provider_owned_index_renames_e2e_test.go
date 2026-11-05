//go:build integration

package providercli_test

import (
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dbschema"
	"ptah.run/migration/generator"
)

// renamedIndexDiff keeps main.users, whose index users_by_label the
// declaration names users_by_name with one definition, and moves the index's
// comment to the new name, as the comparison records a pair.
func renamedIndexDiff() *difftypes.SchemaDiff {
	rules := comparisonRules()
	subject := objectidentity.NewBuilder(rules).TableParts("main", "users")
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{{Subject: subject,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "users", Schema: "main"},
				Fields:  []schemamodel.Field{{Name: "id", Type: "INTEGER", Primary: true}, {Name: "label", Type: "TEXT", Nullable: true}},
				Indexes: []schemamodel.Index{{Name: "users_by_name", Fields: []string{"label"}, Comment: "Lookup"}}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "users", Schema: "main",
				Columns: []catalog.Column{{Name: "id", DataType: "INTEGER", IsPrimaryKey: true}, {Name: "label", DataType: "TEXT", IsNullable: "YES"}}},
				Indexes: []catalog.Index{{Name: "users_by_label", TableName: "users", Schema: "main", Columns: []string{"label"}, Comment: "Lookup"}}}}}},
		IndexesRenamed: []difftypes.IndexRename{{TableName: "main.users", From: "users_by_label", To: "users_by_name"}},
		IndexCommentsChanged: []difftypes.IndexCommentChange{
			{TableName: "main.users", Name: "users_by_name", From: "users_by_label", Current: "Lookup", Desired: "Lookup"},
		},
	}
}

// A provider that renames indexes claims the host's rename step with an
// operation that keeps the index, and the index's comment moves to the new
// name after it: cleared under the held name and set under the declared one.
func TestProviderOwnedLifecycleRenamesAnIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned-renames", renamedIndexDiff())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`ALTER TABLE "users" RENAME INDEX "users_by_label" TO "users_by_name";`,
		`COMMENT ON INDEX "users"."users_by_label" IS NULL;`,
		`COMMENT ON INDEX "users"."users_by_name" IS 'Lookup';`,
	})
}

// A provider that owns indexes without planning.index-renames has no step to
// receive a pair with, so a rename stays refused.
func TestProviderOwnedLifecycleRenamesAnIndex_FailurePath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned-comments", renamedIndexDiff())
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexesRenamed`)
	c.Assert(statements, qt.IsNil)
}

// TestProviderRollbackProjectsARenamedIndex_HappyPath plans, through a
// provider that renames indexes, a change to main.users that renames index
// old_name to new_name with a new comment. The rollback's capture of the table
// holds the indexes the forward plan leaves: the renamed index where old_name
// stood, under its new name and comment, then kept. The forward capture and
// the caller's catalog keep their own indexes, and the rollback renames the
// index back.
func TestProviderRollbackProjectsARenamedIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned-renames", "FIXTURE_SCHEMA_VALIDATION=1"})
	rules := comparisonRules()
	table := catalog.Table{Name: "users", Schema: "main",
		Columns: []catalog.Column{{Name: "id", DataType: "INTEGER", IsPrimaryKey: true}, {Name: "label", DataType: "TEXT", IsNullable: "YES"}}}
	held := func() []catalog.Index {
		return []catalog.Index{
			{Name: "old_name", TableName: "users", Schema: "main", Columns: []string{"label"}, Comment: "old comment"},
			{Name: "kept", TableName: "users", Schema: "main", Columns: []string{"id", "label"}},
		}
	}
	declared := schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "users", Schema: "main"},
		Fields:  []schemamodel.Field{{Name: "id", Type: "INTEGER", Primary: true}, {Name: "label", Type: "TEXT", Nullable: true}},
		Indexes: []schemamodel.Index{{Name: "new_name", Fields: []string{"label"}, Comment: "new comment"}, {Name: "kept", Fields: []string{"id", "label"}}}}
	observed := schemacapture.TableObservation{Table: table, Indexes: held()}
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{{
			Subject: objectidentity.NewBuilder(rules).TableParts("main", "users"), Desired: declared, Current: observed}}},
		TablesModified: []difftypes.TableDiff{{TableName: "main.users", Desired: declared, Current: observed}},
		IndexesRenamed: []difftypes.IndexRename{{TableName: "main.users", From: "old_name", To: "new_name"}},
		IndexCommentsChanged: []difftypes.IndexCommentChange{
			{TableName: "main.users", Name: "new_name", From: "old_name", Current: "old comment", Desired: "new comment"},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{table}, Indexes: held()}
	caps := capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.IndexRename: true})

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff,
		DesiredSchema: &schemamodel.Database{}, CurrentSchema: current, Dialect: "fixture", Capabilities: caps})

	c.Assert(err, qt.IsNil)
	indexes := plan.Reverse.Diff.TablesModified[0].Current.Indexes
	c.Assert(indexes, qt.HasLen, 2)
	c.Assert(indexes[0].Name, qt.Equals, "new_name")
	c.Assert(indexes[0].Comment, qt.Equals, "new comment")
	c.Assert(indexes[0].Columns, qt.DeepEquals, []string{"label"})
	c.Assert(indexes[1].Name, qt.Equals, "kept")
	c.Assert(plan.Reverse.Diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "main.users", From: "new_name", To: "old_name"}})
	indexes[0].Columns[0] = "mutated"
	c.Assert(diff.TablesModified[0].Current.Indexes[0].Name, qt.Equals, "old_name")
	c.Assert(diff.TablesModified[0].Current.Indexes[0].Comment, qt.Equals, "old comment")
	c.Assert(diff.TablesModified[0].Current.Indexes[0].Columns, qt.DeepEquals, []string{"label"})
	c.Assert(current.Indexes[0].Columns, qt.DeepEquals, []string{"label"})
}

// renamesConnection opens a fixture database through a provider started in
// mode with environment.
func renamesConnection(c *qt.C, mode string, environment ...string) (*dbschema.DatabaseConnection, error) {
	c.Helper()
	host, _ := yamlProviderEnvironment(c, "operation-models", append([]string{"FIXTURE_RENDER_MODE=" + mode}, environment...))
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "renames.db")), "/")}).String()
	connection, err := dbschema.ConnectToDatabase(ctx, address)
	if err != nil {
		return nil, err
	}
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	return connection, nil
}

// A session of a provider that renames reports index_rename to this host,
// which lists the feature, so the comparison pairs renamed indexes.
func TestProviderSessionReportsIndexRenames_HappyPath(t *testing.T) {
	c := qt.New(t)
	connection, err := renamesConnection(c, "owned-renames")
	c.Assert(err, qt.IsNil)
	c.Assert(connection.Info().Capabilities.Has(capability.IndexRename), qt.IsTrue)
}

// A session that reports index_rename from an engine that does not
// negotiate planning.index-renames is refused when it opens: the comparison
// would pair renamed indexes the planner has no step for.
func TestProviderSessionReportsIndexRenames_FailurePath(t *testing.T) {
	c := qt.New(t)
	connection, err := renamesConnection(c, "owned-comments", "FIXTURE_INDEX_RENAME=always")
	c.Assert(err, qt.ErrorMatches, `.*provider session capabilities: index_rename needs planning\.index-renames, which engine fixture does not negotiate`)
	c.Assert(connection, qt.IsNil)
}

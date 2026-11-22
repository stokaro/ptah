package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
)

// tableNamed is the creation of the table name with one UInt64 column.
func tableNamed(name string) difftypes.TableCreation {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: name, StructName: "V"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "V", Type: "UInt64", Primary: true}},
	}
	return difftypes.TableCreationFor(desired, desired.Tables[0], name, identifier.ForDialect("clickhouse"))
}

// viewPolicies is what a read of the database d1 records about the row
// policy leak on the relation v that it does not describe as a table, as it
// records one on a view or a materialized view.
func viewPolicies() difftypes.FeatureContext {
	return difftypes.FeatureContext{CurrentCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", "v", "leak"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: `the policy is on "v" (engine View)`}}}))}
}

// selectOnV is the grant of SELECT on the relation v to the role reader, as
// the reader records one.
func selectOnV() difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "d1.v"}
}

// tableRemoved is the removal of the table name, as the catalog captured it.
func tableRemoved(name string) difftypes.TableRemoval {
	return difftypes.TableRemoval{Name: name, Current: capturedColumnFixture(schemamodel.Table{Name: name}).Current}
}

// planHeads plans diff and returns the first line of each statement, leaving
// out the comments the renderer writes above one.
func planHeads(c *qt.C, diff *difftypes.SchemaDiff) []string {
	c.Helper()
	nodes, err := clickhouse.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("clickhouse", nodes...)
	c.Assert(err, qt.IsNil)
	var heads []string
	for _, statement := range sqlutil.SplitStatementsForDialect("clickhouse", sql) {
		heads = append(heads, strings.SplitN(strings.TrimSpace(statement), "\n", 2)[0])
	}
	return heads
}

// TestGenerateMigrationAST_HandsANameToAnotherRelationKind drops a relation
// the plan removes ahead of the creation that takes its name for another
// kind. ClickHouse keeps one namespace for tables and views: measured on 24.10
// and 26.10, a creation while the old relation holds the name answers "Code:
// 57. DB::Exception: Table d1.v already exists. (TABLE_ALREADY_EXISTS)" in
// every direction. The removal carries the catalog's qualified spelling and
// the addition the declaration's bare one, as a comparison produces them.
// Each relation is named once: the drop moves, and no second one is left
// where it was. A view or materialized view a table takes the name of drops
// its row policies first: ClickHouse keeps a policy bound to the name after
// DROP VIEW and applies it to the new table at once.
func TestGenerateMigrationAST_HandsANameToAnotherRelationKind(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a view to a table",
			diff: &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "d1.v"}}, TablesAdded: difftypes.TableChanges{tableNamed("v")}},
			want: []string{"DROP VIEW IF EXISTS `d1`.`v`", "CREATE TABLE v ("}},
		{name: "a materialized view to a table",
			diff: &difftypes.SchemaDiff{MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "d1.v"}}, TablesAdded: difftypes.TableChanges{tableNamed("v")}},
			want: []string{"DROP VIEW IF EXISTS `d1`.`v`", "CREATE TABLE v ("}},
		{name: "a view holding a row policy to a table",
			diff: &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "d1.v"}}, TablesAdded: difftypes.TableChanges{tableNamed("v")}, Features: viewPolicies()},
			want: []string{"DROP ROW POLICY `leak` ON `d1`.`v`", "DROP VIEW IF EXISTS `d1`.`v`", "CREATE TABLE v ("}},
		{name: "a materialized view holding a row policy to a table",
			diff: &difftypes.SchemaDiff{MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "d1.v"}}, TablesAdded: difftypes.TableChanges{tableNamed("v")}, Features: viewPolicies()},
			want: []string{"DROP ROW POLICY `leak` ON `d1`.`v`", "DROP VIEW IF EXISTS `d1`.`v`", "CREATE TABLE v ("}},
		{name: "a table to a view",
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{tableRemoved("v")}, ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT 1 AS id"}}},
			want: []string{"DROP TABLE IF EXISTS v", "CREATE VIEW `v` AS"}},
		{name: "a table to a view named in another spelling",
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{tableRemoved("v")}, ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}}},
			want: []string{"DROP TABLE IF EXISTS v", "CREATE VIEW `d1`.`v` AS"}},
		{name: "a table holding a grant to a view",
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{tableRemoved("v")}, ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT 1 AS id"}},
				GrantsRemoved: []difftypes.GrantRef{selectOnV()}},
			want: []string{"REVOKE SELECT ON `d1`.`v` FROM `reader`", "DROP TABLE IF EXISTS v", "CREATE VIEW `v` AS"}},
		{name: "a view holding a grant to a table",
			diff: &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "d1.v"}}, TablesAdded: difftypes.TableChanges{tableNamed("v")},
				GrantsRemoved: []difftypes.GrantRef{selectOnV()}},
			want: []string{"REVOKE SELECT ON `d1`.`v` FROM `reader`", "DROP VIEW IF EXISTS `d1`.`v`", "CREATE TABLE v ("}},
		{name: "a table to a materialized view",
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{tableRemoved("v")}, MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "v", Body: "SELECT 1 AS id"}}},
			want: []string{"DROP TABLE IF EXISTS v", "CREATE MATERIALIZED VIEW `v` ENGINE = MergeTree ORDER BY tuple() AS"}},
		{name: "unrelated removals keep their place",
			diff: &difftypes.SchemaDiff{
				ViewsRemoved: difftypes.ViewChanges{{Name: "d1.old_view"}}, TablesRemoved: difftypes.TableRemovals{tableRemoved("old_table")},
				TablesAdded: difftypes.TableChanges{tableNamed("v")}, ViewsAdded: difftypes.ViewChanges{{Name: "w", Body: "SELECT 1 AS id"}},
			},
			want: []string{"CREATE TABLE v (", "CREATE VIEW `w` AS", "DROP VIEW IF EXISTS `d1`.`old_view`", "DROP TABLE IF EXISTS old_table"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planHeads(c, test.diff), qt.DeepEquals, test.want)
		})
	}
}

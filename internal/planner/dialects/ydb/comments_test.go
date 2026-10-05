package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// commented returns field with comment.
func commented(field schemamodel.Field, comment string) schemamodel.Field {
	field.Comment = comment
	return field
}

// A table's own comment and its columns' comments change in place after the
// table's other changes. An added column's comment follows its ADD COLUMN,
// and a dropped column's comment is removed after its DROP COLUMN, because
// YDB keeps the attribute under the column's name until a plan removes it.
func TestGenerateMigrationAST_TableComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := modified(difftypes.TableDiff{
		TableName:     "items",
		Desired:       itemsDeclaration(commented(field("note", "TEXT", true), "A note"), field("label", "TEXT", true)),
		CommentChange: &difftypes.CommentChange{Current: "Things", Desired: "Items"},
		ColumnsAdded:  difftypes.ColumnChanges{commented(field("note", "TEXT", true), "A note")},
		ColumnsModified: []difftypes.ColumnDiff{
			{ColumnName: "label", CommentChange: &difftypes.CommentChange{Current: "Shown"}},
		},
		ColumnsRemoved: difftypes.ColumnChanges{commented(field("old", "TEXT", true), "Gone"), field("plain", "TEXT", true)},
	})

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n"+
		"COMMENT ON COLUMN `items`.`note` IS 'A note';\n"+
		"ALTER TABLE `items` DROP COLUMN `old`;\n"+
		"ALTER TABLE `items` DROP COLUMN `plain`;\n"+
		"COMMENT ON TABLE `items` IS 'Items';\n"+
		"COMMENT ON COLUMN `items`.`label` IS NULL;\n"+
		"COMMENT ON COLUMN `items`.`old` IS NULL;\n")
}

// Index comments are written once every index is in place: a changed one in
// place, a renamed index's under its new name and removed from its old one, a
// dropped index's removed, and a rebuilt index's removed where the new index
// writes none over it. One the addition writes, and one on a table the plan
// drops, needs no statement.
func TestGenerateMigrationAST_IndexComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "items", Desired: itemsDeclaration(field("a", "TEXT", true))}},
		TablesRemoved:  []string{"legacy"},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "items", Index: schemamodel.Index{Name: "by_b", Fields: []string{"a"}, Unique: false, Comment: "New"}},
			{TableName: "items", Index: schemamodel.Index{Name: "by_c", Fields: []string{"a"}, Type: "async"}},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{TableName: "items", Name: "by_b"},
			{TableName: "items", Name: "by_c"},
			{TableName: "items", Name: "by_d"},
			{TableName: "legacy", Name: "legacy_ix"},
		},
		IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "by_e", To: "by_f"}},
		IndexCommentsChanged: []difftypes.IndexCommentChange{
			{TableName: "items", Name: "by_a", Current: "Old", Desired: "Changed"},
			{TableName: "items", Name: "by_b", Current: "Old", Desired: "New"},
			{TableName: "items", Name: "by_c", Current: "Old"},
			{TableName: "items", Name: "by_d", Current: "Gone"},
			{TableName: "items", Name: "by_f", From: "by_e", Current: "Moves", Desired: "Moves"},
			{TableName: "legacy", Name: "legacy_ix", Current: "Gone with the table"},
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "ALTER TABLE `items` DROP INDEX `by_b`;\n"+
		"ALTER TABLE `items` DROP INDEX `by_c`;\n"+
		"ALTER TABLE `items` DROP INDEX `by_d`;\n"+
		"ALTER TABLE `items` RENAME INDEX `by_e` TO `by_f`;\n"+
		"ALTER TABLE `items` ADD INDEX `by_b` GLOBAL SYNC ON (`a`);\n"+
		"COMMENT ON INDEX `by_b` ON `items` IS 'New';\n"+
		"ALTER TABLE `items` ADD INDEX `by_c` GLOBAL ASYNC ON (`a`);\n"+
		"COMMENT ON INDEX `by_a` ON `items` IS 'Changed';\n"+
		"COMMENT ON INDEX `by_c` ON `items` IS NULL;\n"+
		"COMMENT ON INDEX `by_d` ON `items` IS NULL;\n"+
		"COMMENT ON INDEX `by_f` ON `items` IS 'Moves';\n"+
		"COMMENT ON INDEX `by_e` ON `items` IS NULL;\n"+
		"DROP TABLE `legacy`;\n")
}

// A renamed index's comment moves only as far as there is one: none to set
// under a name the declaration leaves uncommented, and none to remove under a
// name that held none.
func TestGenerateMigrationAST_IndexComments_RenameHalves(t *testing.T) {
	tests := []struct {
		name   string
		change difftypes.IndexCommentChange
		want   string
	}{
		{name: "a comment dropped in the rename",
			change: difftypes.IndexCommentChange{TableName: "items", Name: "b", From: "a", Current: "Old"},
			want:   "COMMENT ON INDEX `a` ON `items` IS NULL;\n"},
		{name: "a comment declared in the rename",
			change: difftypes.IndexCommentChange{TableName: "items", Name: "b", From: "a", Desired: "New"},
			want:   "COMMENT ON INDEX `b` ON `items` IS 'New';\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesRenamed:       []difftypes.IndexRename{{TableName: "items", From: "a", To: "b"}},
				IndexCommentsChanged: []difftypes.IndexCommentChange{test.change},
			}

			got := render(c, capability.YDB262(), diff)

			c.Assert(got, qt.Equals, "ALTER TABLE `items` RENAME INDEX `a` TO `b`;\n"+test.want)
		})
	}
}

// A view the plan keeps takes its changed comment after every view is
// created; a view it creates, or drops and creates again for a new query,
// writes its comment after its CREATE VIEW instead, and starts with none.
func TestGenerateMigrationAST_ViewComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{
			ViewName: "fresh", PreviousBody: "SELECT 1 AS a",
			Desired: schemamodel.View{Name: "fresh", Body: "SELECT 2 AS a", Comment: "Recreated"},
		}},
		ObjectCommentsChanged: []difftypes.ObjectCommentChange{
			{Kind: difftypes.CommentedView, Name: "kept", Current: "Old", Desired: "New"},
			{Kind: difftypes.CommentedView, Name: "fresh", Current: "Old", Desired: "Recreated"},
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP VIEW `fresh`;\n"+
		"CREATE VIEW `fresh` WITH (security_invoker = TRUE) AS\n"+
		"SELECT 2 AS a\n"+
		";\n"+
		"COMMENT ON VIEW `fresh` IS 'Recreated';\n"+
		"COMMENT ON VIEW `kept` IS 'New';\n")
}

// A comment change the target cannot keep is refused before anything is
// planned, by the key that would keep it.
func TestGenerateMigrationAST_Comments_FailurePath(t *testing.T) {
	withoutAttributes := capability.YDB262().With(capability.CommentAttributes, false)
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantKey capability.Capability
		wantErr string
	}{
		{name: "a table's comment", caps: withoutAttributes,
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				CommentChange: &difftypes.CommentChange{Desired: "x"}}),
			wantKey: capability.CommentAttributes,
			wantErr: `changing the comments of table "items", which requires target capability comment_attributes, .*`},
		{name: "a dropped column's comment", caps: withoutAttributes,
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsRemoved: difftypes.ColumnChanges{commented(field("old", "TEXT", true), "x")}}),
			wantKey: capability.CommentAttributes,
			wantErr: `changing the comments of table "items", which requires target capability comment_attributes, .*`},
		{name: "an index's comment", caps: withoutAttributes,
			diff: &difftypes.SchemaDiff{IndexCommentsChanged: []difftypes.IndexCommentChange{
				{TableName: "items", Name: "i", Desired: "x"}}},
			wantKey: capability.CommentAttributes,
			wantErr: `changing the comment of index "i" of table "items", which requires target capability comment_attributes, .*`},
		{name: "a view's comment", caps: capability.YDB262().With(capability.ViewComments, false),
			diff: &difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedView, Name: "v", Desired: "x"}}},
			wantKey: capability.ViewComments,
			wantErr: `changing the comment of view v, which requires target capability view_comments, .*`},
		{name: "a sequence's comment", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedSequence, Name: "s", Desired: "x"}}},
			wantKey: capability.SequenceComments,
			wantErr: `changing the comment of sequence s, which requires target capability sequence_comments, .*`},
		{name: "a constraint's comment", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{ConstraintCommentsChanged: []difftypes.ConstraintCommentChange{
				{TableName: "items", Name: "items_pkey", Desired: "x"}}},
			wantKey: capability.ConstraintComments,
			wantErr: `changing the comment of constraint "items_pkey" of table "items", which requires target capability constraint_comments, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(nodes, qt.IsNil)
		})
	}
}

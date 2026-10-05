package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
	"ptah.run/internal/renderdiag"
)

// commentedTable is a table in a directory with a comment of its own, on a
// column, on an index and on a UNIQUE constraint, which renders as the unique
// index that keeps the comment.
func commentedTable() *ast.CreateTableNode {
	table := ast.NewCreateTable("shop.users")
	table.Comment = "People who sign in"
	table.AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	table.AddColumn(ast.NewColumn("email", "TEXT").SetComment("Login, it's unique"))
	table.AddColumn(ast.NewColumn("name", "TEXT"))
	table.AddIndex(&ast.IndexNode{Name: "by_name", Table: "shop.users", Columns: []string{"name"}, Comment: "Lookup\nby name"})
	table.Constraints = append(table.Constraints, &ast.ConstraintNode{
		Type: ast.UniqueConstraint, Name: "users_email_key", Columns: []string{"email"}, Comment: "One login each",
	})
	return table
}

// Every comment is Ptah's own COMMENT ON statement after the statement that
// creates its object, since YDB sets an attribute only on an object that
// exists: a table's, its columns' and its indexes' after CREATE TABLE, a
// column's after ADD COLUMN, an index's after ADD INDEX, a view's after
// CREATE VIEW. A changed comment is the same statement, and a removed one is
// written NULL.
func TestRender_Comments_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a new table, its columns and its indexes",
			node: commentedTable(),
			want: "CREATE TABLE `shop/users` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `email` Utf8,\n" +
				"    `name` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `by_name` GLOBAL SYNC ON (`name`),\n" +
				"    INDEX `users_email_key` GLOBAL UNIQUE SYNC ON (`email`)\n" +
				");\n" +
				"COMMENT ON TABLE `shop/users` IS 'People who sign in';\n" +
				"COMMENT ON COLUMN `shop/users`.`email` IS 'Login, it\\'s unique';\n" +
				"COMMENT ON INDEX `by_name` ON `shop/users` IS 'Lookup\\nby name';\n" +
				"COMMENT ON INDEX `users_email_key` ON `shop/users` IS 'One login each';\n",
		},
		{
			name: "a new view",
			node: &ast.CreateViewNode{Name: "shop.active", Body: "SELECT id FROM `shop/users`", Comment: "Recent"},
			want: "CREATE VIEW `shop/active` WITH (security_invoker = TRUE) AS\n" +
				"SELECT id FROM `shop/users`\n" +
				";\n" +
				"COMMENT ON VIEW `shop/active` IS 'Recent';\n",
		},
		{
			name: "a table's comment changed",
			node: alter(&ast.SetCommentOperation{Comment: "Customers", HasCurrent: true}),
			want: "COMMENT ON TABLE `t` IS 'Customers';\n",
		},
		{
			name: "a column's comment removed",
			node: alter(&ast.SetCommentOperation{Column: "email", HasCurrent: true}),
			want: "COMMENT ON COLUMN `t`.`email` IS NULL;\n",
		},
		{
			name: "a column added with its comment",
			node: alter(&ast.AddColumnOperation{Column: ast.NewColumn("phone", "TEXT").SetComment("Mobile")}),
			want: "ALTER TABLE `t` ADD COLUMN `phone` Utf8;\n" +
				"COMMENT ON COLUMN `t`.`phone` IS 'Mobile';\n",
		},
		{
			name: "an index added with its comment",
			node: &ast.IndexNode{Name: "by_phone", Table: "t", Columns: []string{"phone"}, Comment: "Lookup"},
			want: "ALTER TABLE `t` ADD INDEX `by_phone` GLOBAL SYNC ON (`phone`);\n" +
				"COMMENT ON INDEX `by_phone` ON `t` IS 'Lookup';\n",
		},
		{
			name: "a view's comment changed",
			node: ast.NewObjectComment(ast.CommentedView, "shop.active", "Active"),
			want: "COMMENT ON VIEW `shop/active` IS 'Active';\n",
		},
		{
			name: "an index's comment removed",
			node: ast.NewObjectComment(ast.CommentedIndex, "by_phone", "").SetTable("shop.users"),
			want: "COMMENT ON INDEX `by_phone` ON `shop/users` IS NULL;\n",
		},
		{
			name: "a comment of 4096 bytes",
			node: alter(&ast.SetCommentOperation{Comment: strings.Repeat("x", 4096)}),
			want: "COMMENT ON TABLE `t` IS '" + strings.Repeat("x", 4096) + "';\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A schema is a directory, which holds no attribute, so its comment is
// recorded as left out and the schema renders no statement, as one without a
// comment does.
func TestRender_SchemaComment(t *testing.T) {
	c := qt.New(t)
	sink := &renderdiag.Sink{}
	renderer := ydb.NewWithCapabilities(capability.YDB262())
	renderer.ReportOmissionsTo(sink)

	got, err := renderer.Render(&ast.CreateSchemaNode{Name: "shop", Comment: "The shop"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "")
	c.Assert(sink.Omissions(), qt.DeepEquals, []renderdiag.Omission{
		renderdiag.PropertyOmission(renderdiag.SchemaKind, "shop", renderdiag.CommentProperty, "The shop"),
	})
}

// A comment on a target without the key that keeps it is refused by that key;
// so is one on an object YDB does not name or does not have.
func TestRender_Comments_FailurePath_ByKey(t *testing.T) {
	withoutAttributes := capability.YDB262().With(capability.CommentAttributes, false)
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
		wantErr string
	}{
		{name: "a table's comment", caps: withoutAttributes, node: commentedTable(),
			wantKey: capability.CommentAttributes,
			wantErr: `the comment on table "shop.users", which requires target capability comment_attributes, unavailable on this ydb target`},
		{name: "a column's comment", caps: withoutAttributes,
			node:    alter(&ast.SetCommentOperation{Column: "email", Comment: "x"}),
			wantKey: capability.CommentAttributes,
			wantErr: `the comment on column "email" of table "t", which requires target capability comment_attributes, .*`},
		{name: "an index's comment", caps: withoutAttributes,
			node:    &ast.IndexNode{Name: "i", Table: "t", Columns: []string{"n"}, Comment: "x"},
			wantKey: capability.CommentAttributes,
			wantErr: `the comment on index "i" of table "t", which requires target capability comment_attributes, .*`},
		{name: "a view's comment", caps: capability.YDB262().With(capability.ViewComments, false),
			node:    &ast.CreateViewNode{Name: "v", Body: "SELECT 1 AS a", Comment: "x"},
			wantKey: capability.ViewComments,
			wantErr: `the comment on view v, which requires target capability view_comments, .*`},
		{name: "the primary key's comment", caps: capability.YDB262(),
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT")},
				Constraints: []*ast.ConstraintNode{{Type: ast.PrimaryKeyConstraint, Columns: []string{"id"}, Comment: "x"}}},
			wantKey: capability.ConstraintComments,
			wantErr: `the comment on the primary key of table "t", which requires target capability constraint_comments, .*`},
		{name: "a constraint's comment changed", caps: capability.YDB262(),
			node:    alter(&ast.SetConstraintCommentOperation{Constraint: "t_pkey", Comment: "x"}),
			wantKey: capability.ConstraintComments,
			wantErr: `the comment on constraint "t_pkey" of table "t", which requires target capability constraint_comments, .*`},
		{name: "a sequence's comment", caps: capability.YDB262(),
			node:    ast.NewObjectComment(ast.CommentedSequence, "s", "x"),
			wantKey: capability.SequenceComments,
			wantErr: `COMMENT ON SEQUENCE s, which requires target capability sequence_comments, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}

// A comment YDB cannot hold is refused by name, before any statement is
// written, with the limit it breaks.
func TestRender_Comments_FailurePath(t *testing.T) {
	crowded := ast.NewCreateTable("t")
	crowded.AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	for _, name := range []string{"a", "b", "c"} {
		crowded.AddColumn(ast.NewColumn(name, "TEXT").SetComment(strings.Repeat(name, 4096)))
	}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "a comment over 4096 bytes",
			node:    alter(&ast.SetCommentOperation{Column: "c", Comment: strings.Repeat("x", 4097)}),
			wantErr: `the comment on column "c" of table "t": the comment is 4097 bytes long, and YDB keeps at most 4096 bytes in an attribute value`},
		{name: "a column name too long for the key",
			node: alter(&ast.AddColumnOperation{Column: ast.NewColumn(strings.Repeat("c", 81), "TEXT").SetComment("x")}),
			wantErr: `the comment on column "c+" of table "t": YDB keeps the comment as the table attribute ` +
				`"ptah.comment.column.c+", 101 bytes long, and an attribute key takes at most 100 bytes; a column name takes at most 80`},
		{name: "comments over the table's attribute budget",
			node: crowded,
			wantErr: `table "t": the comments of table "t" take 12351 bytes as YDB table attributes, keys and text ` +
				`together, and YDB keeps at most 10240 bytes of attributes on one object`},
		{name: "text that is not UTF-8",
			node:    &ast.CreateViewNode{Name: "v", Body: "SELECT 1 AS a", Comment: "\xff"},
			wantErr: `the comment on view v: the comment is not UTF-8 text, which YDB keeps in an attribute value`},
		{name: "an index comment naming no table",
			node:    ast.NewObjectComment(ast.CommentedIndex, "i", "x"),
			wantErr: `the comment on index i: YDB keeps an index's comment on its table, and the statement names none`},
		{name: "a column definition alone",
			node:    ast.NewColumn("n", "TEXT").SetComment("x"),
			wantErr: `the comment on column "n": a YDB column's comment is an attribute of its table, which a column definition alone cannot carry`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}

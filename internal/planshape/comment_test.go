package planshape_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planshape"
)

// setComment is a COMMENT ON for table, or for one of its columns when column is not
// empty.
func setComment(table, column, comment string) *ast.AlterTableNode {
	return alter(table, &ast.SetCommentOperation{Column: column, Comment: comment})
}

// commentedTasks is a table holding id, title, old and body, to which a plan adds due
// and owner.
func commentedTasks() *difftypes.SchemaDiff {
	held := make([]catalog.Column, 0, 4)
	for _, name := range []string{"id", "title", "old", "body"} {
		held = append(held, catalog.Column{Name: name})
	}
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:    "tasks",
		Current:      schemacapture.TableObservation{Table: catalog.Table{Name: "tasks", Columns: held}},
		ColumnsAdded: difftypes.ColumnChanges{{Name: "due"}, {Name: "owner"}},
	}}}
}

// TestApply_WritesATablesCommentsAfterItsIndexes_HappyPath pins the comments of a
// table the plan changes where the community binary writes them on PostgreSQL: after
// the table's one ALTER TABLE and its new indexes, the table's own comment first, then
// its columns' in the order the table holds them. A comment the planner wrote between
// two of the table's column changes would split that ALTER TABLE.
func TestApply_WritesATablesCommentsAfterItsIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		setComment("tasks", "body", "b2"),
		alter("tasks", addColumn("due")),
		setComment("tasks", "", "t2"),
		alter("tasks", modifyColumn("title", "text")),
		setComment("tasks", "title", "v2"),
		alter("tasks", dropColumn("old")),
		ast.NewIndex("tasks_due", "tasks", "due"),
		alter("tasks", addColumn("owner")),
		setComment("tasks", "owner", "who"),
	}

	got := planshape.Apply(platform.Postgres, nodes, commentedTasks())

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals,
		"ALTER TABLE \"tasks\" ALTER COLUMN \"title\" TYPE text, ALTER COLUMN \"title\" DROP NOT NULL, ALTER COLUMN \"title\" DROP DEFAULT, "+
			"DROP COLUMN \"old\", ADD COLUMN \"due\" integer NULL, ADD COLUMN \"owner\" integer NULL;\n\n"+
			"CREATE INDEX \"tasks_due\" ON \"tasks\" (\"due\");\n"+
			"COMMENT ON TABLE \"tasks\" IS 't2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"body\" IS 'b2';\n\n"+
			"COMMENT ON COLUMN \"tasks\".\"owner\" IS 'who';\n\n")
}

// TestApply_LeavesACommentWhereItCannotMove_HappyPath is a comment the pass keeps in
// the planner's order. On PostgreSQL a column's comment the planner wrote ahead of a
// change to that column holds the change behind it, so the ALTER TABLE stays split
// around the comment. The MySQL family writes a comment into the column or the table
// options, so its comment statements stay where the planner put them.
func TestApply_LeavesACommentWhereItCannotMove_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		want    string
	}{
		{
			name:    "a column's comment ahead of a change to the column on PostgreSQL",
			dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("tasks", addColumn("due")),
				setComment("tasks", "title", "v2"),
				alter("tasks", modifyColumn("title", "text")),
			},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n" +
				"ALTER TABLE \"tasks\" ALTER COLUMN \"title\" TYPE text, ALTER COLUMN \"title\" DROP NOT NULL, ALTER COLUMN \"title\" DROP DEFAULT;\n\n",
		},
		{
			name:    "a table's comment on MySQL",
			dialect: platform.MySQL,
			nodes: []ast.Node{
				setComment("tasks", "", "t2"),
				alter("tasks", addColumn("due")),
			},
			want: "ALTER TABLE `tasks` COMMENT='t2';\n\n" +
				"ALTER TABLE `tasks` ADD COLUMN `due` int NULL;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, commentedTasks())

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// taskIndex is an index of tasks on column, with comment when it is not empty.
func taskIndex(name, column, comment string) *ast.IndexNode {
	index := ast.NewIndex(name, "tasks", column)
	index.Comment = comment
	return index
}

// keptIndexComment is the COMMENT ON INDEX the planner writes for an index of table
// the plan keeps, after every new index.
func keptIndexComment(table, name, comment string) *ast.ObjectCommentNode {
	return ast.NewObjectComment(ast.CommentedIndex, name, comment).SetTable(table)
}

// TestApply_WritesAChangedTablesIndexCommentsAfterItsOwn_HappyPath pins the comments
// of the indexes of a table the plan changes where the community binary writes them
// on PostgreSQL: after the table's new indexes and its columns' comments, those of the
// indexes the table keeps by name, then those of its new indexes by name, ahead of the
// next table the plan changes. The PostgreSQL renderer writes a new index's comment
// right after its CREATE INDEX, and the planner writes a kept index's comment after
// every new index of the plan. The planner's indexes keep their comments.
func TestApply_WritesAChangedTablesIndexCommentsAfterItsOwn_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		alter("tasks", addColumn("due")),
		alter("users", addColumn("nick")),
		setComment("tasks", "title", "v2"),
		taskIndex("tasks_title", "title", "it"),
		taskIndex("tasks_body", "body", ""),
		taskIndex("tasks_due", "due", "id"),
		ast.NewIndex("users_nick", "users", "nick"),
		keptIndexComment("tasks", "tasks_old", "ko"),
		keptIndexComment("tasks", "tasks_id", "ki"),
	}

	got := planshape.Apply(platform.Postgres, slices.Clone(nodes), commentedTasks())

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals,
		"ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n"+
			"CREATE INDEX \"tasks_body\" ON \"tasks\" (\"body\");\n"+
			"CREATE INDEX \"tasks_due\" ON \"tasks\" (\"due\");\n"+
			"CREATE INDEX \"tasks_title\" ON \"tasks\" (\"title\");\n"+
			"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n"+
			"COMMENT ON INDEX \"tasks_id\" IS 'ki';\n"+
			"COMMENT ON INDEX \"tasks_old\" IS 'ko';\n"+
			"COMMENT ON INDEX \"tasks_due\" IS 'id';\n"+
			"COMMENT ON INDEX \"tasks_title\" IS 'it';\n"+
			"ALTER TABLE \"users\" ADD COLUMN \"nick\" integer NULL;\n\n"+
			"CREATE INDEX \"users_nick\" ON \"users\" (\"nick\");\n")
	c.Assert(nodes[3].(*ast.IndexNode).Comment, qt.Equals, "it")
}

// TestApply_LeavesAnObjectCommentWhereItCannotMove_HappyPath is a COMMENT ON the pass
// keeps where the planner put it. A concurrent build ends the run, so its comment stays
// with its CREATE INDEX; a view ends it too, so a kept index's comment the planner
// wrote after the view stays there; and the MySQL family writes an index's comment
// into its CREATE INDEX. An index's comment that names no table, which only a diff
// built by hand plans, and a trigger's comment are not a table's comments, so each
// ends the run and keeps the ALTER TABLE split around it.
func TestApply_LeavesAnObjectCommentWhereItCannotMove_HappyPath(t *testing.T) {
	concurrent := taskIndex("tasks_due", "due", "id")
	concurrent.Concurrently = true
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		want    string
	}{
		{
			name:    "a concurrent index on PostgreSQL",
			dialect: platform.Postgres,
			nodes:   []ast.Node{alter("tasks", addColumn("due")), concurrent, setComment("tasks", "title", "v2")},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"CREATE INDEX CONCURRENTLY \"tasks_due\" ON \"tasks\" (\"due\");\n" +
				"COMMENT ON INDEX \"tasks_due\" IS 'id';\n" +
				"COMMENT ON COLUMN \"tasks\".\"title\" IS 'v2';\n\n",
		},
		{
			name:    "a kept index's comment behind a view on PostgreSQL",
			dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("tasks", addColumn("due")),
				&ast.CreateViewNode{Name: "v", Body: "SELECT id FROM tasks"},
				keptIndexComment("tasks", "tasks_old", "ko"),
			},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"CREATE VIEW \"v\" AS\nSELECT id FROM tasks\n;\n" +
				"COMMENT ON INDEX \"tasks_old\" IS 'ko';\n",
		},
		{
			name:    "a new index's comment on MySQL",
			dialect: platform.MySQL,
			nodes:   []ast.Node{alter("tasks", addColumn("due")), taskIndex("tasks_due", "due", "id")},
			want: "ALTER TABLE `tasks` ADD COLUMN `due` int NULL;\n\n" +
				"CREATE INDEX `tasks_due` ON `tasks` (`due`) COMMENT 'id';\n",
		},
		{
			name:    "an index's comment that names no table on PostgreSQL",
			dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("tasks", addColumn("due")),
				ast.NewObjectComment(ast.CommentedIndex, "tasks_old", "ko"),
				alter("tasks", addColumn("owner")),
			},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"COMMENT ON INDEX \"tasks_old\" IS 'ko';\n" +
				"ALTER TABLE \"tasks\" ADD COLUMN \"owner\" integer NULL;\n\n",
		},
		{
			name:    "a trigger's comment on PostgreSQL",
			dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("tasks", addColumn("due")),
				ast.NewObjectComment(ast.CommentedTrigger, "tasks_touch", "tt").SetTable("tasks"),
				alter("tasks", addColumn("owner")),
			},
			want: "ALTER TABLE \"tasks\" ADD COLUMN \"due\" integer NULL;\n\n" +
				"COMMENT ON TRIGGER \"tasks_touch\" ON \"tasks\" IS 'tt';\n" +
				"ALTER TABLE \"tasks\" ADD COLUMN \"owner\" integer NULL;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, commentedTasks())

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// TestAlignRollback_LeavesAnIndexCommentWhereThePlannerPutIt_HappyPath is a rollback,
// which keeps each comment where the planner wrote it: what it undoes says nothing
// about where a comment goes. A kept index's comment between two changes to its table
// keeps the ALTER TABLE split around it, and an index the rollback creates keeps its
// comment right after its CREATE INDEX.
func TestAlignRollback_LeavesAnIndexCommentWhereThePlannerPutIt_HappyPath(t *testing.T) {
	tests := []struct {
		name              string
		forward, rollback []ast.Node
		want              string
	}{
		{
			name:     "a kept index's comment",
			forward:  []ast.Node{alter("tasks", addColumn("due"), addColumn("owner")), keptIndexComment("tasks", "tasks_old", "ko")},
			rollback: []ast.Node{alter("tasks", dropColumn("owner")), keptIndexComment("tasks", "tasks_old", "k0"), alter("tasks", dropColumn("due"))},
			want: "ALTER TABLE \"tasks\" DROP COLUMN \"owner\";\n\n" +
				"COMMENT ON INDEX \"tasks_old\" IS 'k0';\n" +
				"ALTER TABLE \"tasks\" DROP COLUMN \"due\";\n\n",
		},
		{
			name:     "an index the rollback creates",
			forward:  []ast.Node{alter("tasks", addColumn("due")), ast.NewDropIndex("tasks_title").SetTable("tasks"), alter("tasks", addColumn("owner"))},
			rollback: []ast.Node{alter("tasks", dropColumn("owner")), taskIndex("tasks_title", "title", "it"), alter("tasks", dropColumn("due"))},
			want: "ALTER TABLE \"tasks\" DROP COLUMN \"owner\";\n\n" +
				"CREATE INDEX \"tasks_title\" ON \"tasks\" (\"title\");\n" +
				"COMMENT ON INDEX \"tasks_title\" IS 'it';\n" +
				"ALTER TABLE \"tasks\" DROP COLUMN \"due\";\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(platform.Postgres, test.forward, test.rollback, commentedTasks())

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, test.want)
		})
	}
}

// commentedNewTables is two tables a plan creates, each with an index and with
// comments on the table and on some of its columns.
func commentedNewTables() []ast.Node {
	first := ast.NewCreateTable("a").AddColumn(key("id")).
		AddColumn(nullable("name", "text").SetComment("na")).AddColumn(nullable("code", "text").SetComment("ca"))
	first.Comment = "ta"
	second := ast.NewCreateTable("b").AddColumn(key("id")).AddColumn(nullable("title", "text").SetComment("tb"))
	second.Comment = "tbb"
	return []ast.Node{first, ast.NewIndex("a_name", "a", "name"), second, ast.NewIndex("b_title", "b", "title")}
}

// TestApply_WritesANewTablesCommentsAfterItsIndexes_HappyPath pins the comments of a
// table the plan creates where the community binary writes them on PostgreSQL: after
// the table and its indexes, the table's own comment first, then its columns' in the
// table's column order, ahead of the next table. The PostgreSQL renderer writes them
// right after CREATE TABLE. The planner's nodes keep their comments.
func TestApply_WritesANewTablesCommentsAfterItsIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := commentedNewTables()

	got := planshape.Apply(platform.Postgres, slices.Clone(nodes), nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "a" (
  "id" integer NOT NULL,
  "name" text NULL,
  "code" text NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "a_name" ON "a" ("name");
COMMENT ON TABLE "a" IS 'ta';

COMMENT ON COLUMN "a"."name" IS 'na';

COMMENT ON COLUMN "a"."code" IS 'ca';

CREATE TABLE "b" (
  "id" integer NOT NULL,
  "title" text NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "b_title" ON "b" ("title");
COMMENT ON TABLE "b" IS 'tbb';

COMMENT ON COLUMN "b"."title" IS 'tb';

`)
	c.Assert(nodes[0].(*ast.CreateTableNode).Comment, qt.Equals, "ta")
	c.Assert(nodes[0].(*ast.CreateTableNode).Columns[1].Comment, qt.Equals, "na")
}

// TestApply_KeepsANewTablesCommentsInItsTableOnMySQL_HappyPath is the control: the
// MySQL family writes a new table's comments inside its CREATE TABLE, so they stay
// there.
func TestApply_KeepsANewTablesCommentsInItsTableOnMySQL_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := planshape.Apply(platform.MySQL, commentedNewTables(), nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.MySQL, got...)), qt.Equals, "CREATE TABLE `a` (\n"+
		"  `id` int NOT NULL,\n"+
		"  `name` text NULL COMMENT 'na',\n"+
		"  `code` text NULL COMMENT 'ca',\n"+
		"  PRIMARY KEY (`id`)\n"+
		") COMMENT='ta';\n\n"+
		"CREATE INDEX `a_name` ON `a` (`name`);\n"+
		"CREATE TABLE `b` (\n"+
		"  `id` int NOT NULL,\n"+
		"  `title` text NULL COMMENT 'tb',\n"+
		"  PRIMARY KEY (`id`)\n"+
		") COMMENT='tbb';\n\n"+
		"CREATE INDEX `b_title` ON `b` (`title`);\n")
}

// commentedTable is a table t the plan creates, with a comment on the table and on
// its column a.
func commentedTable() *ast.CreateTableNode {
	table := ast.NewCreateTable("t").AddColumn(key("id")).
		AddColumn(nullable("a", "text").SetComment("aa")).AddColumn(nullable("b", "text"))
	table.Comment = "tt"
	return table
}

// commentedIndex is an index of t on column, with a comment.
func commentedIndex(name, column, comment string) *ast.IndexNode {
	index := ast.NewIndex(name, "t", column)
	index.Comment = comment
	return index
}

// TestApply_WritesANewTablesIndexCommentsAfterItsOwn_HappyPath pins the comments of
// a new table's indexes where the community binary writes them on PostgreSQL: after
// the table's indexes and the comments of the table and its columns, in the order
// the plan creates the indexes. The PostgreSQL renderer writes an index's comment
// right after its CREATE INDEX. The planner's indexes keep their comments.
func TestApply_WritesANewTablesIndexCommentsAfterItsOwn_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{commentedTable(), commentedIndex("t_a", "a", "ia"), commentedIndex("t_b", "b", "ib")}

	got := planshape.Apply(platform.Postgres, slices.Clone(nodes), nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "t" (
  "id" integer NOT NULL,
  "a" text NULL,
  "b" text NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "t_a" ON "t" ("a");
CREATE INDEX "t_b" ON "t" ("b");
COMMENT ON TABLE "t" IS 'tt';

COMMENT ON COLUMN "t"."a" IS 'aa';

COMMENT ON INDEX "t_a" IS 'ia';
COMMENT ON INDEX "t_b" IS 'ib';
`)
	c.Assert(nodes[1].(*ast.IndexNode).Comment, qt.Equals, "ia")
}

// TestApply_WritesANewTablesCommentsAfterAnIndexThatWaits_HappyPath pins the
// comments of a new table whose index has to wait for a view the plan creates after
// the table: they follow the index, after the view and the next table, rather than
// the CREATE TABLE.
func TestApply_WritesANewTablesCommentsAfterAnIndexThatWaits_HappyPath(t *testing.T) {
	c := qt.New(t)
	other := ast.NewCreateTable("u").AddColumn(key("id"))
	view := &ast.CreateViewNode{Name: "v", Body: "SELECT id, a FROM t"}

	got := planshape.Apply(platform.Postgres, []ast.Node{commentedTable(), other, view, commentedIndex("t_a", "a", "ia")}, nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "t" (
  "id" integer NOT NULL,
  "a" text NULL,
  "b" text NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "u" (
  "id" integer NOT NULL,
  PRIMARY KEY ("id")
);

CREATE VIEW "v" AS
SELECT id, a FROM t
;
CREATE INDEX "t_a" ON "t" ("a");
COMMENT ON TABLE "t" IS 'tt';

COMMENT ON COLUMN "t"."a" IS 'aa';

COMMENT ON INDEX "t_a" IS 'ia';
`)
}

// TestApply_WritesANewTablesIndexCommentsTogetherWhenOneWaits_HappyPath pins the
// comments of a new table with two indexes, of which the second waits for a view the
// plan creates after the table. Every comment of the table follows the second index,
// the first index's included, as on a table whose indexes nothing holds back.
func TestApply_WritesANewTablesIndexCommentsTogetherWhenOneWaits_HappyPath(t *testing.T) {
	c := qt.New(t)
	view := &ast.CreateViewNode{Name: "v", Body: "SELECT id, a FROM t"}

	got := planshape.Apply(platform.Postgres,
		[]ast.Node{commentedTable(), commentedIndex("t_a", "a", "ia"), view, commentedIndex("t_b", "b", "ib")}, nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "t" (
  "id" integer NOT NULL,
  "a" text NULL,
  "b" text NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "t_a" ON "t" ("a");
CREATE VIEW "v" AS
SELECT id, a FROM t
;
CREATE INDEX "t_b" ON "t" ("b");
COMMENT ON TABLE "t" IS 'tt';

COMMENT ON COLUMN "t"."a" IS 'aa';

COMMENT ON INDEX "t_a" IS 'ia';
COMMENT ON INDEX "t_b" IS 'ib';
`)
}

// TestApply_WritesANewTablesCommentsBeforeAnotherTablesIndex_HappyPath pins the
// comments of a new table followed by another table's index. A function that reads
// the first table stands between the two tables, so the first table's index waits
// until after the second table: the second table's comments follow its CREATE
// TABLE, and the first table's index comes after them.
func TestApply_WritesANewTablesCommentsBeforeAnotherTablesIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	first := ast.NewCreateTable("a").AddColumn(key("id")).AddColumn(nullable("n", "text"))
	reader := ast.NewCreateFunction("a_count").SetReturns("bigint")
	reader.Body = "SELECT count(*) FROM a"
	reader.Language = "sql"

	second := commentedTable().AddColumn(nullable("code", "bigint").SetDefaultExpression("a_count()"))

	got := planshape.Apply(platform.Postgres, []ast.Node{first, reader, second, ast.NewIndex("a_n", "a", "n")},
		&difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "a_count"}}}})

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "a" (
  "id" integer NOT NULL,
  "n" text NULL,
  PRIMARY KEY ("id")
);

CREATE FUNCTION "a_count"() RETURNS bigint AS $$
SELECT count(*) FROM a
$$
LANGUAGE sql;
CREATE TABLE "t" (
  "id" integer NOT NULL,
  "a" text NULL,
  "b" text NULL,
  "code" bigint NULL DEFAULT a_count(),
  PRIMARY KEY ("id")
);

COMMENT ON TABLE "t" IS 'tt';

COMMENT ON COLUMN "t"."a" IS 'aa';

CREATE INDEX "a_n" ON "a" ("n");
`)
}

// TestApply_KeepsANewTablesIndexAheadOfAnotherTablesConcurrentIndex_HappyPath pins
// the index of a new table that the planner writes after another table's index. The
// other index is built concurrently, so it ends the run the pass arranges, and only
// the index found to be the new table's moves ahead of it with the table's comments.
func TestApply_KeepsANewTablesIndexAheadOfAnotherTablesConcurrentIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("t").AddColumn(key("id")).AddColumn(nullable("a", "text"))
	table.Comment = "tt"
	other := ast.NewIndex("b_x", "b", "x")
	other.Concurrently = true

	got := planshape.Apply(platform.Postgres, []ast.Node{table, other, ast.NewIndex("t_a", "t", "a")}, nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, `CREATE TABLE "t" (
  "id" integer NOT NULL,
  "a" text NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "t_a" ON "t" ("a");
COMMENT ON TABLE "t" IS 'tt';

CREATE INDEX CONCURRENTLY "b_x" ON "b" ("x");
`)
}

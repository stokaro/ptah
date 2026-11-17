package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// commentedNote is a plain column the table gains, with a comment.
func commentedNote() schemamodel.Field {
	return schemamodel.Field{StructName: "Task", Name: "note", Type: "text", Nullable: true, Comment: "plain note"}
}

// commentedSearchVector is a stored generated column the table gains, with a
// comment: the shape a full-text search column takes.
func commentedSearchVector() schemamodel.Field {
	return schemamodel.Field{
		StructName: "Task", Name: "search_vector", Type: "tsvector", Nullable: true,
		GeneratedExpression: "to_tsvector('simple', coalesce(title, ''))", GeneratedKind: "STORED",
		Comment: "search:v1",
	}
}

// addedColumnsDiff adds columns to tasks, which the declaration holds.
func addedColumnsDiff(columns ...schemamodel.Field) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
		TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsAdded: columns}},
	}
}

// renderStyledPlan plans diff in style and renders it for PostgreSQL 18.
func renderStyledPlan(c *qt.C, style planext.StatementStyle, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := postgres.New().WithStatementStyle(style).GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_AddedColumnComment_HappyPath pins the comment of a column the
// plan adds. PostgreSQL has no comment clause in a column definition, so the
// comment is a COMMENT ON COLUMN after the statement that adds the column.
// Without it the column is added bare, and the next comparison plans the
// comment on its own: the schema takes two migrations to converge. A generated
// column is planned on the same path as a plain one.
func TestPlanner_AddedColumnComment_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		style   planext.StatementStyle
		columns []schemamodel.Field
		want    string
	}{
		{
			name:    "a plain column, exact style",
			style:   planext.StatementStyleExact,
			columns: []schemamodel.Field{commentedNote()},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ADD COLUMN "note" text;

COMMENT ON COLUMN "tasks"."note" IS 'plain note';

`,
		},
		{
			name:    "a stored generated column, exact style",
			style:   planext.StatementStyleExact,
			columns: []schemamodel.Field{commentedSearchVector()},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ADD COLUMN "search_vector" tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(title, ''))) STORED;

COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v1';

`,
		},
		{
			name:    "both columns, the comments after both, annotated style",
			style:   planext.StatementStyleAnnotated,
			columns: []schemamodel.Field{commentedSearchVector(), commentedNote()},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ADD COLUMN "search_vector" tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(title, ''))) STORED;

ALTER TABLE "tasks" ADD COLUMN "note" text;

COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v1';

COMMENT ON COLUMN "tasks"."note" IS 'plain note';

`,
		},
		{
			name:    "a column without a comment",
			style:   planext.StatementStyleExact,
			columns: []schemamodel.Field{{StructName: "Task", Name: "note", Type: "text", Nullable: true}},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ADD COLUMN "note" text;

`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderStyledPlan(c, test.style, addedColumnsDiff(test.columns...)), qt.Equals, test.want)
		})
	}
}

// The control for the comments above: a table the declaration does not hold
// gets no statement, and no comment either. A COMMENT ON COLUMN there would
// apply to a relation nobody declared.
func TestPlanner_AddedColumnCommentOfAnUndeclaredTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := addedColumnsDiff(commentedNote())
	diff.DeclaredTables = []schemamodel.Table{{StructName: "Task", Name: "reporting.tasks"}}

	c.Assert(renderStyledPlan(c, planext.StatementStyleExact, diff), qt.Equals, "")
}

// generatedColumnChange changes the expression and the comment of the stored
// generated column search_vector at once.
func generatedColumnChange() *difftypes.SchemaDiff {
	desired := commentedSearchVector()
	desired.GeneratedExpression = "to_tsvector('english', coalesce(title, ''))"
	desired.Comment = "search:v2"
	return &difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
		TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "search_vector",
			Changes: map[string]string{
				"generated": "STORED to_tsvector('simple',coalesce(title,'')) -> STORED to_tsvector('english',coalesce(title,''))",
			},
			Desired:       desired,
			CommentChange: &difftypes.CommentChange{Current: "search:v1", Desired: "search:v2"},
		}}}},
	}
}

// TestPlanner_ModifiedGeneratedColumnComment_HappyPath pins the comment of a
// generated column whose expression changes in the same plan. The expression
// is a statement of its own, which does not carry the comment, so the comment
// is planned beside it. A target that cannot change the expression in place
// still gets the comment.
func TestPlanner_ModifiedGeneratedColumnComment_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want string
	}{
		{
			name: "postgresql 18 changes the expression in place",
			caps: capability.Postgres18(),
			want: `-- Add/modify columns for table: tasks
COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v2';

ALTER TABLE "tasks" ALTER COLUMN "search_vector" SET EXPRESSION AS (to_tsvector('english', coalesce(title, '')));

-- Modify generated column tasks.search_vector: STORED to_tsvector('simple',coalesce(title,'')) -> STORED to_tsvector('english',coalesce(title,''))
`,
		},
		{
			name: "postgresql 16 leaves the expression to a person",
			caps: capability.Postgres16(),
			want: `-- Add/modify columns for table: tasks
COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v2';

-- WARNING: Generated column tasks.search_vector changed, but ALTER COLUMN SET EXPRESSION requires target capability alter_generated_column_expression, unavailable on this target (PostgreSQL added it in 17); manual migration required.
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.NewWithCapabilities(test.caps).WithStatementStyle(planext.StatementStyleExact)

			nodes, err := planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), generatedColumnChange())
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, test.caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

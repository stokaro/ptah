//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// columnCommentStep is a database a migration directory starts from, a desired
// schema whose columns carry comments, and what PostgreSQL holds once the
// migration `migrate diff` writes for it is applied.
type columnCommentStep struct {
	name    string
	initial string
	desired string
	// wantChange is the migration `migrate diff` writes.
	wantChange string
	// wantComments is each column's comment as col_description reads it back,
	// in column order.
	wantComments []string
}

var columnCommentSteps = []columnCommentStep{
	{
		name:    "a plain and a stored generated column added with comments",
		initial: "CREATE TABLE tasks (id bigint PRIMARY KEY, title text);\n",
		desired: `CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  title text,
  note text,
  search_vector tsvector GENERATED ALWAYS AS (setweight(to_tsvector('simple', coalesce(title, '')), 'B')) STORED
);
COMMENT ON COLUMN tasks.note IS 'plain note';
COMMENT ON COLUMN tasks.search_vector IS 'search:v1:0df63add';
`,
		wantChange: `-- Modify "tasks" table
ALTER TABLE "tasks" ADD COLUMN "note" text NULL, ADD COLUMN "search_vector" tsvector NULL GENERATED ALWAYS AS (setweight(to_tsvector('simple', coalesce(title, '')), 'B')) STORED;
-- Set comment to column: "note" on table: "tasks"
COMMENT ON COLUMN "tasks"."note" IS 'plain note';
-- Set comment to column: "search_vector" on table: "tasks"
COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v1:0df63add';
`,
		wantComments: []string{"", "", "plain note", "search:v1:0df63add"},
	},
	{
		name: "a stored generated column whose expression and comment change",
		initial: `CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  title text,
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(title, ''))) STORED
);
COMMENT ON COLUMN tasks.search_vector IS 'search:v1';
`,
		desired: `CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  title text,
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, ''))) STORED
);
COMMENT ON COLUMN tasks.search_vector IS 'search:v2';
`,
		wantChange: `-- Modify "tasks" table
ALTER TABLE "tasks" ALTER COLUMN "search_vector" SET EXPRESSION AS (to_tsvector('english', coalesce(title, '')));
-- Set comment to column: "search_vector" on table: "tasks"
COMMENT ON COLUMN "tasks"."search_vector" IS 'search:v2';
`,
		wantComments: []string{"", "", "search:v2"},
	},
	{
		// The community binary writes this change as one ALTER TABLE, the index, the
		// table's comment and then its columns' comments in column order. A column
		// comment left between two of the table's column changes splits the ALTER
		// TABLE in two.
		name: "columns added, dropped and changed beside the comments of the table and a column",
		initial: `CREATE TABLE tasks (id bigint PRIMARY KEY, title text, old text, body text);
COMMENT ON TABLE tasks IS 't1';
COMMENT ON COLUMN tasks.title IS 'v1';
`,
		desired: `CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  title text NOT NULL,
  body text,
  due date,
  owner text
);
COMMENT ON TABLE tasks IS 't2';
COMMENT ON COLUMN tasks.title IS 'v2';
COMMENT ON COLUMN tasks.owner IS 'who';
CREATE INDEX tasks_owner_idx ON tasks (owner);
`,
		wantChange: `-- POSTGRES: SET NOT NULL fails if any row of "tasks" holds NULL in "title"; the column declares no default to fill it with.
-- Modify "tasks" table
ALTER TABLE "tasks" ALTER COLUMN "title" SET NOT NULL, DROP COLUMN "old", ADD COLUMN "due" date NULL, ADD COLUMN "owner" text NULL;
-- Create index "tasks_owner_idx" to table: "tasks"
CREATE INDEX "tasks_owner_idx" ON "tasks" ("owner");
-- Set comment to table: "tasks"
COMMENT ON TABLE "tasks" IS 't2';
-- Set comment to column: "title" on table: "tasks"
COMMENT ON COLUMN "tasks"."title" IS 'v2';
-- Set comment to column: "owner" on table: "tasks"
COMMENT ON COLUMN "tasks"."owner" IS 'who';
`,
		wantComments: []string{"", "v2", "", "", "who"},
	},
}

// TestMigrateDiffCarriesTheCommentsOfTheColumnsItChangesE2E runs the shipped
// compatibility path over a column comment: `migrate diff` writes the migration,
// `migrate apply` runs it on PostgreSQL, the server holds every comment the
// schema declares, and the next `migrate diff` finds the directory synced. A
// plan that adds a column without its COMMENT ON COLUMN, or changes a generated
// column's expression without its comment, converges only in a second
// migration that sets the comment alone.
func TestMigrateDiffCarriesTheCommentsOfTheColumnsItChangesE2E(t *testing.T) {
	for _, test := range columnCommentSteps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20200101000000_init.sql"), []byte(test.initial), 0o600), qt.IsNil)
			schema := filepath.Join(work, "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)
			dirURL, schemaURL := "file://"+filepath.ToSlash(dir), "file://"+filepath.ToSlash(schema)
			hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
			dev, _ := scratchReplayDatabase(c)
			target, _ := scratchReplayDatabase(c)

			diffed, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL, "--to", schemaURL, "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", diffed))
			c.Assert(filesMatching(c, dir, "*_change.sql"), qt.Equals, test.wantChange)
			applied, err := runCompatVerb("migrate", "apply", "--dir", dirURL, "--url", target)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			again, err := runCompatVerb("migrate", "diff", "--dir", dirURL, "--to", schemaURL, "--dev-url", dev, "--dry-run")

			c.Assert(heldColumnComments(c, target, "tasks"), qt.DeepEquals, test.wantComments)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
			c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// heldColumnComments is the comment of each column of table, in column order,
// as the server reports it. A column without one reads as the empty string.
func heldColumnComments(c *qt.C, targetURL, table string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), `SELECT coalesce(col_description(a.attrelid, a.attnum), '')
FROM pg_attribute AS a
WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	comments := make([]string, 0, 4)
	for rows.Next() {
		var comment string
		c.Assert(rows.Scan(&comment), qt.IsNil)
		comments = append(comments, comment)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return comments
}

// addedColumnCommentEngine is one engine `schema apply` adds commented columns
// on: a database of the test's own, the schema it starts from and the one that
// adds the columns, and a query answering every column's comment in the order
// of the column names, a dash for none.
type addedColumnCommentEngine struct {
	name     string
	target   func(c *qt.C) string
	file     string
	initial  string
	desired  string
	comments string
	want     string
}

var addedColumnCommentEngines = []addedColumnCommentEngine{
	{
		name: "postgresql",
		target: func(c *qt.C) string {
			target, _ := scratchReplayDatabase(c)
			return target
		},
		file:    "schema.sql",
		initial: "CREATE TABLE tasks (id bigint PRIMARY KEY, title text);\n",
		desired: `CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  title text,
  note text,
  code text GENERATED ALWAYS AS (upper(title)) STORED
);
COMMENT ON COLUMN tasks.note IS 'plain note';
COMMENT ON COLUMN tasks.code IS 'derived code';
`,
		comments: `SELECT string_agg(coalesce(col_description(attrelid, attnum), '-'), ',' ORDER BY attname)
FROM pg_attribute WHERE attrelid = 'tasks'::regclass AND attnum > 0 AND NOT attisdropped`,
		want: "derived code,-,plain note,-",
	},
	{
		name: "sqlserver",
		target: func(c *qt.C) string {
			target, _ := declaredSQLServerDatabase(c)
			return target
		},
		file:    "schema.hcl",
		initial: sqlServerTasksDocument(""),
		desired: sqlServerTasksDocument("  column \"note\" {\n    type = nvarchar(64)\n    null = true\n    comment = \"plain note\"\n  }\n"),
		comments: `SELECT STRING_AGG(COALESCE(CAST(ep.value AS nvarchar(4000)), N'-'), ',') WITHIN GROUP (ORDER BY c.name)
FROM sys.columns AS c
LEFT JOIN sys.extended_properties AS ep
  ON ep.class = 1 AND ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = N'MS_Description'
WHERE c.object_id = OBJECT_ID(N'dbo.tasks')`,
		want: "-,plain note,-",
	},
	{
		name:    "oracle",
		target:  oracleScratchURL,
		file:    "schema.sql",
		initial: "CREATE TABLE tasks (id NUMBER(10) PRIMARY KEY, title VARCHAR2(64));\n",
		desired: `CREATE TABLE tasks (
  id NUMBER(10) PRIMARY KEY,
  title VARCHAR2(64),
  note VARCHAR2(64),
  code VARCHAR2(64) GENERATED ALWAYS AS (UPPER(title)) VIRTUAL
);
COMMENT ON COLUMN tasks.note IS 'plain note';
COMMENT ON COLUMN tasks.code IS 'derived code';
`,
		comments: `SELECT LISTAGG(NVL(cc.comments, '-'), ',') WITHIN GROUP (ORDER BY tc.column_name)
FROM user_tab_columns tc
LEFT JOIN user_col_comments cc ON cc.table_name = tc.table_name AND cc.column_name = tc.column_name
WHERE tc.table_name = 'TASKS'`,
		want: "derived code,-,plain note,-",
	},
}

// sqlServerTasksDocument is the tasks table in dbo with columns after its
// own two.
func sqlServerTasksDocument(columns string) string {
	return "schema \"dbo\" {\n}\n\ntable \"tasks\" {\n  schema = schema.dbo\n" +
		"  column \"id\" {\n    type = int\n    null = false\n  }\n" +
		"  column \"title\" {\n    type = nvarchar(64)\n    null = true\n  }\n" +
		columns +
		"  primary_key {\n    columns = [column.id]\n  }\n}\n"
}

// TestSchemaApplyWritesTheCommentOfAnAddedColumnE2E adds commented columns with
// the native `schema apply` on each engine that keeps a column comment outside
// the column definition, reads the comments back from the catalog, and plans
// once more: the plan is empty. A plan that adds the column bare leaves the
// comment to the next run.
func TestSchemaApplyWritesTheCommentOfAnAddedColumnE2E(t *testing.T) {
	for _, test := range addedColumnCommentEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.target(c)
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, test.file, test.initial), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			file := writeDeclaredSchemaFile(c, test.file, test.desired)

			added := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(added.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", added.Stdout, added.Stderr))
			c.Assert(heldColumns(c, target, test.comments), qt.Equals, test.want)
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

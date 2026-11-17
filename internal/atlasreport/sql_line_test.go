package atlasreport_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasreport"
)

// TestMigrateDiffWithoutAnIndentWritesEachStatementOnOneLine_HappyPath pins the
// layout the pinned community binary writes when the `sql` helper is given no
// indent: each statement it plans on one line, a line break becoming a space,
// or nothing after an opening parenthesis and before a closing one. A line
// break inside a quoted token or at the end of a line comment stays, and a
// statement only Ptah plans keeps the renderer's layout.
func TestMigrateDiffWithoutAnIndentWritesEachStatementOnOneLine_HappyPath(t *testing.T) {
	tests := []struct{ name, dialect, sql, want string }{
		{
			name: "a new table", dialect: platform.Postgres,
			sql:  "CREATE TABLE \"u\" (\n  \"id\" integer NOT NULL,\n  \"note\" text NULL,\n  PRIMARY KEY (\"id\")\n);",
			want: "CREATE TABLE \"u\" (\"id\" integer NOT NULL, \"note\" text NULL, PRIMARY KEY (\"id\"));\n",
		},
		{
			name: "a constraint on a line of its own", dialect: platform.Postgres,
			sql:  "ALTER TABLE \"c\" ADD\nCONSTRAINT \"c_fk\" FOREIGN KEY (\"p_id\") REFERENCES \"p\" (\"id\");",
			want: "ALTER TABLE \"c\" ADD CONSTRAINT \"c_fk\" FOREIGN KEY (\"p_id\") REFERENCES \"p\" (\"id\");\n",
		},
		{
			name: "a table on SQLite", dialect: platform.SQLite,
			sql:  "CREATE TABLE `t` (\n  `id` integer NULL,\n  PRIMARY KEY (`id`)\n);",
			want: "CREATE TABLE `t` (`id` integer NULL, PRIMARY KEY (`id`));\n",
		},
		{
			name: "a string", dialect: platform.Postgres,
			sql:  "CREATE TABLE t (\n  note text DEFAULT 'first\n  second'\n);",
			want: "CREATE TABLE t (note text DEFAULT 'first\n  second');\n",
		},
		{
			name: "a quoted identifier", dialect: platform.Postgres,
			sql:  "CREATE TABLE \"first\n  second\" (\n  id int\n);",
			want: "CREATE TABLE \"first\n  second\" (id int);\n",
		},
		{
			name: "a backtick identifier", dialect: platform.MySQL,
			sql:  "CREATE TABLE `first\n  second` (\n  id int\n);",
			want: "CREATE TABLE `first\n  second` (id int);\n",
		},
		{
			name: "a line comment", dialect: platform.Postgres,
			sql:  "CREATE TABLE t (\n  id int, -- the key\n  note text\n);",
			want: "CREATE TABLE t (id int, -- the key\n  note text);\n",
		},
		{
			name: "a view", dialect: platform.Postgres,
			sql:  "CREATE VIEW v AS\nSELECT id\nFROM t;",
			want: "CREATE VIEW v AS\nSELECT id\nFROM t;\n",
		},
		{
			name: "declared rows", dialect: platform.Postgres,
			sql:  "INSERT INTO t (id) VALUES\n  (1),\n  (2);",
			want: "INSERT INTO t (id) VALUES\n  (1),\n  (2);\n",
		},
		{
			name: "a trigger", dialect: platform.MySQL,
			sql:  "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW\nSET NEW.a = 1;",
			want: "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW\nSET NEW.a = 1;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: test.dialect, Statements: []string{test.sql}})

			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.String(), qt.Equals, test.want)
		})
	}
}

// TestMigrateDiffDefaultFormatWritesEachStatementOnOneLine_HappyPath pins the
// file a migration diff without --format writes, and the one an empty indent
// writes, to the one-line layout the community binary writes for both.
func TestMigrateDiffDefaultFormatWritesEachStatementOnOneLine_HappyPath(t *testing.T) {
	const want = "-- Create \"t\" table\nCREATE TABLE \"t\" (\"id\" integer NOT NULL, PRIMARY KEY (\"id\"));\n"
	for _, format := range []string{atlasreport.NormalizeMigrateDiffFormat(""), `{{ sql . "" }}`} {
		t.Run(format, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
				Dialect: platform.Postgres,
				Changes: []atlasreport.SchemaChange{{Cmd: "CREATE TABLE \"t\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);", Comment: `create "t" table`}},
			})

			err := atlasreport.WriteMigrateDiff(&output, format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.String(), qt.Equals, want)
		})
	}
}

// TestSchemaDiffCmdHoldsTheStatementOnOneLine_HappyPath pins `.Cmd` to the
// one-line statement, as the community binary's template data holds it, while
// [atlasreport.SchemaDiff.Statements] keeps the renderer's layout.
func TestSchemaDiffCmdHoldsTheStatementOnOneLine_HappyPath(t *testing.T) {
	c := qt.New(t)
	const laidOut = "CREATE TABLE \"t\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n)"
	var output bytes.Buffer
	report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: platform.Postgres, Statements: []string{laidOut + ";"}})

	err := atlasreport.WriteMigrateDiff(&output, `{{ range .Changes }}{{ .Cmd }};{{ "\n" }}{{ end }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Equals, "CREATE TABLE \"t\" (\"id\" integer NOT NULL, PRIMARY KEY (\"id\"));\n")
	c.Assert(report.Statements(), qt.DeepEquals, []string{laidOut})
}

// TestNativeSchemaDiffFormatKeepsTheRenderersLayout_HappyPath pins the template
// native `ptah schema diff` renders: the columns of a CREATE TABLE one per line,
// where the default `schema diff` template writes the statement on one line.
func TestNativeSchemaDiffFormatKeepsTheRenderersLayout_HappyPath(t *testing.T) {
	c := qt.New(t)
	const laidOut = "CREATE TABLE \"t\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);"
	report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: platform.Postgres, Statements: []string{laidOut}})
	var native, compat bytes.Buffer

	nativeErr := atlasreport.WriteSchemaDiff(context.Background(), &native, atlasreport.NativeSchemaDiffFormat, report, schemaext.Registry{})
	compatErr := atlasreport.WriteSchemaDiff(context.Background(), &compat, atlasreport.NormalizeSchemaDiffFormat(""), report, schemaext.Registry{})

	c.Assert(nativeErr, qt.IsNil)
	c.Assert(native.String(), qt.Equals, laidOut+"\n")
	c.Assert(compatErr, qt.IsNil)
	c.Assert(compat.String(), qt.Equals, "CREATE TABLE \"t\" (\"id\" integer NOT NULL, PRIMARY KEY (\"id\"));\n")
}

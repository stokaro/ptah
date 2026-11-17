package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
)

// TestInspect_SQLCreationPlan_HappyPath writes, for the `sql` helper, the plan
// that creates the inspected schema on a new database, as the pinned community
// binary v1.3.0 does: a change comment over each statement, the foreign key
// inside the CREATE TABLE of the table that holds it, and that table after the
// one it references. The bytes are those of `schema diff` from an empty
// database to this one.
func TestInspect_SQLCreationPlan_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "one statement a line",
			format: `{{ sql . }}`,
			want: "-- Create \"users\" table\n" +
				"CREATE TABLE \"users\" (\"id\" integer NULL, \"email\" text NOT NULL, PRIMARY KEY (\"id\"));\n" +
				"-- Create index \"users_email_key\" to table: \"users\"\n" +
				"CREATE UNIQUE INDEX \"users_email_key\" ON \"users\" (\"email\");\n" +
				"-- Create \"posts\" table\n" +
				"CREATE TABLE \"posts\" (\"id\" integer NULL, \"user_id\" integer NOT NULL CONSTRAINT \"posts_user_fk\" REFERENCES \"users\" (\"id\") ON DELETE NO ACTION ON UPDATE NO ACTION, \"title\" text NOT NULL, PRIMARY KEY (\"id\"));\n",
		},
		{
			name:   "an indent lays each statement out",
			format: `{{ sql . "  " }}`,
			want: "-- Create \"users\" table\n" +
				"CREATE TABLE \"users\" (\n  \"id\" integer NULL,\n  \"email\" text NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n" +
				"-- Create index \"users_email_key\" to table: \"users\"\n" +
				"CREATE UNIQUE INDEX \"users_email_key\" ON \"users\" (\"email\");\n" +
				"-- Create \"posts\" table\n" +
				"CREATE TABLE \"posts\" (\n  \"id\" integer NULL,\n" +
				"  \"user_id\" integer NOT NULL CONSTRAINT \"posts_user_fk\" REFERENCES \"users\" (\"id\") ON DELETE NO ACTION ON UPDATE NO ACTION,\n" +
				"  \"title\" text NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-plan.db"))
			defer dbschema.CloseAndWarn(conn)
			createInspectSchema(c, conn)
			runtime := inspectFeatureRuntime(c)

			got, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
				Runtime: runtime, Format: test.format, SQLCreationPlan: runtime,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got.Rendered, qt.Equals, test.want)
		})
	}
}

// TestInspect_SQLCreationPlan_EmptyDatabase writes no bytes for a database
// holding nothing, as the community binary writes none.
func TestInspect_SQLCreationPlan_EmptyDatabase(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-empty.db"))
	defer dbschema.CloseAndWarn(conn)
	runtime := inspectFeatureRuntime(c)

	got, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Runtime: runtime, Format: `{{ sql . }}`, SQLCreationPlan: runtime,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Rendered, qt.Equals, "")
}

// TestInspect_SQLWithoutCreationPlan writes the rendered schema, the native
// answer, when the caller asks for no creation plan.
func TestInspect_SQLWithoutCreationPlan(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-render.db"))
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c, conn)

	got, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Runtime: inspectFeatureRuntime(c), Format: `{{ printf "%.15s" (sql .) }}`,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Rendered, qt.Equals, `CREATE TABLE "p`)
}

package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

func TestInspect_HappyPathHCL(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-hcl.db"))
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c, conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "users"`)
	c.Assert(rendered, qt.Contains, `column "email"`)
}

func TestInspect_HappyPathCustomTemplate(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-template.db"))
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c, conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format: `{{ len .Realm.Schemas }}/{{ len (index .Schema.Schemas 0).Tables }}/{{ base64url "a+b/c=" }}/{{ printf "%.6s" (sql .) }}`,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, "1/2/a-b_c/CREATE")
}

func TestInspect_ExcludeFilter(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "inspect-exclude.db"))
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c, conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format:  "hcl",
		Exclude: []string{"posts"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "users"`)
	c.Assert(rendered, qt.Not(qt.Contains), `table "posts"`)
	c.Assert(rendered, qt.Not(qt.Contains), `posts_user_fk`)
}

func TestInspect_IncludeSelection(t *testing.T) {
	c := qt.New(t)

	c.Run("selects the named table and its children", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "inspect-include.db"))
		defer dbschema.CloseAndWarn(conn)
		createInspectSchema(c, conn)

		rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
			Format:  "hcl",
			Include: []string{"users"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(rendered, qt.Contains, `table "users"`)
		c.Assert(rendered, qt.Contains, `column "email"`)
		c.Assert(rendered, qt.Contains, `users_email_key`)
		c.Assert(rendered, qt.Not(qt.Contains), `table "posts"`)
	})

	c.Run("refuses a selection that drops a dependency", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "inspect-include-dep.db"))
		defer dbschema.CloseAndWarn(conn)
		createInspectSchema(c, conn)

		rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
			Format:  "hcl",
			Include: []string{"posts"},
		})

		c.Assert(err, qt.ErrorMatches, `(?s)the --schema/--include selection drops objects.*`)
		c.Assert(rendered, qt.Equals, "")
	})

	c.Run("exclusion-only inspection is unchanged", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "inspect-include-none.db"))
		defer dbschema.CloseAndWarn(conn)
		createInspectSchema(c, conn)

		withoutInclude, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
			Format:  "hcl",
			Exclude: []string{"posts"},
		})
		c.Assert(err, qt.IsNil)

		withEmptyInclude, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
			Format:  "hcl",
			Exclude: []string{"posts"},
			Include: []string{"", " "},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(withEmptyInclude, qt.Equals, withoutInclude)
		c.Assert(withoutInclude, qt.Contains, `table "users"`)
		c.Assert(withoutInclude, qt.Not(qt.Contains), `table "posts"`)
	})
}

func TestInspect_FailurePath(t *testing.T) {
	c := qt.New(t)

	t.Run("invalid format before connection", func(t *testing.T) {
		c := qt.New(t)
		rendered, err := atlasschema.Inspect(context.Background(), nil, atlasschema.InspectOptions{
			Format: "{{ if }}",
		})
		c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
		c.Assert(rendered, qt.Equals, "")
	})

	t.Run("nil connection", func(t *testing.T) {
		c := qt.New(t)
		rendered, err := atlasschema.Inspect(context.Background(), nil, atlasschema.InspectOptions{})
		c.Assert(err, qt.ErrorMatches, "schema inspect requires database connection")
		c.Assert(rendered, qt.Equals, "")
	})

	c.Run("dev url dialect mismatch", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "inspect-mismatch.db"))
		defer dbschema.CloseAndWarn(conn)

		rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
			DevURL: "postgres://localhost/dev",
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "postgres" does not match --url dialect "sqlite"`)
		c.Assert(rendered, qt.Equals, "")
	})
}

func TestSplitSchemaNames(t *testing.T) {
	c := qt.New(t)

	schemas := atlasschema.SplitSchemaNames([]string{"public, auth", "tenant"})

	c.Assert(schemas, qt.DeepEquals, []string{"public", "auth", "tenant"})
}

func createInspectSchema(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	_, err := conn.ExecContext(context.Background(), `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
);
CREATE UNIQUE INDEX users_email_key ON users (email);
`)
	c.Assert(err, qt.IsNil)
}

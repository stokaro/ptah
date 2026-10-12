//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A view's attributes are SQL Server's WITH clause, and PostgreSQL has none.
// A view created without them is not the one declared, and since the
// comparison compares the attributes and the read reports none, every diff
// would replace it again. So the plan refuses the view by name.

// viewAttributesDocument declares t and the view v over it, with attributes
// when there are any.
func viewAttributesDocument(attributes string) string {
	return `schema "public" {}

table "t" {
  schema = schema.public
  column "id" {
    type = bigint
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}

view "v" {
  schema = schema.public
  as     = "SELECT id FROM t"
` + attributes + `}
`
}

// postgresViewAttributesDatabase is a scratch PostgreSQL database.
func postgresViewAttributesDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	return devDialectDatabase{url: devURL, conn: connectDevDialect(c, devURL)}
}

// countedViews counts the views called v in the current schema.
func countedViews(c *qt.C, dev devDialectDatabase) int {
	c.Helper()
	var views int
	err := dev.conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM information_schema.views WHERE table_schema = current_schema() AND table_name = 'v'",
	).Scan(&views)
	c.Assert(err, qt.IsNil)
	return views
}

// writtenViewDocument writes a document declaring v with attributes, and
// returns its path.
func writtenViewDocument(c *qt.C, attributes string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(viewAttributesDocument(attributes)), 0o600), qt.IsNil)
	return path
}

// The apply refuses the view and creates nothing, and the diff of a database
// whose view was created without the attribute refuses rather than planning
// the replacement it would plan on every run.
func TestSchemaApplyViewAttributesPostgresE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	dev := postgresViewAttributesDatabase(c)
	bound := writtenViewDocument(c, `  attributes = ["SCHEMABINDING"]`+"\n")
	const refusal = `(?s).*unsupported feature: postgres: view public\.v declares WITH SCHEMABINDING, ` +
		`which only SQL Server has; a view created without it is not the view declared.*`

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", bound, "--auto-approve")
	c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("%s", out))
	c.Assert(countedViews(c, dev), qt.Equals, 0)

	out, err = runPtahNativeWithError("schema", "apply", "--db-url", dev.url,
		"--schema-file", writtenViewDocument(c, ""), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", bound)
	c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("%s", out))
}

// The control: the same view without attributes is created, and a second
// diff is in sync.
func TestSchemaApplyViewAttributesPostgresE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	dev := postgresViewAttributesDatabase(c)
	plain := writtenViewDocument(c, "")

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", plain, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(countedViews(c, dev), qt.Equals, 1)

	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", plain)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
}

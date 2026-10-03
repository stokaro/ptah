//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// A `data "composite_schema"` block assembles a desired state from parts, each
// placed in the schema its label names. ariga/atlas#3807 builds its desired
// state this way, from an HCL directory and a SQL directory. The community
// binary v1.3.0 has no handler for the data source, so strict compatibility
// keeps its refusal; the default surface evaluates the block.

// compositeAuthHCL declares the auth schema and its users table in Atlas HCL.
const compositeAuthHCL = `schema "auth" {}

table "users" {
  schema = schema.auth
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// compositePublicSQL is unqualified, so it lands in public, the label of its
// part and the run's default schema, and references the HCL part's table.
const compositePublicSQL = `CREATE TABLE posts (
  id int PRIMARY KEY,
  author_id int REFERENCES auth.users (id)
);
`

// writeCompositeProject writes the two parts, an empty migration directory and
// an atlas.hcl whose env "dev" takes the composition as its desired state. It
// returns the path of the atlas.hcl.
func writeCompositeProject(c *qt.C, targetURL, devURL string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "auth.hcl"), []byte(compositeAuthHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "public.sql"), []byte(compositePublicSQL), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
	config := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(config, []byte(`data "composite_schema" "project" {
  schema "auth" {
    url = "file://auth.hcl"
  }
  schema "public" {
    url = "file://public.sql"
  }
}

env "dev" {
  url = "`+targetURL+`"
  dev = "`+devURL+`"
  migration {
    dir = "file://migrations"
  }
  schema {
    src = data.composite_schema.project.url
  }
}
`), 0o600), qt.IsNil)
	return config
}

// TestCompatMigrateDiffComposesSchemaPartsE2E writes a migration from the
// composition, applies it to an empty target, and reads the target back: both
// tables exist in the schemas their parts name, and the foreign key crosses
// them. A second diff over the applied directory plans nothing.
func TestCompatMigrateDiffComposesSchemaPartsE2E(t *testing.T) {
	c := qt.New(t)
	devURL, _ := scratchReplayDatabase(c)
	targetURL, _ := scratchReplayDatabase(c)
	config := writeCompositeProject(c, targetURL, devURL)
	project := []string{"--config", "file://" + filepath.ToSlash(config), "--env", "dev"}

	out, err := runCompatVerb(append([]string{"migrate", "diff"}, append(project, "init")...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runCompatVerb(append([]string{"migrate", "apply"}, project...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	again, err := runCompatVerb(append([]string{"migrate", "diff"}, append(project, "again")...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(strings.TrimSpace(again), qt.Equals,
		"The migration directory is synced with the desired state, no changes to be made")
	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	var tables, references string
	c.Assert(target.QueryRowContext(c.Context(), `
		SELECT string_agg(table_schema || '.' || table_name, ',' ORDER BY table_schema, table_name)
		FROM information_schema.tables
		WHERE table_schema IN ('auth', 'public') AND table_type = 'BASE TABLE'`).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, "auth.users,public.posts")
	c.Assert(target.QueryRowContext(c.Context(), `
		SELECT confrelid::regclass::text
		FROM pg_constraint
		WHERE contype = 'f' AND conrelid = 'public.posts'::regclass`).Scan(&references), qt.IsNil)
	c.Assert(references, qt.Equals, "auth.users")
}

// TestCompatMigrateDiffRefusesCompositeSchemaUnderStrictCompatE2E is the
// community binary's answer, which strict compatibility keeps: exit 1 and
// `missing data source handler for "composite_schema"`, before anything is
// connected, and no migration file.
func TestCompatMigrateDiffRefusesCompositeSchemaUnderStrictCompatE2E(t *testing.T) {
	c := qt.New(t)
	config := writeCompositeProject(c, "postgres://127.0.0.1:1/target", "postgres://127.0.0.1:1/dev")
	cmd := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "diff", "--config", "file://" + filepath.ToSlash(config), "--env", "dev", "init"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `missing data source handler for "composite_schema"`, qt.Commentf("%s", out.String()))
	entries, readErr := os.ReadDir(filepath.Join(filepath.Dir(config), "migrations"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

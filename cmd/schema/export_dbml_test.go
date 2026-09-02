package schema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// dbmlModel is a schema with a key, a unique column, a foreign key and a view,
// so the export can be checked against what DBML carries AND against what it
// has to say it cannot.
const dbmlModel = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string
}

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="author_id" type="BIGINT" not_null="true" foreign="users(id)" foreign_key_name="posts_author_fk"
	AuthorID int64
}
`

func writeDBMLModel(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(dbmlModel), 0o600), qt.IsNil)
	return dir
}

// TestSchemaExportDBMLWritesTheDocument is the acceptance check for the target.
func TestSchemaExportDBMLWritesTheDocument(t *testing.T) {
	c := qt.New(t)
	dir := writeDBMLModel(c)

	stdout, stderr, err := runSchemaExport("--to", "dbml", "--root-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, `Table "posts" {`)
	c.Assert(stdout, qt.Contains, `Table "users" {`)
	c.Assert(stdout, qt.Contains, `"email" VARCHAR(255) [unique, not null]`)
	c.Assert(stdout, qt.Contains, `Ref "posts_author_fk": "posts"."author_id" > "users"."id"`)
	c.Assert(strings.HasSuffix(stdout, "\n"), qt.IsTrue)
}

// TestSchemaExportDBMLIsByteIdenticalAcrossRuns pins the canonical contract
// through the command rather than only in the renderer, because a target that
// stamped a timestamp or a host into its output would satisfy the renderer's
// own test and break every diff of an exported file.
func TestSchemaExportDBMLIsByteIdenticalAcrossRuns(t *testing.T) {
	c := qt.New(t)
	dir := writeDBMLModel(c)

	first, _, err := runSchemaExport("--to", "dbml", "--root-dir", dir)
	c.Assert(err, qt.IsNil)
	second, _, err := runSchemaExport("--to", "dbml", "--root-dir", dir)
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.Equals, first)
}

// TestSchemaExportDBMLSaysWhatItCannotCarry pins the warning, and pins that it
// reaches stderr rather than the document.
//
// A caller redirecting stdout to a file has the file by the time anything else
// is printed, so a loss reported into stdout would be a loss reported into the
// artifact.
func TestSchemaExportDBMLSaysWhatItCannotCarry(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	model := dbmlModel + `
//ptah:schema:view name="recent_posts" body="SELECT id FROM posts"
type RecentPosts struct{}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(model), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExport("--to", "dbml", "--root-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, "DBML cannot express views (1)")
	c.Assert(stdout, qt.Not(qt.Contains), "recent_posts")
	c.Assert(stdout, qt.Not(qt.Contains), "cannot express")
}

// TestSchemaExportDBMLRefusesContractMetadataBeforeWriting proves the command
// cannot leave a plausible but contract-incomplete DBML file behind. The
// existing file is the control for ordering: validation must happen before the
// output path is opened or truncated.
func TestSchemaExportDBMLRefusesContractMetadataBeforeWriting(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	model := strings.Replace(
		dbmlModel,
		`//ptah:schema:table name="users"`,
		`//ptah:schema:table name="users" api_name="Account"`,
		1,
	)
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(model), 0o600), qt.IsNil)
	outPath := filepath.Join(dir, "schema.dbml")
	before := []byte("keep this file\n")
	c.Assert(os.WriteFile(outPath, before, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExport(
		"--to", "dbml", "--root-dir", dir, "--out", outPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "DBML cannot represent API export metadata without loss")
	c.Assert(stdout, qt.Equals, "")
	after, readErr := os.ReadFile(outPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(after, qt.DeepEquals, before)
}

// TestSchemaExportRefusesAnUnknownTargetAndNamesDBML keeps the refusal in step
// with what the command accepts. A target wired in and left out of that
// sentence reads as unsupported to whoever hits the error.
func TestSchemaExportRefusesAnUnknownTargetAndNamesDBML(t *testing.T) {
	c := qt.New(t)
	dir := writeDBMLModel(c)

	_, stderr, err := runSchemaExport("--to", "nonsense", "--root-dir", dir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "dbml")
}

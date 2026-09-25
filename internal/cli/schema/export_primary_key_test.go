package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// primaryKeySchema is the SQL shape that read as nullable: a column key written
// without NOT NULL, and a key declared at table level.
const primaryKeySchema = `CREATE TABLE notes (
    id   BIGINT PRIMARY KEY,
    body TEXT
);
CREATE TABLE memberships (
    org_id  BIGINT,
    user_id BIGINT,
    PRIMARY KEY (org_id, user_id)
);
`

// exportDocument runs `ptah schema export` over primaryKeySchema to one
// document target and returns what it wrote.
func exportDocument(c *qt.C, target string) string {
	c.Helper()
	dir := c.TempDir()
	source := filepath.Join(dir, "schema.sql")
	out := filepath.Join(dir, "schema."+target)
	c.Assert(os.WriteFile(source, []byte(primaryKeySchema), 0o600), qt.IsNil)

	_, stderr, err := runSchemaExport("--from", "sql", "--to", target, "--schema-file", source, "--out", out)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	data, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	return string(data)
}

// TestSchemaExportMarkdownReadsAPrimaryKeyColumnAsNotNull drives the whole path
// from a SQL schema file: the reader leaves a key column written without NOT
// NULL nullable, and the document has to say what the rendered DDL does.
func TestSchemaExportMarkdownReadsAPrimaryKeyColumnAsNotNull(t *testing.T) {
	c := qt.New(t)

	doc := exportDocument(c, "markdown")

	c.Assert(doc, qt.Contains, "| id | BIGINT | no | — | PK | — |")
	c.Assert(doc, qt.Contains, "| org_id | BIGINT | no | — | PK | — |")
	c.Assert(doc, qt.Contains, "| user_id | BIGINT | no | — | PK | — |")
	c.Assert(doc, qt.Contains, "| body | TEXT | yes | — | — | — |")
}

// TestSchemaExportHTMLReadsAPrimaryKeyColumnAsNotNull is the same path to the
// HTML page: the key column carries the primary tag and no null tag, and the
// ordinary nullable column keeps its null tag.
func TestSchemaExportHTMLReadsAPrimaryKeyColumnAsNotNull(t *testing.T) {
	c := qt.New(t)

	page := exportDocument(c, "html")

	c.Assert(page, qt.Contains,
		`<td class="name">id</td><td class="type">BIGINT</td><td><span class="none">—</span></td><td><span class="tag key">primary</span></td>`)
	c.Assert(page, qt.Contains,
		`<td class="name">org_id</td><td class="type">BIGINT</td><td><span class="none">—</span></td><td><span class="tag key">primary</span></td>`)
	c.Assert(page, qt.Contains,
		`<td class="name">body</td><td class="type">TEXT</td><td><span class="tag null">null</span></td>`)
}

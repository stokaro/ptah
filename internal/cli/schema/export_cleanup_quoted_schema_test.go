package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaExportCleanupRemovesAnObjectInAQuotedSchema_HappyPath exports, with
// the cleanup, a model whose second object names its schema as one quoted
// identifier, the spelling the refusal of a spaced bare schema asks for. The
// frontend reads the schema as the name inside the quotes, and the cleanup
// compares the tag with the export as the frontend read it, so the tag is
// removed and the HCL holds the object in that schema. Read raw, the cleanup
// would compare "App" in quotes with App and refuse the tag.
func TestSchemaExportCleanupRemovesAnObjectInAQuotedSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name, directive, want string
	}{
		{
			name:      "a domain in a schema with a capital",
			directive: `//ptah:schema:domain name="d" type="integer" schema="\"App\""`,
			want:      "domain \"d\" {\n  schema = schema.App\n",
		},
		{
			name:      "a domain in a schema beginning with a space",
			directive: `//ptah:schema:domain name="d" type="integer" schema="\" app\""`,
			want:      "domain \"d\" {\n  schema = schema[\" app\"]\n",
		},
		{
			name:      "a range",
			directive: `//ptah:schema:range name="r" subtype="integer" schema="\"App\""`,
			want:      "range \"r\" {\n  schema = schema.App\n",
		},
		{
			name:      "an extension",
			directive: `//ptah:schema:extension name="pgcrypto" schema="\"App\""`,
			want:      "extension \"pgcrypto\" {\n  schema = schema.App\n",
		},
		{
			name:      "a schema",
			directive: `//ptah:schema:schema name="\"App\""`,
			want:      "schema \"App\" {\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			model := filepath.Join(dir, "model.go")
			c.Assert(os.WriteFile(model, []byte(`package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}

`+test.directive+`
type D struct{}
`), 0o600), qt.IsNil)
			out := filepath.Join(dir, "schema.hcl")

			stdout, stderr, err := runSchemaExportCleanup(c, "--to", "hcl", "--root-dir", dir, "--out", out,
				"--cleanup-go-annotations")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Contains, "Cleaned 1 file(s), removed 3 annotation line(s)")
			exported, err := os.ReadFile(out)
			c.Assert(err, qt.IsNil)
			c.Assert(string(exported), qt.Contains, test.want)
			cleaned, err := os.ReadFile(model)
			c.Assert(err, qt.IsNil)
			c.Assert(string(cleaned), qt.Equals, "package models\n\ntype T struct {\n\tID int\n}\n\ntype D struct{}\n")
		})
	}
}

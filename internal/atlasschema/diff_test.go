package atlasschema

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestDiffLocalFilesReturnsSchemaDiffReport(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`
table "users" {
  column "id" {
    type = int
  }
  column "email" {
    null = false
    type = varchar(255)
  }
}
`), 0o600), qt.IsNil)

	report, err := DiffLocalFiles(DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + to},
		DevURL:   "postgres://localhost/dev",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 1)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
}

func TestDiffLocalFilesRequiresDevURL(t *testing.T) {
	c := qt.New(t)

	_, err := DiffLocalFiles(DiffOptions{})

	c.Assert(err, qt.ErrorMatches, `--dev-url is required for local schema file diffing`)
}

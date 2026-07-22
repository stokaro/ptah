package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasschema"
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

	report, err := atlasschema.DiffLocalFiles(atlasschema.DiffOptions{
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

func TestDiffLocalFiles_ExcludeFilter(t *testing.T) {
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
table "audit_logs" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	report, err := atlasschema.DiffLocalFiles(atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + to},
		DevURL:   "postgres://localhost/dev",
		Exclude:  []string{"audit_logs"},
	})

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
	c.Assert(sql, qt.Not(qt.Contains), "audit_logs")
}

func TestDiffLocalFiles_ExcludeFilterIgnoresRemovedFromObject(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(from, []byte(`
CREATE TABLE diff_skip (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(""), 0o600), qt.IsNil)

	report, err := atlasschema.DiffLocalFiles(atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + to},
		DevURL:   "sqlite://dev.db",
		Exclude:  []string{"diff_skip"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "")
}

func TestDiffLocalFilesRequiresDevURL(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.DiffLocalFiles(atlasschema.DiffOptions{})

	c.Assert(err, qt.ErrorMatches, `--dev-url is required for local schema file diffing`)
}

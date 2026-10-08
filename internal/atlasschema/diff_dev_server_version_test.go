package atlasschema_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
)

// notEnforcedCheck declares a CHECK only PostgreSQL 18 and later can hold.
const notEnforcedCheck = `CREATE TABLE t (a int CHECK (a > 0) NOT ENFORCED);`

// notEnforcedDiffOptions compares the file with itself on devURL.
func notEnforcedDiffOptions(c *qt.C, devURL string) atlasschema.DiffOptions {
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(notEnforcedCheck), 0o600), qt.IsNil)
	return atlasschema.DiffOptions{
		FromURLs: []string{"file://" + path},
		ToURLs:   []string{"file://" + path},
		DevURL:   devURL,
		Runtime:  must.Must(builtin.New())}
}

// TestDiff_TwoDocumentsPlanForTheDevServer_HappyPath compares two documents
// on a docker:// dev server, whose image tag names the release: they plan for
// PostgreSQL 18, which holds a NOT ENFORCED CHECK, rather than for the dialect
// default, which refused it (stokaro/ptah#3910). The tag is read without
// starting the container.
func TestDiff_TwoDocumentsPlanForTheDevServer_HappyPath(t *testing.T) {
	c := qt.New(t)

	report, err := atlasschema.Diff(c.Context(), notEnforcedDiffOptions(c, "docker://postgres/18/dev"))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
}

// TestDiff_TwoDocumentsPlanForTheDevServer_FailurePath is the control: a dev
// server tagged 17 cannot hold the CHECK, and the refusal names it.
func TestDiff_TwoDocumentsPlanForTheDevServer_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(c.Context(), notEnforcedDiffOptions(c, "docker://postgres/17/dev"))

	c.Assert(err, qt.ErrorMatches, `(?s).*NOT ENFORCED CHECK, which requires target capability not_enforced_checks.*`)
}

// TestDiff_TwoDocumentsLeaveASQLiteDevFileAlone compares two documents on a
// SQLite dev URL that names a file nobody created. There is no server to ask
// for a version, so the diff does not open the file, and opening it would
// create it: the working directory gained an empty dev.db.
func TestDiff_TwoDocumentsLeaveASQLiteDevFileAlone(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	devPath := filepath.Join(dir, "dev.db")

	report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + path},
		ToURLs:   []string{"file://" + path},
		DevURL:   atlasurl.SQLiteURLFromPath(devPath),
		Runtime:  must.Must(builtin.New())})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
	_, err = os.Stat(devPath)
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

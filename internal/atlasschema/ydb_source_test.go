package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// Comparing documents needs no server but still needs the dev declaration's
// database root to spell permissions on the database and older YDB directories.
func TestYQLDocumentDiffUsesDeclaredDatabaseRoot(t *testing.T) {
	for _, version := range []string{"25.1.4.7", "26.2.1.14"} {
		t.Run(version, func(t *testing.T) {
			c := qt.New(t)
			directory := c.TempDir()
			before, after := filepath.Join(directory, "from.sql"), filepath.Join(directory, "to.sql")
			prefix := "CREATE TABLE `shop/orders` (id Int64 NOT NULL, PRIMARY KEY(id)); CREATE USER app;"
			c.Assert(os.WriteFile(before, []byte(prefix), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(after, []byte(prefix+"GRANT CONNECT ON `/local` TO app; GRANT LIST ON `/local/shop` TO app;"), 0o600), qt.IsNil)
			report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
				FromURLs: []string{"file://" + filepath.ToSlash(before)}, ToURLs: []string{"file://" + filepath.ToSlash(after)},
				DevURL: "docker://ydb/" + version + "/local", Schemas: []string{"shop"},
			})
			c.Assert(err, qt.IsNil)
			sql, err := report.MarshalSQL()
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "GRANT 'ydb.database.connect' ON `/local` TO `app`;")
			c.Assert(sql, qt.Contains, "ydb.generic.list")
		})
	}
}

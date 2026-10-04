package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/ydb"
)

// A directory whose name starts with a dot is a server directory such as .sys,
// and TableNames never reads one: it answers before it asks the session for the
// driver, which is why no session is passed here.
func TestTableNames_NeverReadsADotDirectory(t *testing.T) {
	for _, directory := range []string{".sys", ".metadata/x", "app/.tmp", "."} {
		t.Run(directory, func(t *testing.T) {
			c := qt.New(t)
			names, err := ydb.TableNames(c.Context(), nil, directory)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.IsNil)
		})
	}
}

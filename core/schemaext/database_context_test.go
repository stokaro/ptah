package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestDatabaseContextRequiresExactNonSecretRootSpellings(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value schemaext.DatabaseContext
		valid bool
	}{
		{name: "exact", value: schemaext.DatabaseContext{Database: "/database/realm", SourceDatabase: "/database"}, valid: true},
		{name: "opaque", value: schemaext.DatabaseContext{Database: "target:tenant", SourceDatabase: "author:tenant"}, valid: true},
		{name: "empty"},
		{name: "missing source", value: schemaext.DatabaseContext{Database: "/db"}},
		{name: "space", value: schemaext.DatabaseContext{Database: " /db", SourceDatabase: "/db"}},
		{name: "newline", value: schemaext.DatabaseContext{Database: "/db", SourceDatabase: "/db\nother"}},
		{name: "separator", value: schemaext.DatabaseContext{Database: "/db", SourceDatabase: "/db\u2028other"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			err := test.value.Validate()
			if test.valid {
				c.Assert(err, qt.IsNil)
			} else {
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			}
		})
	}
}

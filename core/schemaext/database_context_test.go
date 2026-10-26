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
		want  error
	}{
		{name: "exact", value: schemaext.DatabaseContext{Database: "/database/realm", SourceDatabase: "/database"}},
		{name: "opaque", value: schemaext.DatabaseContext{Database: "target:tenant", SourceDatabase: "author:tenant"}},
		{name: "empty", want: schemaext.ErrInvalidValue},
		{name: "missing source", want: schemaext.ErrInvalidValue, value: schemaext.DatabaseContext{Database: "/db"}},
		{name: "space", want: schemaext.ErrInvalidValue, value: schemaext.DatabaseContext{Database: " /db", SourceDatabase: "/db"}},
		{name: "newline", want: schemaext.ErrInvalidValue, value: schemaext.DatabaseContext{Database: "/db", SourceDatabase: "/db\nother"}},
		{name: "separator", want: schemaext.ErrInvalidValue, value: schemaext.DatabaseContext{Database: "/db", SourceDatabase: "/db\u2028other"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			err := test.value.Validate()
			c.Assert(err, qt.ErrorIs, test.want)
		})
	}
}

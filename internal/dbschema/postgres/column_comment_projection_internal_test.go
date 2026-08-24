package postgres

// White-box testing required: columnCommentExpr builds one expression of a
// shared query string, and the property under test is what that string contains
// for a target that cannot answer. Reaching it from outside the package would
// mean a live server that emulates the catalog.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// TestColumnCommentProjection_AsksOnlyWhereTheCatalogCanAnswer pins the gate.
//
// col_description is pg_catalog's, and a target emulating the catalog does not
// necessarily have it. Measured on Spanner through PGAdapter, which builds
// pg_class and pg_attribute as views over information_schema and answers
// `function col_description(bigint, bigint) does not exist` (SQLSTATE 42883).
//
// The projection is one expression in a shared query, so its absence took the
// WHOLE column read with it -- `failed to read tables: failed to read columns
// for schema public` -- rather than one empty field. Three live Spanner tests
// went red on the first attempt at this, which is what the gate is for
// (stokaro/ptah#2101).
func TestColumnCommentProjection_AsksOnlyWhereTheCatalogCanAnswer(t *testing.T) {
	tests := []struct {
		name  string
		caps  capability.Capabilities
		wants bool
	}{
		{
			name:  "a target whose preset rules the catalog out",
			caps:  capability.Postgres16().With(capability.PostgresCatalogFunctions, false),
			wants: false,
		},
		{
			// The control. Without it a reader that never asked would pass the
			// row above, and every comment would read empty everywhere.
			name:  "a target that has the catalog",
			caps:  capability.Postgres16().With(capability.PostgresCatalogFunctions, true),
			wants: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewPostgreSQLReaderWithCapabilities(nil, "public", test.caps)

			projection := reader.columnCommentExpr()

			c.Assert(strings.Contains(projection, "col_description"), qt.Equals, test.wants)
			// Either way the column is in the result, because the scan reads it
			// by position.
			c.Assert(projection, qt.Contains, "AS column_comment")
		})
	}
}

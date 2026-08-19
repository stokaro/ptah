package postgres

// White-box testing required: readSchemas is unexported, and what it reports is
// not observable through ReadSchema without a live server -- the exported path
// returns a whole DBSchema whose other members would have to be faked to reach
// this one field.
//
// The property under test is that the schema list and the objects underneath it
// come from ONE decision: readSchemas must report exactly schemasToRead(), the
// list readTables and every other read below iterates. It used to report
// nothing at all unless an allow-list had been passed, so an unscoped read
// described tables in `public` while denying it had read any schema
// (stokaro/ptah#1276).
//
// The fake server answers per bound schema name, which is what makes the count
// assertion load-bearing: a reader that reported a name it never asked about
// would leave the count behind, and a reader that asked and discarded the answer
// would lose the comment. Both spellings of the defect are therefore visible
// here, and neither is visible from the parsers alone.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// pgNamespaceFixture is pg_namespace as PostgreSQL 17.10 reports it for a
// database holding one extra schema. The `public` comment is the server's own
// default, measured on that server; carrying it is how a caller can tell a row
// that came from the catalog from one synthesized downstream.
func pgNamespaceFixture() map[string][][]driver.Value {
	return map[string][][]driver.Value{
		"public": {{"public", "standard public schema"}},
		"extra":  {{"extra", ""}},
	}
}

// pgNamespaceServer answers readSchemaInfo's query for the schema it was asked
// about. A name absent from the fixture yields no rows, which is what a real
// server does for a schema that does not exist.
func pgNamespaceServer(catalog map[string][][]driver.Value) dbtest.QueryHandler {
	return func(_ string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		name, _ := args[0].Value.(string)
		return dbtest.QueryResult{
			Columns: []string{"nspname", "schema_comment"},
			Rows:    catalog[name],
		}, nil
	}
}

func TestPostgreSQLReaderReadSchemasReportsWhatItRead(t *testing.T) {
	tests := []struct {
		name string
		// connected is the schema the connection itself is on, which is what an
		// unscoped read covers.
		connected string
		// scope is the allow-list, nil for an unscoped read.
		scope []string
		want  []types.DBSchemaInfo
		// wantQueries is how many schemas were asked about. It pins the list the
		// reader worked from rather than the list it returned: the two were
		// allowed to differ, and that is the defect.
		wantQueries int
	}{
		{
			// The #1276 row. Nothing was named, so the read covers the schema
			// the connection is on -- and says so, comment included.
			name:        "unscoped reports the connected schema",
			connected:   "public",
			scope:       nil,
			want:        []types.DBSchemaInfo{{Name: "public", Comment: "standard public schema"}},
			wantQueries: 1,
		},
		{
			// The connected schema is not hardcoded: a connection on `extra`
			// covers `extra`.
			name:        "unscoped follows the connection",
			connected:   "extra",
			scope:       nil,
			want:        []types.DBSchemaInfo{{Name: "extra"}},
			wantQueries: 1,
		},
		{
			// The control against over-correction. Widening the unscoped answer
			// must not widen a scoped one: a read told to cover `extra` reports
			// `extra` and not the schema it is connected to.
			name:        "scoped reports only what was named",
			connected:   "public",
			scope:       []string{"extra"},
			want:        []types.DBSchemaInfo{{Name: "extra"}},
			wantQueries: 1,
		},
		{
			name:        "scoped reports every name it was given",
			connected:   "public",
			scope:       []string{"extra", "public"},
			want:        []types.DBSchemaInfo{{Name: "extra"}, {Name: "public", Comment: "standard public schema"}},
			wantQueries: 2,
		},
		{
			// A named schema that is not there is reported as absent rather
			// than invented, and it does not fall back to the connected one.
			name:        "scoped drops a schema the server does not have",
			connected:   "public",
			scope:       []string{"missing"},
			want:        make([]types.DBSchemaInfo, 0),
			wantQueries: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, pgNamespaceServer(pgNamespaceFixture()))
			reader := NewPostgreSQLReader(db.SQL, test.connected)
			reader.SetSchemas(test.scope)

			schemas, err := reader.readSchemas()

			c.Assert(err, qt.IsNil)
			c.Assert(schemas, qt.DeepEquals, test.want)
			c.Assert(db.QueryCount(), qt.Equals, test.wantQueries)
		})
	}
}

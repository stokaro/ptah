package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/internal/dbschema/sqlite"
)

func TestReaderTableOptions(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITHOUT ROWID, STRICT`)
	c.Assert(err, qt.IsNil)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	c.Assert(schema.Tables, qt.HasLen, 1)
	observed, found, err := schemaext.FacetAs[*sqlitetable.ObservedTable](schema.Tables[0].Facets, sqlitetable.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(observed.Options, qt.Equals, sqlitetable.Options{Strict: true, WithoutRowID: true})
	c.Assert(schema.FeatureCoverage.Lookup(sqlitetable.TableKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete,
		qt.Commentf("the read looked at every table's options"))
}

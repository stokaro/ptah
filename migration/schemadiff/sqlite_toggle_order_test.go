package schemadiff_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestCompareWithDatabaseRejectsMalformedSQLiteVirtualDropToggleBeforeCatalogQueries(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "target.db"))
	c.Assert(err, qt.IsNil)
	// A closed connection makes any catalog query fail, independently of
	// context validation at the API boundary.
	dbschema.CloseAndWarn(conn)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	_, err = schemadiff.CompareWithDatabase(
		t.Context(),
		conn,
		&schemamodel.Database{},
		&catalog.Database{},
		nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "database is closed")
}

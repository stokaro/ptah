package schemadiff_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompareWithDatabaseRejectsMalformedSQLiteVirtualDropToggleBeforeCatalogQueries(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "target.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = schemadiff.CompareWithDatabase(
		ctx,
		conn,
		&goschema.Database{},
		&dbschematypes.DBSchema{},
		nil,
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "context canceled")
}

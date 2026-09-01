//go:build integration

package integration_test

// Live PostgreSQL coverage for the claim internal/pgtypeext writes down.
//
// That package says which extension provides which type, from a list, because
// schema comparison runs against a description that may have come from a file
// and there is no catalog to ask. A list is a claim, and this is the control:
// the server is asked the same question and the two must agree
// (stokaro/ptah#2389).

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/pgtypeext"
)

// TestPgTypeExt_TheClaimAgreesWithTheCatalogLive asks the server.
//
// Both directions. A mapping this package gets wrong is the obvious failure; a
// type it names that no installed extension provides is the quieter one, and it
// is what a typo produces -- an entry that matches no column and reads as
// coverage.
func TestPgTypeExt_TheClaimAgreesWithTheCatalogLive(t *testing.T) {
	setup := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	measured := providedTypes(setup, ctx, typeExtensionDatabase(setup, ctx))

	for typeName, extension := range pgtypeext.Types() {
		t.Run(typeName, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(measured[typeName], qt.Equals, extension,
				qt.Commentf("the catalog says %q provides %q", measured[typeName], typeName))
		})
	}
}

// TestPgTypeExt_TheCatalogHasEveryTypeTheClaimNamesLive is the other direction.
func TestPgTypeExt_TheCatalogHasEveryTypeTheClaimNamesLive(t *testing.T) {
	setup := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	measured := providedTypes(setup, ctx, typeExtensionDatabase(setup, ctx))

	for typeName := range pgtypeext.Types() {
		t.Run(typeName, func(t *testing.T) {
			c := qt.New(t)
			_, found := measured[typeName]
			c.Assert(found, qt.IsTrue,
				qt.Commentf("no installed extension provides %q, so nothing can match it", typeName))
		})
	}
}

// typeExtensionDatabase installs every extension the claim names.
//
// Its own database, because it installs extensions and the shared one is not
// this test's to change.
func typeExtensionDatabase(c *qt.C, ctx context.Context) *sql.DB {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.TimescaleDB)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	name := fmt.Sprintf("ptah_typeext_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() {
		cleanup, err := sql.Open("pgx", adminURL)
		c.Assert(err, qt.IsNil)
		defer cleanup.Close()
		dropE2EDatabase(c, context.Background(), cleanup, name)
	})

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })

	// Every extension the claim names, so a mapping cannot go unmeasured for
	// want of the extension being installed.
	for _, extension := range pgtypeext.Types() {
		_, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS "+extension)
		c.Assert(err, qt.IsNil, qt.Commentf("CREATE EXTENSION %s", extension))
	}
	return db
}

// providedTypes asks the catalog which extension provides each type.
func providedTypes(c *qt.C, ctx context.Context, db *sql.DB) map[string]string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT t.typname, e.extname
		FROM pg_type t
		JOIN pg_depend d ON d.objid = t.oid AND d.deptype = 'e'
		JOIN pg_extension e ON e.oid = d.refobjid`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	provided := map[string]string{}
	for rows.Next() {
		var typeName, extension string
		c.Assert(rows.Scan(&typeName, &extension), qt.IsNil)
		provided[typeName] = extension
	}
	c.Assert(rows.Err(), qt.IsNil)
	return provided
}

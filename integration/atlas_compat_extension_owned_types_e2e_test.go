//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// extensionOwnedTypeCase is one database shape and the type blocks its
// description must and must not carry.
type extensionOwnedTypeCase struct {
	name string
	// extensions are installed with CASCADE, in order, before seed runs.
	extensions []string
	// seed is the shape's DDL. It creates the user's own types alongside the
	// extension's, so both directions are measured on one document.
	seed string
	// absentBlocks are block spellings the description must NOT carry, because
	// CREATE EXTENSION already creates what they would declare.
	absentBlocks []string
	// presentBlocks are block spellings the description MUST carry: the user's
	// own types, and the extension blocks without which the document names a
	// type nothing creates.
	presentBlocks []string
	// unreplayable, when set, is the reason this row's fixture cannot be
	// materialized on a fresh dev database whatever the renderer emits. See
	// [atlasCompatExtensionRoundTripCase.unreplayable].
	unreplayable string
	// why records the measurement behind the row.
	why string
}

// TestAtlasCompatExtensionOwnedTypesE2E pins that a domain, composite or range
// type an extension owns is not described as a user type, and that a type the
// user declared still is.
//
// `CREATE EXTENSION` creates the extension's own types, so a description that
// declares both the extension and the type cannot be replayed: the second
// declaration collides with what the first already made. Measured on
// PostgreSQL 17.10 against `lo`, which supplies the domain `lo`, with a
// `lo`-typed column so the extension block survives suppression:
//
//	before  domain "lo" declared, extension "lo" declared, replay exit 1
//	        ERROR: type "lo" already exists (SQLSTATE 42710)
//	        SQL: CREATE DOMAIN "lo" AS oid;
//	after   domain "lo" absent, extension "lo" declared, replay exit 0
//
// The reader already excluded extension-owned FUNCTIONS for exactly this
// reason -- its comment says they "cannot be dropped independently and should
// be managed by the extension" -- and the three type reads were never given
// that filter (stokaro/ptah#1294).
//
// Both directions are asserted on every row, on ONE document, because the
// cheap wrong fix is a name filter. The user's types are named as closely to
// the extension's as PostgreSQL allows -- `lo` beside `lo_own` -- so a
// `NOT LIKE 'lo_%'` written without an ESCAPE clause, which is the mistake this
// reader has already had to correct for roles (stokaro/ptah#1291), takes the
// user's type with it and this test says so.
//
// Which extensions can carry a row at all was measured rather than assumed.
// Over every extension the postgres:17 image ships, exactly six extension-owned
// types reach these three reads:
//
//	dblink         dblink_pkey_results     composite
//	earthdistance  earth                   domain
//	lo             lo                      domain
//	tablefunc      tablefunc_crosstab_2/3/4 composite
//
// pg_buffercache and pg_stat_statements also own composites, but those are the
// row types of their views, and readComposites joins pg_class on relkind = 'c',
// so they never reached a description. NO shipped extension supplies a range
// type, which is why the range row below builds its member with
// `ALTER EXTENSION ... ADD` and asserts the blocks rather than the round trip.
//
// `earthdistance` reproduces the defect too and is deliberately NOT a row here.
// Its column spelling is not stable across environments, and the divergence is
// measured rather than suspected. On a database holding
//
//	CREATE EXTENSION earthdistance CASCADE;
//	CREATE TABLE places (id integer PRIMARY KEY, loc earth NOT NULL);
//
// this suite's own assertions rendered `column "loc" { type = sql("cube") }`
// with an `extension "cube"` block on PostgreSQL 17.10 AND on 18.4 locally, and
// `type = sql("earth")` with an `extension "earthdistance"` block on CI, from
// the same catalog answer -- `format_type` reports `earth` and
// `information_schema.columns.domain_name` reports `earth` on both. Which
// spelling appears decides which extension the reference scan keeps, so an
// assertion about either is really an assertion about stokaro/ptah#1242's
// flattening and about which block survives, neither of which this change is
// for. A fixture that answers differently in CI than on every server it was
// written against is not a regression test; it is a second finding, and it is
// reported as one rather than pinned here.
func TestAtlasCompatExtensionOwnedTypesE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []extensionOwnedTypeCase{
		{
			name:       "a domain the extension's install script creates",
			extensions: []string{"lo"},
			seed: "CREATE DOMAIN lo_own AS text;" +
				" CREATE TABLE docs (id integer PRIMARY KEY, payload lo, own_payload lo_own)",
			absentBlocks:  []string{`domain "lo"`},
			presentBlocks: []string{`extension "lo"`, `domain "lo_own"`},
			why: "lo supplies the domain lo and a lo-typed column keeps the extension in the" +
				" document, so the collision is reachable; lo_own is the user's and must survive",
		},
		{
			name:       "a composite the extension's install script creates",
			extensions: []string{"dblink"},
			seed: "CREATE TYPE dblink_pkey_results_own AS (position integer, colname text);" +
				" CREATE TABLE keys (id integer PRIMARY KEY, k dblink_pkey_results," +
				" own_k dblink_pkey_results_own)",
			absentBlocks:  []string{`composite "dblink_pkey_results"`},
			presentBlocks: []string{`extension "dblink"`, `composite "dblink_pkey_results_own"`},
			why: "dblink supplies the composite dblink_pkey_results; the user's type differs from" +
				" it by a suffix an unescaped LIKE underscore would swallow",
		},
		{
			name:       "a range type an extension owns",
			extensions: []string{"lo"},
			seed: "CREATE TYPE ptahspan AS RANGE (subtype = integer);" +
				" ALTER EXTENSION lo ADD TYPE ptahspan;" +
				" CREATE TYPE ptahspan_own AS RANGE (subtype = integer);" +
				" CREATE TABLE spans (id integer PRIMARY KEY, s ptahspan, own_s ptahspan_own)",
			absentBlocks:  []string{`range "ptahspan"`},
			presentBlocks: []string{`extension "lo"`, `range "ptahspan_own"`},
			unreplayable: "lo owns ptahspan through ALTER EXTENSION ... ADD, so no CREATE EXTENSION" +
				" recreates it and the spans table can never be built on a fresh dev database",
			why: "no extension the postgres:17 image ships supplies a range type, so the only way to" +
				" reach the range arm is to write the membership row directly",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", adminURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			stamp := time.Now().UnixNano()
			sourceName := fmt.Sprintf("ptah_ext_type_src_%d", stamp)
			devName := fmt.Sprintf("ptah_ext_type_dev_%d", stamp)
			createE2EDatabase(c.TB, ctx, adminDB, sourceName)
			defer dropE2EDatabase(c.TB, context.Background(), adminDB, sourceName)
			createE2EDatabase(c.TB, ctx, adminDB, devName)
			defer dropE2EDatabase(c.TB, context.Background(), adminDB, devName)

			sourceURL := replaceDatabaseName(c.TB, adminURL, sourceName)
			devURL := replaceDatabaseName(c.TB, adminURL, devName)

			seedExtensionOwnedTypeDB(c.TB, ctx, sourceURL, test)

			rendered := runAtlasCompatInspect(c.TB, sourceURL, "")

			for _, block := range test.presentBlocks {
				c.Assert(rendered, qt.Contains, block, qt.Commentf("%s", test.why))
			}
			for _, block := range test.absentBlocks {
				c.Assert(rendered, qt.Not(qt.Contains), block,
					qt.Commentf("the document declares a type CREATE EXTENSION already makes: %s", test.why))
			}

			assertExtensionOwnedTypeRoundTrip(c.TB, test, rendered, devURL)
		})
	}
}

// seedExtensionOwnedTypeDB installs the row's extensions and runs its DDL, then
// verifies through pg_depend that an extension really does own a domain,
// composite or range type here. Without that check a row whose membership never
// landed would assert the absence of a block that was never going to be there.
func seedExtensionOwnedTypeDB(tb testing.TB, ctx context.Context, dbURL string, test extensionOwnedTypeCase) {
	c := qt.New(tb)
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	for _, extension := range test.extensions {
		_, err = db.ExecContext(ctx, "CREATE EXTENSION "+quoteE2EIdent(extension)+" CASCADE")
		c.Assert(err, qt.IsNil)
	}
	_, err = db.ExecContext(ctx, test.seed)
	c.Assert(err, qt.IsNil)

	var owned int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*)
		  FROM pg_depend d
		  JOIN pg_type t ON t.oid = d.objid
		  JOIN pg_namespace n ON n.oid = t.typnamespace
		 WHERE d.deptype = 'e'
		   AND d.classid = 'pg_type'::regclass
		   AND t.typtype IN ('d', 'c', 'r')
		   AND n.nspname = 'public'`,
	).Scan(&owned), qt.IsNil)
	c.Assert(owned > 0, qt.IsTrue,
		qt.Commentf("no extension owns a domain, composite or range type here, so this row asserts nothing"))
}

// assertExtensionOwnedTypeRoundTrip materializes the description on a fresh dev
// database, unless the row recorded a reason its fixture can never be
// materialized at all.
func assertExtensionOwnedTypeRoundTrip(tb testing.TB, test extensionOwnedTypeCase, rendered, devURL string) {
	c := qt.New(tb)
	c.Helper()

	if test.unreplayable != "" {
		c.Logf("round trip not asserted: %s", test.unreplayable)
		return
	}

	documentPath := filepath.Join(c.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(documentPath, []byte(rendered), 0o600), qt.IsNil)
	readBack := runAtlasCompatInspect(c.TB, "file://"+documentPath, devURL)
	c.Assert(readBack, qt.Not(qt.Equals), "",
		qt.Commentf("the round trip produced an empty document"))
}

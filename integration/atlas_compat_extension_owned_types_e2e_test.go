//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// extensionOwnedTypeCase is one extension, the type kind it supplies, and the
// seed that makes the document name it.
type extensionOwnedTypeCase struct {
	name string
	// extension is the CREATE EXTENSION name. All of these ship with
	// PostgreSQL's contrib, so the fixture needs nothing on the server's
	// filesystem.
	extension string
	// seed creates whatever references the extension, so the extension block
	// survives suppression and the collision is reachable.
	seed string
	// blockPrefix is the document block spelling that must NOT appear.
	blockPrefix string
	// why records the measurement behind the row.
	why string
}

// TestAtlasCompatExtensionOwnedTypesE2E pins that a domain, composite or range
// type an extension owns is not described as a user type.
//
// `CREATE EXTENSION` creates those types, so a document that declares both the
// extension and the type cannot be replayed: the second declaration collides
// with what the first already made. Measured on PostgreSQL 17 against `lo`,
// which supplies the domain `lo`:
//
//	master   domain blocks 1, extension blocks 1, replay rc=1
//	         ERROR: type "lo" already exists (SQLSTATE 42710)
//	fixed    domain blocks 0, extension blocks 1, replay rc=0
//
// The reader already excludes extension-owned FUNCTIONS for the same reason --
// its comment says they "cannot be dropped independently and should be managed
// by the extension" -- and the three type reads were never given that filter
// (stokaro/ptah#1294).
//
// The collision is only reachable when the extension block survives, which is
// why the seed references the extension. An unused extension is omitted from the
// document, and then re-creating its types succeeds because no CREATE EXTENSION
// precedes them.
//
// One row rather than several, and the reasons the others were rejected are
// worth keeping. `tablefunc` supplies three crosstab composites, but a schema
// cannot reference them: `crosstab` without a column definition list is refused
// outright, and with one it does not use the composite. `earthdistance` supplies
// the domain `earth` and does reproduce the collision on master -- but its
// column renders as `sql("cube")`, the domain flattened to its base type
// (stokaro/ptah#1242), so after the fix nothing names `earth` and the extension
// is legitimately omitted. That makes it a confounded fixture rather than a
// second measurement of this property. `lo` has neither problem: its column
// keeps the domain spelling, so the extension survives on both commits and only
// the duplicate declaration moves.
func TestAtlasCompatExtensionOwnedTypesE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)

	tests := []extensionOwnedTypeCase{
		{
			name:        "a domain the extension owns",
			extension:   "lo",
			seed:        "CREATE TABLE docs (id integer PRIMARY KEY, payload lo)",
			blockPrefix: `domain "lo"`,
			why:         "lo supplies the domain lo, and a lo-typed column keeps the extension in the document",
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
			createE2EDatabase(c, ctx, adminDB, sourceName)
			defer dropE2EDatabase(c, context.Background(), adminDB, sourceName)
			createE2EDatabase(c, ctx, adminDB, devName)
			defer dropE2EDatabase(c, context.Background(), adminDB, devName)

			sourceURL := replaceDatabaseName(c, adminURL, sourceName)
			devURL := replaceDatabaseName(c, adminURL, devName)

			seedAtlasCompatExtensionDB(c, ctx, sourceURL, test.extension, test.seed)

			rendered := runAtlasCompatInspect(c, sourceURL, "")

			// The extension block has to be there, or the collision this test is
			// about is unreachable and a pass would mean nothing.
			c.Assert(rendered, qt.Contains, fmt.Sprintf("extension %q", test.extension),
				qt.Commentf("%s", test.why))
			c.Assert(strings.Contains(rendered, test.blockPrefix), qt.IsFalse,
				qt.Commentf("the document declares a type %q already creates", test.extension))

			// Replaying against a fresh database is what proves the two
			// declarations no longer fight: CREATE EXTENSION makes the type, and
			// nothing tries to make it again.
			documentPath := filepath.Join(t.TempDir(), "inspected.hcl")
			c.Assert(os.WriteFile(documentPath, []byte(rendered), 0o600), qt.IsNil)
			readBack := runAtlasCompatInspect(c, "file://"+documentPath, devURL)
			c.Assert(readBack, qt.Not(qt.Equals), "")
		})
	}
}

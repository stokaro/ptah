//go:build integration

package integration_test

import (
	"bytes"
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

	"go.5x5.cz/ptah/cmd/atlas"
)

// atlasCompatExtensionRoundTripCase is one extension shape, its seed DDL, and
// whether the compatibility surface is expected to keep the extension block.
type atlasCompatExtensionRoundTripCase struct {
	name string
	// extension is the CREATE EXTENSION name.
	extension string
	// seed runs after the extension is installed and creates whatever depends
	// on it (or, for the control, whatever does not).
	seed string
	// wantBlock is whether `extension "<extension>"` must survive into the
	// rendered document.
	wantBlock bool
	// why records the measurement behind the expectation.
	why string
}

// TestAtlasCompatInspectExtensionRoundTripE2E pins the property that
// stokaro/ptah#1266 broke: `ptah-compat schema inspect` must emit a document
// Ptah can read back.
//
// The assertion is a real round trip and not a re-run of the renderer's own
// reference scan -- the rendered document is fed to the same binary surface
// against a FRESH dev database, where the extension is not installed, so a
// document that dropped a still-needed extension fails to materialize. That is
// exactly how the regression showed up in the field:
//
//	CREATE EXTENSION isn;
//	CREATE TABLE books (id integer PRIMARY KEY, code isbn NOT NULL);
//
// rendered at exit 0 with `extensions.isn: omitted ...` on stderr, and the
// result was then unreadable -- `type "isbn" does not exist` -- by Ptah AND by
// the pinned Atlas community binary, measured on PostgreSQL 17.10. The omission
// was not a compatibility win, it was a document nobody could read.
//
// The third row is the control that keeps the fix from becoming "never omit an
// extension", which would throw away what #1266 bought.
func TestAtlasCompatInspectExtensionRoundTripE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)

	tests := []atlasCompatExtensionRoundTripCase{
		{
			name:      "extension supplying a column type",
			extension: "isn",
			seed:      "CREATE TABLE books (id integer PRIMARY KEY, code isbn NOT NULL)",
			wantBlock: true,
			why:       "isn supplies the type isbn; the word isn appears nowhere in the document",
		},
		{
			name:      "extension supplying only a function",
			extension: "pgcrypto",
			seed:      "CREATE TABLE tokens (id integer PRIMARY KEY, salt text NOT NULL DEFAULT gen_salt('bf'))",
			wantBlock: true,
			// gen_salt is the deliberate choice over gen_random_uuid: measured
			// on PostgreSQL 17.10, gen_random_uuid also exists in pg_catalog, so
			// a document using it round-trips even with pgcrypto dropped and
			// would pass this test without testing anything. gen_salt has no
			// core equivalent.
			why: "pgcrypto supplies gen_salt, which has no core equivalent",
		},
		{
			name:      "extension nothing depends on",
			extension: "isn",
			seed:      "CREATE TABLE plain (id integer PRIMARY KEY, label text NOT NULL)",
			wantBlock: false,
			why:       "nothing uses anything isn supplies, so #1266's omission still applies",
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
			sourceName := fmt.Sprintf("ptah_ext_rt_src_%d", stamp)
			devName := fmt.Sprintf("ptah_ext_rt_dev_%d", stamp)
			createE2EDatabase(c, ctx, adminDB, sourceName)
			defer dropE2EDatabase(c, context.Background(), adminDB, sourceName)
			createE2EDatabase(c, ctx, adminDB, devName)
			defer dropE2EDatabase(c, context.Background(), adminDB, devName)

			sourceURL := replaceDatabaseName(c, adminURL, sourceName)
			devURL := replaceDatabaseName(c, adminURL, devName)

			seedAtlasCompatExtensionDB(c, ctx, sourceURL, test.extension, test.seed)

			rendered := runAtlasCompatInspect(c, sourceURL, "")
			c.Assert(
				strings.Contains(rendered, fmt.Sprintf("extension %q", test.extension)),
				qt.Equals, test.wantBlock,
				qt.Commentf("%s", test.why),
			)

			// The round trip. A fresh dev database has none of these
			// extensions, so materializing the document is what proves it
			// carries everything it depends on.
			documentPath := filepath.Join(t.TempDir(), "inspected.hcl")
			c.Assert(os.WriteFile(documentPath, []byte(rendered), 0o600), qt.IsNil)
			readBack := runAtlasCompatInspect(c, "file://"+documentPath, devURL)
			c.Assert(readBack, qt.Not(qt.Equals), "",
				qt.Commentf("the round trip produced an empty document"))
		})
	}
}

// seedAtlasCompatExtensionDB installs the extension and runs the shape's DDL,
// then verifies through the catalog that both actually landed rather than
// trusting that the statements returned without error.
func seedAtlasCompatExtensionDB(c *qt.C, ctx context.Context, dbURL, extension, seed string) {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE EXTENSION "+quoteE2EIdent(extension))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, seed)
	c.Assert(err, qt.IsNil)

	var installed int
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_extension WHERE extname = $1", extension,
	).Scan(&installed), qt.IsNil)
	c.Assert(installed, qt.Equals, 1,
		qt.Commentf("the catalog does not report %q installed", extension))

	// pg_depend is what the reader consults to answer "what does this extension
	// supply". An extension whose membership rows are missing would make every
	// expectation here vacuous.
	var members int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*)
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		 WHERE d.deptype = 'e' AND e.extname = $1`, extension,
	).Scan(&members), qt.IsNil)
	c.Assert(members > 0, qt.IsTrue,
		qt.Commentf("the catalog reports %q supplying nothing", extension))
}

// runAtlasCompatInspect runs `schema inspect` on the compatibility surface and
// returns the rendered document, failing the test on a non-nil error.
//
// Roles are excluded because PostgreSQL roles are CLUSTER-scoped: every role
// any other database on the same server has ever created is visible here, and
// replaying a document that declares them dies on "role already exists" before
// reaching the extension question this test is about.
func runAtlasCompatInspect(c *qt.C, sourceURL, devURL string) string {
	c.Helper()

	args := []string{
		"schema", "inspect",
		"--url", sourceURL,
		"--format", "{{ hcl . }}",
		"--exclude", "*[type=role]",
	}
	args = append(args, devURLArgs(devURL)...)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	return out.String()
}

// devURLArgs appends --dev-url only when one was given, keeping the caller's
// test body free of branching.
func devURLArgs(devURL string) []string {
	if devURL == "" {
		return nil
	}
	return []string{"--dev-url", devURL}
}

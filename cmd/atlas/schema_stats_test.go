package atlas_test

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
)

// The Atlas-compatible surface spells this verb `schema stats inspect`, three
// words deep. These tests hold the path itself, not only the body it reaches:
// a script written against that surface types all three (stokaro/ptah#1711).

// seedStatsSQLite creates a small database to scrape.
func seedStatsSQLite(c *qt.C, path, ddl string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.Exec(ddl)
	c.Assert(err, qt.IsNil)
}

// TestCompatSchemaStatsInspectScrapesALiveDatabase is the verb end to end on
// the compat surface.
func TestCompatSchemaStatsInspectScrapesALiveDatabase(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "app.db")
	seedStatsSQLite(c, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);"+
			"CREATE INDEX idx_users_email ON users (email);")

	out, err := runAtlasArgs("schema", "stats", "inspect", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `ptah_schema_tables{dialect="sqlite"} 1`)
	c.Assert(out, qt.Contains, `ptah_schema_columns{dialect="sqlite"} 2`)
	c.Assert(out, qt.Contains, `ptah_schema_indexes{dialect="sqlite"} 1`)
	c.Assert(strings.HasSuffix(out, "# EOF\n"), qt.IsTrue)
}

// TestCompatSchemaStatsIsAGroupNotTheBody is why the sub-verb exists.
//
// Registering the body directly as `schema stats` would answer two of the three
// words and leave `inspect` looking like a stray positional. That is a worse
// failure than a missing verb: some shapes of it run, report success, and hand
// the operator a usage message where a scrape was expected. `stats` alone must
// therefore behave as a group, and must not emit metrics.
func TestCompatSchemaStatsIsAGroupNotTheBody(t *testing.T) {
	c := qt.New(t)

	out, err := runAtlasArgs("schema", "stats")

	// Exit 0 with help, which is also what the community binary does for this
	// word. A group that errored here would be stricter than the surface it
	// mirrors on a word that surface already answers.
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Not(qt.Contains), "ptah_schema_")
	c.Assert(out, qt.Contains, "inspect")
}

// TestCompatSchemaStatsInspectAcceptsSQLite states a deliberate divergence.
//
// The Atlas surface this mirrors refuses SQLite for this verb at runtime. Ptah
// does not: its reader handles SQLite like any other dialect, and refusing
// would copy a limitation this implementation does not have.
//
// This is a widening, never a narrowing. The pinned community binary carries no
// `stats` spelling at all -- it prints the `schema` group help at exit 0, the
// same as for a nonsense subcommand -- so there is no refusal here to be looser
// than. The measurement is transcribed in newAtlasSchemaStatsCommand's doc.
func TestCompatSchemaStatsInspectAcceptsSQLite(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "app.db")
	seedStatsSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runAtlasArgs("schema", "stats", "inspect", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Not(qt.Contains), "not supported")
	c.Assert(out, qt.Contains, `ptah_schema_tables{dialect="sqlite"} 1`)
}

// TestCompatSchemaStatsIsGatedUnderStrictCompat covers the other branch.
//
// The verb is registered on the full compatibility surface and gated out of the
// strict one, because the pinned community binary registers no `stats` at all.
// A fix that lands on only one of those two branches leaves the other answering
// something nobody chose -- here, a verb that strict mode was never meant to
// carry -- so both are measured.
func TestCompatSchemaStatsIsGatedUnderStrictCompat(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"schema", "stats"})

	_, err := root.ExecuteC()

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Contains,
		"Abort: 'ptah-compat schema stats' is unavailable while PTAH_ATLAS_STRICT_COMPAT is enabled.")
}

// TestCompatSchemaStatsStaysHiddenFromStrictHelp keeps the gate off the help
// listing, so strict mode advertises the community verb set and nothing else.
func TestCompatSchemaStatsStaysHiddenFromStrictHelp(t *testing.T) {
	c := qt.New(t)

	strict := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	strictSchema, _, err := strict.Find([]string{"schema"})
	c.Assert(err, qt.IsNil)
	strictStats, _, err := strict.Find([]string{"schema", "stats"})
	c.Assert(err, qt.IsNil)
	c.Assert(strictStats.Hidden, qt.IsTrue)
	c.Assert(strictSchema.Commands(), qt.Not(qt.HasLen), 0)

	// On the full surface it is a listed verb, not a hidden one.
	full := atlas.NewCompatCommand("atlas")
	fullStats, _, err := full.Find([]string{"schema", "stats"})
	c.Assert(err, qt.IsNil)
	c.Assert(fullStats.Hidden, qt.IsFalse)
}

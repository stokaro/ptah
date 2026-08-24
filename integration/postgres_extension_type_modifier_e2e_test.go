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

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// extensionTypeColumns are the column types under test, one row each.
//
// The first three belong to an extension and carry a modifier. The rest are the
// controls, and they are the reason the fix could be measured rather than
// merely believed: an enum and a composite type are reported by
// information_schema as USER-DEFINED exactly like a vector is, and they carry
// no modifier at all. A rule keyed on the category alone would change their
// spelling too -- schema-qualifying them when they are off the search path --
// and only a row that holds one can say so.
//
// The sized built-ins are here for the other direction: their width lives in a
// column of its own and a rule that reached them would take the read back to
// what #1138 and #1662 already settled.
//
// The two expectations are separate because the answers are. wantServer is
// format_type's, and wantRead is Ptah's -- and for varchar they differ, because
// a read composes the width with the shorter spelling the catalog keeps in
// udt_name. That difference predates this and is not what the rows are for;
// carrying it as its own field is what keeps the vector rows from having to
// share a single "correct" answer with it.
var extensionTypeColumns = []struct {
	name       string
	declared   string
	wantServer string
	wantRead   string
}{
	{
		name: "a vector, whose dimension is the indexable part", declared: "vector(384)",
		wantServer: "vector(384)", wantRead: "vector(384)",
	},
	{
		name: "a half-precision vector", declared: "halfvec(768)",
		wantServer: "halfvec(768)", wantRead: "halfvec(768)",
	},
	{
		name: "a sparse vector", declared: "sparsevec(64)",
		wantServer: "sparsevec(64)", wantRead: "sparsevec(64)",
	},

	// The controls.
	{
		name: "an enum, USER-DEFINED with no modifier", declared: "ptah_mood",
		wantServer: "ptah_mood", wantRead: "ptah_mood",
	},
	{
		name: "a composite, the same", declared: "ptah_pair",
		wantServer: "ptah_pair", wantRead: "ptah_pair",
	},
	{
		name: "varchar, whose width lives in another column", declared: "varchar(80)",
		wantServer: "character varying(80)", wantRead: "varchar(80)",
	},
	{
		name: "numeric, whose precision lives in two more", declared: "numeric(12,4)",
		wantServer: "numeric(12,4)", wantRead: "numeric(12,4)",
	},
	{
		name: "text, which has no modifier to lose", declared: "text",
		wantServer: "text", wantRead: "text",
	},
}

// TestPostgresExtensionTypeModifierReplaysE2E pins that a type belonging to an
// extension keeps its modifier through a read, and that the document replays.
//
// It is a REPLAY rather than a convergence check, and the distinction is the
// whole point. `schema apply --dry-run` against the source answered
// `Schema is synced, no changes to be made.` for the fixture below while the
// dimension was being dropped, because both sides of that comparison read
// through the same projection and a reader that misunderstands a type agrees
// with itself. Only a second database can tell:
//
//	ERROR: column does not have dimensions (SQLSTATE 22023)
//	SQL: CREATE INDEX "..." ON "..." USING hnsw ("embedding" vector_cosine_ops)
//
// The index is what makes the failure loud instead of silent. A vector column
// with no dimension is accepted by the server; it is the index over it that is
// refused, so a fixture without one would replay clean and report nothing
// (stokaro/ptah#2121).
//
// It runs against the TimescaleDB target because that image ships pgvector,
// and the extension is what the test needs -- nothing here is about
// TimescaleDB. Measured on pgvector 0.8.1 there and 0.8.6 on pgvector/pgvector:pg17.
func TestPostgresExtensionTypeModifierReplaysE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	stamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("ptah_ext_typmod_src_%d", stamp)
	replayName := fmt.Sprintf("ptah_ext_typmod_replay_%d", stamp)
	createE2EDatabase(c, ctx, adminDB, sourceName)
	defer dropE2EDatabase(c, context.Background(), adminDB, sourceName)
	createE2EDatabase(c, ctx, adminDB, replayName)
	defer dropE2EDatabase(c, context.Background(), adminDB, replayName)

	sourceURL := replaceDatabaseName(c, dbURL, sourceName)
	replayURL := replaceDatabaseName(c, dbURL, replayName)
	seedExtensionTypeFixture(c, ctx, sourceURL)

	// What the server says, before Ptah is asked anything. Without this the
	// assertions below could agree with a server that never stored the
	// modifier, and the test would pass on a database that proves nothing.
	c.Assert(liveColumnTypes(c, ctx, sourceURL), qt.DeepEquals, wantedServerTypes())

	conn, err := dbschema.ConnectToDatabase(ctx, sourceURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	// Non-vacuity: every column has to be in the read, or the comparison below
	// is a comparison of nothing.
	c.Assert(readTypeColumnCount(read, "vectors"), qt.Equals, len(extensionTypeColumns)+1)

	// The read, column by column. This is where the defect lived: the three
	// extension rows came back as `vector`, `halfvec` and `sparsevec`.
	c.Assert(readColumnTypes(read, "vectors"), qt.DeepEquals, wantedReadTypes())

	// And the property the read exists for. The document Ptah writes has to
	// produce the database it was read from, in a database that shares nothing
	// with it.
	repoRoot := e2eRepoRoot(t)
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	dir := c.TempDir()
	documentPath := filepath.Join(dir, "source.hcl")
	document, stderr, err := runCLIProcess(ctx, dir, binary, "schema", "inspect", "--db-url", sourceURL)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(os.WriteFile(documentPath, []byte(document), 0o600), qt.IsNil)

	_, applyErr, err := runCLIProcess(ctx, dir, binary,
		"schema", "apply", "--db-url", replayURL, "--to", "file://"+documentPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("the replay was refused:\n%s\ndocument:\n%s", applyErr, document))

	// The replayed database read back with the server's own answer, not with
	// Ptah's: a reader that drops a modifier on the way in would drop it on the
	// way out too and the two sides would agree while both were wrong.
	c.Assert(liveColumnTypes(c, ctx, replayURL), qt.DeepEquals, wantedServerTypes())
}

// seedExtensionTypeFixture creates the table this test reads, by hand.
//
// It is written here rather than produced by Ptah on purpose. A table Ptah
// created would round-trip through whatever the renderer happens to write, and
// a defect in the reader stays invisible against a fixture the renderer wrote.
func seedExtensionTypeFixture(c *qt.C, ctx context.Context, dbURL string) {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS vector",
		"CREATE TYPE ptah_mood AS ENUM ('ok', 'bad')",
		"CREATE TYPE ptah_pair AS (a integer, b text)",
		createExtensionTypeTable(),
		// The index is the assertion the server makes for us. See the test's
		// own comment for why a fixture without one would report nothing.
		`CREATE INDEX vectors_embedding_hnsw ON vectors USING hnsw (c_0 vector_cosine_ops)`,
	} {
		_, err = db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// createExtensionTypeTable builds one table with a column per row above.
func createExtensionTypeTable() string {
	statement := "CREATE TABLE vectors (id integer PRIMARY KEY"
	for i, column := range extensionTypeColumns {
		statement += fmt.Sprintf(", c_%d %s", i, column.declared)
	}
	return statement + ")"
}

// wantedServerTypes is how the server spells every column, keyed by name.
func wantedServerTypes() map[string]string {
	types := make(map[string]string, len(extensionTypeColumns))
	for i, column := range extensionTypeColumns {
		types[fmt.Sprintf("c_%d", i)] = column.wantServer
	}
	return types
}

// wantedReadTypes is how a read spells them.
func wantedReadTypes() map[string]string {
	types := make(map[string]string, len(extensionTypeColumns))
	for i, column := range extensionTypeColumns {
		types[fmt.Sprintf("c_%d", i)] = column.wantRead
	}
	return types
}

// liveColumnTypes asks the server how it spells each column of the table.
//
// It reads format_type directly rather than through Ptah, so it can be held
// against Ptah's answer without sharing anything with it.
func liveColumnTypes(c *qt.C, ctx context.Context, dbURL string) map[string]string {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = 'public.vectors'::regclass
		AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> 'id'`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var name, spelling string
		c.Assert(rows.Scan(&name, &spelling), qt.IsNil)
		types[name] = spelling
	}
	c.Assert(rows.Err(), qt.IsNil)
	return types
}

// readColumnTypes is the same question asked of Ptah's description.
func readColumnTypes(schema *dbschematypes.DBSchema, table string) map[string]string {
	types := make(map[string]string)
	for _, dbTable := range schema.Tables {
		if dbTable.Name != table {
			continue
		}
		for _, column := range dbTable.Columns {
			if column.Name == "id" {
				continue
			}
			types[column.Name] = column.RawType()
		}
	}
	return types
}

// readTypeColumnCount counts what the read carries, so a vacuous comparison
// cannot pass for a healthy one.
func readTypeColumnCount(schema *dbschematypes.DBSchema, table string) int {
	for _, dbTable := range schema.Tables {
		if dbTable.Name == table {
			return len(dbTable.Columns)
		}
	}
	return 0
}

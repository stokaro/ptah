//go:build integration

// Live guard for the OTHER schema-file surface an index has to survive:
// Ptah's own `.sql` one.
//
// The HCL round trip in index_attributes_postgres_test.go is not a proof about
// this one. The two share the live reader and the comparator and nothing else
// -- a different writer, a different parser and a different converter -- and
// the `.sql` half was the one that was broken.

package gonative_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// TestPostgreSQLIndexAttributes_ItsOwnSQLDescriptionChangesNothing plans a
// database's own `--format sql` description back against it and requires the
// plan to touch no index.
//
// Measured on PostgreSQL 17.10 before the fix, `schema inspect --format sql` of
// the fixture wrote `USING gist ("tsv" tsvector_ops(siglen=64))`, the parser
// read the whole element back as one expression, and planning that document
// against the database it came from produced
//
//	DROP INDEX IF EXISTS "i_opclass_params";
//	CREATE INDEX IF NOT EXISTS "i_opclass_params" ON "t" USING gist (("tsv" tsvector_ops(siglen=64)));
//
// for an identical schema. psql refuses that CREATE at exit 3 with `syntax
// error at or near "tsvector_ops"`, and the DROP ahead of it had already
// committed, so applying the plan left the table with no index at all.
// `i_desc`, `i_nullsfirst` and `i_opclass` were lost the same way, and had been
// since before #1242.
func TestPostgreSQLIndexAttributes_ItsOwnSQLDescriptionChangesNothing(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	dbURL := newBoundaryDatabase(c.TB, dsn, boundaryCase{
		name:  "index_attributes_sql_apply",
		seed:  indexAttributeSeed(),
		query: "search_path=public",
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	document, err := atlasschema.InspectSource(c.Context(), atlasschema.InspectSourceOptions{
		URL:         dbURL,
		Format:      "sql",
		Diagnostics: io.Discard,
	})
	c.Assert(err, qt.IsNil)

	// What the document says, before what the plan does. A writer that stopped
	// emitting the suffix would make the plan empty for the wrong reason.
	c.Assert(document, qt.Contains, `USING gist ("tsv" tsvector_ops(siglen=64))`)
	c.Assert(document, qt.Contains, `("code" text_pattern_ops)`)
	c.Assert(document, qt.Contains, `("created_at" DESC NULLS LAST)`)
	c.Assert(document, qt.Contains, `("score" NULLS FIRST)`)
	c.Assert(document, qt.Contains, `WITH (pages_per_range='32')`)

	path := filepath.Join(c.TempDir(), "inspected.sql")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + path},
		DryRun: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(indexStatements(boundaryStripComments(plan.Statements())), qt.DeepEquals, []string(nil))
}

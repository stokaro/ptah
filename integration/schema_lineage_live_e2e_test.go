//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	cmdschema "ptah.run/cmd/schema"
	"ptah.run/internal/dbtarget"
)

// lineageSeed is one table, one view over it, and one routine that reads and
// writes it. Each of the three answers a different half of the verb.
var lineageSeed = []string{
	`CREATE TABLE lineage_customers (id INTEGER PRIMARY KEY, email TEXT, country TEXT)`,
	`CREATE VIEW lineage_active AS SELECT email AS contact FROM lineage_customers`,
	`CREATE FUNCTION lineage_touch(target INTEGER) RETURNS void LANGUAGE plpgsql AS $$
	 BEGIN UPDATE lineage_customers SET country = 'CZ' WHERE id = target; END;
	 $$`,
}

// lineageDocument is the shape `schema lineage --format json` publishes, read
// back here rather than asserted as text: a consumer decodes it, and a test
// matching substrings would pass on a document nothing can parse.
type lineageDocument struct {
	Edges []struct {
		FromTable  string `json:"from_table"`
		FromColumn string `json:"from_column"`
		ToView     string `json:"to_view"`
	} `json:"edges"`
	Routines struct {
		Reads []struct {
			Table     string `json:"table"`
			Column    string `json:"column"`
			ByRoutine string `json:"by_routine"`
		} `json:"reads"`
		Writes []struct {
			Table     string `json:"table"`
			Column    string `json:"column"`
			ByRoutine string `json:"by_routine"`
		} `json:"writes"`
	} `json:"routines"`
}

// TestSchemaLineageReadsALiveDatabaseE2E is #1270's criterion 8.
//
// No analysis path reached a server before: `ptah schema lineage` took a schema
// source and nothing else, so no test under integration/ could exercise it
// against a database however much it wanted to. This drives the verb against
// one, and asserts on what it publishes rather than on what it prints.
func TestSchemaLineageReadsALiveDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	databaseURL := newLineageDatabase(c, ctx, dbtarget.URL(t, dbtarget.PostgreSQL))

	document := runLineageJSON(c, databaseURL)

	c.Assert(document.Edges, qt.HasLen, 1)
	c.Assert(document.Edges[0].FromTable, qt.Equals, "lineage_customers")
	c.Assert(document.Edges[0].FromColumn, qt.Equals, "email")
	c.Assert(document.Edges[0].ToView, qt.Contains, "lineage_active")
}

// TestSchemaLineageResolvesALiveRoutineE2E is the half that needs a routine
// body, which only a server or a declaration can supply.
//
// The routine reads the column its predicate names and writes the one it
// assigns, and the two are different columns on purpose: a rule that reported
// every column the statement mentions would satisfy an assertion about either
// one alone.
func TestSchemaLineageResolvesALiveRoutineE2E(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	databaseURL := newLineageDatabase(c, ctx, dbtarget.URL(t, dbtarget.PostgreSQL))

	document := runLineageJSON(c, databaseURL)

	c.Assert(document.Routines.Reads, qt.HasLen, 1)
	c.Assert(document.Routines.Reads[0].Column, qt.Equals, "id")
	c.Assert(document.Routines.Reads[0].ByRoutine, qt.Contains, "lineage_touch")
	c.Assert(document.Routines.Writes, qt.HasLen, 1)
	c.Assert(document.Routines.Writes[0].Column, qt.Equals, "country")
	c.Assert(document.Routines.Writes[0].ByRoutine, qt.Contains, "lineage_touch")
}

// runLineageJSON drives `schema lineage --db-url` and decodes what it published.
func runLineageJSON(c *qt.C, databaseURL string) lineageDocument {
	c.Helper()

	cmd := cmdschema.NewSchemaCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"lineage", "--db-url", databaseURL, "--format", "json"})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	var document lineageDocument
	c.Assert(json.Unmarshal(out.Bytes(), &document), qt.IsNil,
		qt.Commentf("stdout is not a decodable document:\n%s", out.String()))
	return document
}

// newLineageDatabase creates a throwaway database carrying the seed above.
func newLineageDatabase(c *qt.C, ctx context.Context, adminURL string) string {
	c.Helper()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_lineage_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	databaseURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range lineageSeed {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return databaseURL
}

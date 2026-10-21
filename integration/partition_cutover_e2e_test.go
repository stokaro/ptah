//go:build integration

package integration_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPartitionCutoverRefusesPopulatedOrdinaryTablePostgresE2E(t *testing.T) {
	c := qt.New(t)
	dev := postgresUnloggedDatabase(c)
	_, err := dev.conn.ExecContext(c.Context(), "CREATE TABLE words (tenant integer PRIMARY KEY, word text); INSERT INTO words VALUES (7, 'keep')")
	c.Assert(err, qt.IsNil)
	var originalOID string
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT 'words'::regclass::oid::text").Scan(&originalOID), qt.IsNil)
	source := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE words (tenant integer PRIMARY KEY, word text) PARTITION BY RANGE (tenant);\n"), 0o600), qt.IsNil)
	plan := filepath.Join(c.TempDir(), "plan.json")
	out, err := runPtahNativeWithError("schema", "plan", "--db-url", dev.url, "--schema-file", source, "--output", plan)
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "a reviewed manual partition cutover is required")
	c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
	_, statErr := os.Stat(plan)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
	var afterOID, kind, word string
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT oid::text, relkind::text FROM pg_class WHERE oid = 'words'::regclass").Scan(&afterOID, &kind), qt.IsNil)
	c.Assert(afterOID, qt.Equals, originalOID)
	c.Assert(kind, qt.Equals, "r")
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT word FROM words WHERE tenant = 7").Scan(&word), qt.IsNil)
	c.Assert(word, qt.Equals, "keep")
}

func TestPartitionParentPlanPreservesExcludedChildrenPostgresE2E(t *testing.T) {
	c := qt.New(t)
	dev := postgresUnloggedDatabase(c)
	_, err := dev.conn.ExecContext(c.Context(), `
CREATE TABLE words (tenant integer PRIMARY KEY, word text) PARTITION BY RANGE (tenant);
CREATE TABLE words_20261020 PARTITION OF words FOR VALUES FROM (0) TO (10);
CREATE TABLE words_20261021 PARTITION OF words FOR VALUES FROM (10) TO (20);
INSERT INTO words VALUES (7, 'keep');
`)
	c.Assert(err, qt.IsNil)
	var originalChildren string
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT array_agg(inhrelid ORDER BY inhrelid)::text FROM pg_inherits WHERE inhparent = 'words'::regclass").Scan(&originalChildren), qt.IsNil)
	source := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE words (tenant integer PRIMARY KEY, word text) PARTITION BY RANGE (tenant);\n"), 0o600), qt.IsNil)
	plan := filepath.Join(c.TempDir(), "plan.json")
	out, err := runPtahNativeWithError("schema", "plan", "--db-url", dev.url, "--schema-file", source, "--exclude", "words_[0-9]*", "--output", plan)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE words (tenant integer PRIMARY KEY, word text, note text) PARTITION BY RANGE (tenant);\n"), 0o600), qt.IsNil)
	out, err = runPtahNativeWithError("schema", "plan", "--db-url", dev.url, "--schema-file", source, "--exclude", "words_[0-9]*", "--output", plan)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "ADD COLUMN")
	c.Assert(out, qt.Not(qt.Contains), "DROP TABLE")
	out, err = runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--plan", plan, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var children, word string
	var columns int
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT array_agg(inhrelid ORDER BY inhrelid)::text FROM pg_inherits WHERE inhparent = 'words'::regclass").Scan(&children), qt.IsNil)
	c.Assert(children, qt.Equals, originalChildren)
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT word FROM words WHERE tenant = 7").Scan(&word), qt.IsNil)
	c.Assert(word, qt.Equals, "keep")
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM information_schema.columns WHERE table_name IN ('words', 'words_20261020', 'words_20261021') AND column_name = 'note'").Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, 3)
	out, err = runPtahNativeWithError("schema", "plan", "--db-url", dev.url, "--schema-file", source, "--exclude", "words_[0-9]*")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced")
}

//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPartitionOwnerApplyAndReplanPostgresE2E(t *testing.T) {
	for _, key := range []string{`RANGE (tenant)`, `LIST (lower(word))`, `HASH (tenant)`, `RANGE (word COLLATE "C" text_pattern_ops)`, `RANGE (tenant pg_catalog.int4_ops)`} {
		t.Run(key, func(t *testing.T) {
			c := qt.New(t)
			dev := postgresUnloggedDatabase(c)
			checkPartitionOwnerCycle(c, dev, key)
		})
	}
}

func TestPartitionOwnerApplyAndReplanYugabyteDBE2E(t *testing.T) {
	for _, key := range []string{`RANGE (tenant)`, `LIST (lower(word))`, `HASH (tenant)`, `RANGE (word COLLATE "C" text_pattern_ops)`, `RANGE (tenant pg_catalog.int4_ops)`} {
		t.Run(key, func(t *testing.T) { c := qt.New(t); checkPartitionOwnerCycle(c, yugabyteDevDatabase(c), key) })
	}
}

func checkPartitionOwnerCycle(c *qt.C, dev devDialectDatabase, key string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE words (tenant integer, word varchar(20)) PARTITION BY "+key+";\nCOMMENT ON TABLE words IS 'partition probe';\n"), 0o600), qt.IsNil)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
	var oid, keyBefore string
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT 'words'::regclass::oid::text, pg_get_partkeydef('words'::regclass)").Scan(&oid, &keyBefore), qt.IsNil)
	c.Assert(keyBefore, qt.Not(qt.Equals), "")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE words (tenant integer, word varchar(20)) PARTITION BY LIST (word);\n"), 0o600), qt.IsNil)
	out, err = runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "partition key cannot change in place")
	var after, comment string
	c.Assert(dev.conn.QueryRowContext(c.Context(), "SELECT 'words'::regclass::oid::text, obj_description('words'::regclass, 'pg_class')").Scan(&after, &comment), qt.IsNil)
	c.Assert(after, qt.Equals, oid)
	c.Assert(comment, qt.Equals, "partition probe")
}

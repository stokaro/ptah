//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const postgresCatalogNamesSchema = `CREATE TYPE public.point AS (x integer, y integer);
CREATE DOMAIN public.money AS numeric(10, 2) CHECK (VALUE >= 0);
CREATE TABLE pg_settings (
  id integer PRIMARY KEY,
  position public.point,
  positions public.point[],
  amount public.money,
  builtin_point point,
  builtin_money money
);
CREATE TABLE settings_links (
  id integer PRIMARY KEY,
  setting_id integer REFERENCES pg_settings (id)
);
COMMENT ON TABLE pg_settings IS 'Application settings';
GRANT SELECT ON TABLE pg_settings TO PUBLIC;
`

// The generated files must keep every reference in the user's schema even
// when pg_catalog contains the same name. A pinned search_path still searches
// pg_catalog first, so both URL scopes exercise the complete migration cycle.
func TestPostgresCatalogNamesMigrationE2E(t *testing.T) {
	for _, scope := range []struct {
		name   string
		schema string
	}{
		{name: "realm"},
		{name: "pinned", schema: "public"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			c := qt.New(t)
			target := catalogNamesDatabase(c, scope.schema, "ptah_catalog_names_target")
			shadow := catalogNamesDatabase(c, scope.schema, "ptah_catalog_names_shadow")
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			source := filepath.Join(work, "schema.sql")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(source, []byte(postgresCatalogNamesSchema), 0o600), qt.IsNil)

			runPtahNative(c, "migrations", "generate", "--db-url", target, "--shadow-db", shadow,
				"--schema-file", source, "--migrations-dir", dir, "--name", "catalog_names")
			pairs := readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 1)
			c.Assert(pairs[0].Up, qt.Contains, `REFERENCES "public"."pg_settings"`)
			c.Assert(pairs[0].Up, qt.Contains, `COMMENT ON TABLE "public"."pg_settings"`)
			c.Assert(pairs[0].Up, qt.Contains, `GRANT SELECT ON TABLE "public"."pg_settings"`)
			c.Assert(pairs[0].Down, qt.Contains, `DROP TABLE "public"."pg_settings"`)
			c.Assert(pairs[0].Down, qt.Contains, `DROP TYPE "public"."point"`)

			runPtahNative(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
			db, err := sql.Open("pgx", target)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			c.Assert(queryRows(c, db, `SELECT a.attname || ':' || n.nspname || '.' || ty.typname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
JOIN pg_catalog.pg_namespace n ON n.oid = ty.typnamespace
WHERE a.attrelid = 'public.pg_settings'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attname`), qt.DeepEquals, []string{
				"amount:public.money", "builtin_money:pg_catalog.money", "builtin_point:pg_catalog.point",
				"id:pg_catalog.int4", "position:public.point", "positions:public._point",
			})
			c.Assert(runPtahNative(c, "schema", "apply", "--dry-run", "--db-url", target,
				"--schema-file", source), qt.Contains, "Schema is synced, no changes to be made.")

			changed := strings.Replace(postgresCatalogNamesSchema, "builtin_money money", "builtin_money money, note text", 1)
			c.Assert(os.WriteFile(source, []byte(changed), 0o600), qt.IsNil)
			runPtahNative(c, "migrations", "generate", "--db-url", target, "--shadow-db", shadow,
				"--schema-file", source, "--migrations-dir", dir, "--name", "add_note")
			pairs = readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 2)
			c.Assert(pairs[1].Up, qt.Contains, `ALTER TABLE "public"."pg_settings"`)
			c.Assert(pairs[1].Down, qt.Contains, `ALTER TABLE "public"."pg_settings"`)
			runPtahNative(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
			c.Assert(runPtahNative(c, "schema", "apply", "--dry-run", "--db-url", target,
				"--schema-file", source), qt.Contains, "Schema is synced, no changes to be made.")

			runPtahNative(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")
			c.Assert(queryRows(c, db, `SELECT n.nspname || '.' || t.typname
FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname IN ('public', 'pg_catalog') AND t.typname IN ('point', 'money') ORDER BY 1`),
				qt.DeepEquals, []string{"pg_catalog.money", "pg_catalog.point"})
			c.Assert(queryRows(c, db, `SELECT n.nspname || '.' || cl.relname
FROM pg_catalog.pg_class cl JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace
WHERE n.nspname IN ('public', 'pg_catalog') AND cl.relname = 'pg_settings' ORDER BY 1`),
				qt.DeepEquals, []string{"pg_catalog.pg_settings"})
		})
	}
}

func catalogNamesDatabase(c *qt.C, schema, prefix string) string {
	c.Helper()
	raw := postgresScratchDevURL(c, prefix)
	if schema == "" {
		return raw
	}
	return pinnedDevURL(c, raw, schema)
}

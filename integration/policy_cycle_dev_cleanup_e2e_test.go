//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// policyCycleFirst holds two tables whose row-security policies read each
// other's table, so neither can be dropped with RESTRICT while the other's
// policy is there.
const policyCycleFirst = `CREATE TABLE a (id bigint PRIMARY KEY, who text);
CREATE TABLE b (id bigint PRIMARY KEY, who text);
ALTER TABLE a ENABLE ROW LEVEL SECURITY;
ALTER TABLE b ENABLE ROW LEVEL SECURITY;
CREATE POLICY pa ON a USING (EXISTS (SELECT 1 FROM b WHERE b.who = current_user));
CREATE POLICY pb ON b USING (EXISTS (SELECT 1 FROM a WHERE a.who = current_user));
`

// policyCycleSecond keeps the cycle and adds a table.
const policyCycleSecond = policyCycleFirst + "CREATE TABLE other (id bigint PRIMARY KEY);\n"

// policyCycleWriter writes the migration for one schema into dir on dev.
type policyCycleWriter func(c *qt.C, dir, dev, name, schema string)

func policyCycleCompat(c *qt.C, dir, dev, name, schema string) {
	c.Helper()
	output, err := runCompatVerb("migrate", "diff", name,
		"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
		"--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
}

func policyCycleNative(c *qt.C, dir, dev, name, schema string) {
	c.Helper()
	runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", dev,
		"--schema-file", schema, "--migrations-dir", dir, "--name", name)
}

// policyCycleCatalog names the tables of the public schema with their
// row-security switch, and the policies on them.
func policyCycleCatalog(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT c.relname || ' rls=' || c.relrowsecurity
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
UNION ALL
SELECT tablename || '.' || policyname FROM pg_policies WHERE schemaname = 'public'
ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		result = append(result, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

// TestPolicyCycleDevCleanupE2E_TheSecondStepReplaysTheCycle diffs two steps
// whose tables hold policies that read each other's table, through `ptah-compat
// migrate diff` into a golang-migrate directory and through `ptah migrations
// generate --replay`. The second step replays the first on the dev database
// and empties it afterwards, which drops the policies before the tables, so
// the step writes its files. The files then run up, down to an empty schema
// and up again, with the catalog read after each.
func TestPolicyCycleDevCleanupE2E_TheSecondStepReplaysTheCycle(t *testing.T) {
	first := []string{"a rls=true", "a.pa", "b rls=true", "b.pb"}
	second := []string{"a rls=true", "a.pa", "b rls=true", "b.pb", "other rls=false"}
	for _, writer := range []struct {
		name  string
		write policyCycleWriter
	}{
		{name: "migrate diff golang-migrate", write: policyCycleCompat},
		{name: "migrations generate", write: policyCycleNative},
	} {
		t.Run(writer.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_policy_cycle_dev"), "public")
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for _, step := range []struct{ name, schema string }{{"s1", policyCycleFirst}, {"s2", policyCycleSecond}} {
				path := filepath.Join(work, step.name+".sql")
				c.Assert(os.WriteFile(path, []byte(step.schema), 0o600), qt.IsNil)
				writer.write(c, dir, dev, step.name, path)
			}
			pairs := readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 2)
			target, err := sql.Open("pgx", postgresScratchDevURL(c, "ptah_policy_cycle_target"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
			steps := []struct {
				sql  string
				want []string
			}{
				{pairs[0].Up, first},
				{pairs[1].Up, second},
				{pairs[1].Down, first},
				{pairs[0].Down, nil},
				{pairs[0].Up, first},
				{pairs[1].Up, second},
			}
			for _, step := range steps {
				_, err := target.ExecContext(c.Context(), step.sql)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", step.sql))
				c.Assert(policyCycleCatalog(c, target), qt.DeepEquals, step.want, qt.Commentf("%s", step.sql))
			}
		})
	}
}

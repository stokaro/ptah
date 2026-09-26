//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// foreignKeyRenames start from a migration holding one set of foreign keys and
// declare, in the schema file, keys whose name or definition differs. Each row
// ends with the foreign keys the schema file builds on an empty database, and
// the plan drops the key it does not declare. Excusing a live key because its
// column declares one adds the declared key beside it and never drops the live
// one, so the table holds two (stokaro/ptah#3718). Atlas CE v1.3.0 ends every
// row with the same keys.
//
// The rows whose ON DELETE changes are the controls: their definitions differ
// as well, so they are planned whatever decides the pairing.
var foreignKeyRenames = []struct {
	name      string
	migration string
	schema    string
	wantDrop  string
	wantAdd   string
	want      string
}{
	{
		name: "a named key the schema declares on the column without a name",
		migration: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL,
  CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`,
		schema: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL REFERENCES p(id) ON DELETE CASCADE);`,
		wantDrop: `DROP CONSTRAINT IF EXISTS "c_p_fk"`,
		wantAdd:  `ADD CONSTRAINT "c_p_id_fkey"`,
		want:     "c:c_p_id_fkey(p_id)->p CASCADE",
	},
	{
		name: "a named key the schema renames on the column",
		migration: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL,
  CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`,
		schema: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL CONSTRAINT c_p_new REFERENCES p(id) ON DELETE CASCADE);`,
		wantDrop: `DROP CONSTRAINT IF EXISTS "c_p_fk"`,
		wantAdd:  `ADD CONSTRAINT "c_p_new"`,
		want:     "c:c_p_new(p_id)->p CASCADE",
	},
	{
		name: "a composite key the schema replaces with a key on its first column",
		migration: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE q (a bigint, b bigint, UNIQUE (a, b));
CREATE TABLE c (id bigint PRIMARY KEY, a bigint, b bigint,
  CONSTRAINT c_ab_fk FOREIGN KEY (a, b) REFERENCES q(a, b));`,
		schema: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE q (a bigint, b bigint, UNIQUE (a, b));
CREATE TABLE c (id bigint PRIMARY KEY, a bigint REFERENCES p(id), b bigint);`,
		wantDrop: `DROP CONSTRAINT IF EXISTS "c_ab_fk"`,
		wantAdd:  `ADD CONSTRAINT "c_a_fkey"`,
		want:     "c:c_a_fkey(a)->p NO ACTION",
	},
	{
		name: "the column key's ON DELETE changes",
		migration: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL REFERENCES p(id) ON DELETE CASCADE);`,
		schema: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL REFERENCES p(id) ON DELETE RESTRICT);`,
		wantDrop: `DROP CONSTRAINT IF EXISTS "c_p_id_fkey"`,
		wantAdd:  `ADD CONSTRAINT "c_p_id_fkey"`,
		want:     "c:c_p_id_fkey(p_id)->p RESTRICT",
	},
	{
		name: "a named key the schema declares on the column with another ON DELETE",
		migration: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL,
  CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`,
		schema: `CREATE TABLE p (id bigint PRIMARY KEY);
CREATE TABLE c (id bigint PRIMARY KEY, p_id bigint NOT NULL REFERENCES p(id) ON DELETE RESTRICT);`,
		wantDrop: `DROP CONSTRAINT IF EXISTS "c_p_fk"`,
		wantAdd:  `ADD CONSTRAINT "c_p_id_fkey"`,
		want:     "c:c_p_id_fkey(p_id)->p RESTRICT",
	},
}

// readForeignKeys answers the foreign keys of the current schema as
// `table:name(columns)->referenced ON DELETE`, in name order.
func readForeignKeys(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var keys string
	err = conn.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(
			conrelid::regclass::text || ':' || conname || '(' ||
			(SELECT string_agg(attname, ',' ORDER BY array_position(conkey, attnum)) FROM pg_attribute
			 WHERE attrelid = conrelid AND attnum = ANY (conkey)) || ')->' ||
			confrelid::regclass::text || ' ' ||
			CASE confdeltype
				WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL' ELSE 'SET DEFAULT'
			END,
			' ' ORDER BY conname), '')
		FROM pg_constraint
		WHERE contype = 'f' AND connamespace = current_schema()::regnamespace`,
	).Scan(&keys)
	c.Assert(err, qt.IsNil)
	return keys
}

// TestMigrateDiffReplacesAForeignKeyTheSchemaNamesOtherwiseE2E writes the
// migration `migrate diff` plans, applies the directory to an empty database,
// and diffs again: the database holds the declared keys and nothing else, and
// the directory is synced.
func TestMigrateDiffReplacesAForeignKeyTheSchemaNamesOtherwiseE2E(t *testing.T) {
	for _, test := range foreignKeyRenames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			dev, _ := scratchReplayDatabase(c)
			target, _ := scratchReplayDatabase(c)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			out, err = runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			again, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))

			c.Assert(plan, qt.Contains, test.wantDrop)
			c.Assert(plan, qt.Contains, test.wantAdd)
			c.Assert(readForeignKeys(c, target), qt.Equals, test.want)
			c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaApplyReplacesAForeignKeyTheSchemaNamesOtherwiseE2E applies the
// schema file natively to the database the migration built, and plans again:
// the database holds the declared keys and nothing else, and the schema is
// synced.
func TestSchemaApplyReplacesAForeignKeyTheSchemaNamesOtherwiseE2E(t *testing.T) {
	for _, test := range foreignKeyRenames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := databaseBuiltFrom(c, test.migration)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, test.wantDrop)
			c.Assert(plan, qt.Contains, test.wantAdd)
			c.Assert(readForeignKeys(c, target), qt.Equals, test.want)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

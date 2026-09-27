//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// postgresMergedKeys are schema files that declare one key more than once, with
// the keys of `c` PostgreSQL 18.6 built from each, read back from pg_constraint
// as `name(type)`. The server builds identical declarations of one CREATE TABLE
// as one key: the primary key where there is one, otherwise the first, under
// the first name the declarations give. Atlas CE v1.3.0 reports each file
// synced with the database it built. The rows marked control are keys the
// server builds apart, and a native apply to an empty database has to build
// them apart too.
var postgresMergedKeys = []struct {
	name   string
	schema string
	want   string
}{
	{
		name:   "a column's UNIQUE and a named UNIQUE",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));`,
		want:   "c_pkey(p) uq_a(u)",
	},
	{
		name:   "two named UNIQUEs",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT u1 UNIQUE (a), CONSTRAINT u2 UNIQUE (a));`,
		want:   "c_pkey(p) u1(u)",
	},
	{
		name:   "an unnamed UNIQUE and a named one",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE (a), CONSTRAINT u2 UNIQUE (a));`,
		want:   "c_pkey(p) u2(u)",
	},
	{
		name:   "two unnamed UNIQUEs",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE (a), UNIQUE (a));`,
		want:   "c_a_key(u) c_pkey(p)",
	},
	{
		name:   "a UNIQUE on the primary key column",
		schema: `CREATE TABLE c (id int PRIMARY KEY UNIQUE, a int);`,
		want:   "c_pkey(p)",
	},
	{
		name:   "a primary key and a named UNIQUE",
		schema: `CREATE TABLE c (id int, a int, PRIMARY KEY (a), CONSTRAINT uq_a UNIQUE (a));`,
		want:   "uq_a(p)",
	},
	{
		name:   "a column's primary key and two named UNIQUEs",
		schema: `CREATE TABLE c (id int, a int PRIMARY KEY, CONSTRAINT u1 UNIQUE (a), CONSTRAINT u2 UNIQUE (a));`,
		want:   "u1(p)",
	},
	{
		name:   "a primary key over two columns and a UNIQUE over them",
		schema: `CREATE TABLE c (id int, a int, b int, PRIMARY KEY (a, b), UNIQUE (a, b));`,
		want:   "c_pkey(p)",
	},
	{
		name: "control: a column's UNIQUE, then a named UNIQUE in a later statement",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE);
ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);`,
		want: "c_a_key(u) c_pkey(p) uq_a(u)",
	},
	{
		name: "control: a column's UNIQUE, then a primary key over it in a later statement",
		schema: `CREATE TABLE c (id int UNIQUE);
ALTER TABLE c ADD PRIMARY KEY (id);`,
		want: "c_id_key(u) c_pkey(p)",
	},
	{
		name:   "control: columns in another order",
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT u1 UNIQUE (a, b), CONSTRAINT u2 UNIQUE (b, a));`,
		want:   "c_pkey(p) u1(u) u2(u)",
	},
	{
		name:   "control: NULLS NOT DISTINCT on one",
		schema: `CREATE TABLE c (id int, a int, PRIMARY KEY (a), CONSTRAINT u UNIQUE NULLS NOT DISTINCT (a));`,
		want:   "c_pkey(p) u(u)",
	},
}

// readKeysOfC answers the primary key and UNIQUE constraints of `c` in the
// current schema as `name(type)`, in name order.
func readKeysOfC(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var keys string
	err = conn.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(conname || '(' || contype::text || ')', ' ' ORDER BY conname), '')
		FROM pg_constraint
		WHERE contype IN ('p', 'u') AND conrelid = 'c'::regclass`,
	).Scan(&keys)
	c.Assert(err, qt.IsNil)
	return keys
}

// TestMigrateDiffFindsPostgresMergedKeysSyncedE2E replays a directory whose
// one migration is the schema file itself and diffs: the directory is synced.
// Applied to an empty database, the directory builds the keys the row names.
func TestMigrateDiffFindsPostgresMergedKeysSyncedE2E(t *testing.T) {
	for _, test := range postgresMergedKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			dev, _ := scratchReplayDatabase(c)
			target, _ := scratchReplayDatabase(c)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
			c.Assert(readKeysOfC(c, target), qt.Equals, test.want)
		})
	}
}

// TestSchemaApplyFindsPostgresMergedKeysSyncedE2E plans the native apply of
// the schema file against the database the same file built, with nothing of
// Ptah's in between: nothing is planned.
func TestSchemaApplyFindsPostgresMergedKeysSyncedE2E(t *testing.T) {
	for _, test := range postgresMergedKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			target := databaseBuiltFrom(c, test.schema)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(readKeysOfC(c, target), qt.Equals, test.want)
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyBuildsPostgresMergedKeysE2E applies the schema file natively
// to an empty database: Ptah's own statements build the keys the file builds
// when run by hand, and a second plan finds nothing to do.
func TestSchemaApplyBuildsPostgresMergedKeysE2E(t *testing.T) {
	for _, test := range postgresMergedKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			target, _ := scratchReplayDatabase(c)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(readKeysOfC(c, target), qt.Equals, test.want)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

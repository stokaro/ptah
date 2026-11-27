//go:build integration

package integration_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// createdBuiltinNameEntities declares a domain, a composite type and an enum
// named like types pg_catalog holds on PostgreSQL and YugabyteDB, and a table
// named like a catalog view. Each is created in public, beside the catalog's
// object of the same name.
const createdBuiltinNameEntities = `package entities

//ptah:schema:domain name="money" type="NUMERIC(12,2)"
type MoneyDomain struct{}

//ptah:schema:composite name="point" fields="x:INTEGER,y:INTEGER"
type PointType struct{}

//ptah:schema:enum name="box" values="small,large"
type BoxEnum struct{}

//ptah:schema:table name="pg_settings"
type Settings struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`

// createdCatalogNameCockroachEntities is the same on CockroachDB, which
// refuses a type named like one of its builtin types as a type that already
// exists: an enum named like the row type of a catalog view, and a table
// named like the PostGIS table CockroachDB keeps in pg_extension.
const createdCatalogNameCockroachEntities = `package entities

//ptah:schema:enum name="pg_settings" values="small,large"
type SettingsEnum struct{}

//ptah:schema:table name="spatial_ref_sys"
type SpatialRefSys struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`

// createdNamesQuery lists every type and relation of the names in $1 with its
// schema, so the catalog's object and the one the migration created both
// show.
const createdNamesQuery = `SELECT pg_catalog.string_agg(o.name, ' ' ORDER BY o.name) FROM (
	SELECT n.nspname || '.' || t.typname || ' type' AS name
	FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
	WHERE t.typname = ANY ($1) AND t.typtype IN ('b', 'c', 'd', 'e')
	UNION ALL
	SELECT n.nspname || '.' || c.relname || ' relation'
	FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
	WHERE c.relname = ANY ($1) AND c.relkind IN ('r', 'v')
) AS o`

// TestReplayCreatedBuiltinNameE2E_HappyPath generates a migration from
// entities whose names pg_catalog also holds, validates it on a dev database,
// applies it, and replays the history on a shadow database through migrations
// drift and a second migrations generate, none of it trusted. It reads the
// target back: each object is in public, and the catalog's object of the same
// name is still there. A CREATE puts an object whose name has no schema in the
// session's creation schema, so the guard judges it by that schema rather than
// by what the name reaches.
//
// The first generate runs without a shadow database. Its round trip replays
// the down migration, which drops each object by its bare name, and a DROP is
// a reference: the server resolves `DROP TYPE "point"` to pg_catalog.point and
// refuses it as required by the database system, and the guard refuses it
// first. The second generate writes nothing, so it has no down to replay.
func TestReplayCreatedBuiltinNameE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		entities string
		creates  string
		names    []string
		want     string
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, entities: createdBuiltinNameEntities,
			creates: `CREATE TYPE "point" AS`,
			names:   []string{"money", "point", "box", "pg_settings"},
			want: "pg_catalog.box type pg_catalog.money type pg_catalog.pg_settings relation pg_catalog.pg_settings type " +
				"pg_catalog.point type public.box type public.money type public.pg_settings relation public.pg_settings type " +
				"public.point type"},
		{name: "PostgreSQL 16", engine: dbtarget.PostgreSQL16, entities: createdBuiltinNameEntities,
			creates: `CREATE TYPE "point" AS`,
			names:   []string{"money", "point", "box", "pg_settings"},
			want: "pg_catalog.box type pg_catalog.money type pg_catalog.pg_settings relation pg_catalog.pg_settings type " +
				"pg_catalog.point type public.box type public.money type public.pg_settings relation public.pg_settings type " +
				"public.point type"},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB, entities: createdBuiltinNameEntities,
			creates: `CREATE TYPE "point" AS`,
			names:   []string{"money", "point", "box", "pg_settings"},
			want: "pg_catalog.box type pg_catalog.money type pg_catalog.pg_settings relation pg_catalog.pg_settings type " +
				"pg_catalog.point type public.box type public.money type public.pg_settings relation public.pg_settings type " +
				"public.point type"},
		{name: "CockroachDB", engine: dbtarget.CockroachDB, entities: createdCatalogNameCockroachEntities,
			creates: `CREATE TYPE "pg_settings" AS ENUM`,
			names:   []string{"pg_settings", "spatial_ref_sys"},
			want: "pg_catalog.pg_settings relation pg_catalog.pg_settings type pg_extension.spatial_ref_sys relation " +
				"pg_extension.spatial_ref_sys type public.pg_settings type public.spatial_ref_sys relation public.spatial_ref_sys type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, test.engine)
			entities := writePgDumpEntities(c, h, "entities", test.entities)

			pgDumpRun(c, h, "migrations", "generate", "--db-url", h.target, "--migrations-dir", h.dir,
				"--root-dir", entities, "--name", "builtins")
			up, down := takeGeneratedPgDumpMigration(c, h, "builtins")
			writePgDumpMigration(c, h, 1, "builtins", up, down)
			validated := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)
			pgDumpRun(c, h, "migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir)
			drift := pgDumpRun(c, h, "migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir,
				"--shadow-db", h.shadow)
			again := pgDumpRun(c, h, "migrations", "generate", "--db-url", h.target, "--migrations-dir", h.dir,
				"--root-dir", entities, "--name", "again", "--shadow-db", h.shadow)

			c.Assert(up, qt.Contains, test.creates)
			c.Assert(validated, qt.Contains, "migration SQL validated on dev database")
			c.Assert(drift, qt.Contains, "No drift")
			c.Assert(again, qt.Contains, "no migration files generated")
			c.Assert(pgDumpQuery[string](c, h.target, createdNamesQuery, test.names), qt.Equals, test.want)
			c.Assert(pgDumpQuery[int](c, dev, createdPublicNamesQuery, test.names), qt.Equals, 0)
		})
	}
}

// createdPublicNamesQuery counts the types and relations of the names in $1
// in public.
const createdPublicNamesQuery = `SELECT (SELECT count(*) FROM pg_catalog.pg_type AS t
	JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace WHERE n.nspname = 'public' AND t.typname = ANY ($1)) +
	(SELECT count(*) FROM pg_catalog.pg_class AS c
	JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = ANY ($1))`

// createdInCatalogType is a type with a name no schema holds. A session
// whose search path lists pg_catalog first creates it there: PostgreSQL lets
// a superuser create a type in pg_catalog, and the cleanup of the dev
// database does not remove it.
const createdInCatalogType = "CREATE TYPE ptah_created_in_catalog AS ENUM ('a');\n"

// TestReplayCreatedNameE2E_FailurePath validates, with no trust, a CREATE
// whose name has no schema on a dev URL whose options start the search path
// with pg_catalog, and reads the dev database back: the type the CREATE would
// put in pg_catalog does not exist. The run refuses the dev database before
// it replays a statement, as a realm whose root schema is pg_catalog, so the
// guard's refusal of such a session is held by its unit tests.
func TestReplayCreatedNameE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.PostgreSQL)
	catalogFirst := withPostgresOptions(c, dev, "-csearch_path=pg_catalog,public")

	got := pgDumpRefused(c, h, catalogFirst, createdInCatalogType)

	c.Assert(got, qt.Contains, `refusing to clean PostgreSQL database realm with system root schema "pg_catalog"`)
	c.Assert(pgDumpQuery[int](c, dev,
		"SELECT count(*) FROM pg_catalog.pg_type WHERE typname = 'ptah_created_in_catalog'"), qt.Equals, 0)
}

// TestReplayCreatedNameE2E_HappyPath is the control for the test above: the
// same CREATE on a session with the default search path replays, and the dev
// database ends without the type.
func TestReplayCreatedNameE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.PostgreSQL)
	writePgDumpMigration(c, h, 1, "created", createdInCatalogType, "DROP TYPE ptah_created_in_catalog;\n")

	got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)

	c.Assert(got, qt.Contains, "migration SQL validated on dev database")
	c.Assert(pgDumpQuery[int](c, dev,
		"SELECT count(*) FROM pg_catalog.pg_type WHERE typname = 'ptah_created_in_catalog'"), qt.Equals, 0)
}

// withPostgresOptions returns rawURL with options, the startup parameter that
// sets the session's parameters at connection.
func withPostgresOptions(c *qt.C, rawURL, options string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("options", options)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

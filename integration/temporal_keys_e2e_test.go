//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// temporalKeysSchema declares a room table keyed WITHOUT OVERLAPS and a booking
// table with a WITHOUT OVERLAPS unique constraint and a PERIOD foreign key onto
// the rooms. The keys have a scalar first column, so the server builds them on
// a GiST index that needs btree_gist.
const temporalKeysSchema = `CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE rooms (id integer NOT NULL, valid_at daterange NOT NULL, label text,
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));
CREATE TABLE bookings (id integer PRIMARY KEY, room_id integer NOT NULL, valid_at daterange NOT NULL,
  CONSTRAINT bookings_uq UNIQUE (room_id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT bookings_room_fk FOREIGN KEY (room_id, PERIOD valid_at) REFERENCES rooms (id, PERIOD valid_at));
`

// plainKeysSchema is the same tables with ordinary keys.
const plainKeysSchema = `CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE rooms (id integer NOT NULL, valid_at daterange NOT NULL, label text,
  PRIMARY KEY (id, valid_at));
CREATE TABLE bookings (id integer PRIMARY KEY, room_id integer NOT NULL, valid_at daterange NOT NULL,
  CONSTRAINT bookings_uq UNIQUE (room_id, valid_at),
  CONSTRAINT bookings_room_fk FOREIGN KEY (room_id, valid_at) REFERENCES rooms (id, valid_at));
`

// temporalKeysReadBack lists each key of the two tables with the flag the
// catalog keeps for it, pg_constraint.conperiod, and the definition the server
// prints. It is the server's own answer, read with no help from Ptah.
const temporalKeysReadBack = `SELECT conname || ' ' || contype::text || ' conperiod=' || conperiod::text || ' ' || pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid IN ('rooms'::regclass, 'bookings'::regclass) AND contype IN ('p', 'u', 'f')
ORDER BY 1`

var temporalKeysHeld = []string{
	"bookings_pkey p conperiod=false PRIMARY KEY (id)",
	"bookings_room_fk f conperiod=true FOREIGN KEY (room_id, PERIOD valid_at) REFERENCES rooms(id, PERIOD valid_at)",
	"bookings_uq u conperiod=true UNIQUE (room_id, valid_at WITHOUT OVERLAPS)",
	"rooms_pkey p conperiod=true PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
}

var plainKeysHeld = []string{
	"bookings_pkey p conperiod=false PRIMARY KEY (id)",
	"bookings_room_fk f conperiod=false FOREIGN KEY (room_id, valid_at) REFERENCES rooms(id, valid_at)",
	"bookings_uq u conperiod=false UNIQUE (room_id, valid_at)",
	"rooms_pkey p conperiod=false PRIMARY KEY (id, valid_at)",
}

// temporalKeysDatabase is a scratch database on the PostgreSQL 18 test server,
// with sql run in it.
func temporalKeysDatabase(c *qt.C, sql string) string {
	c.Helper()
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	if sql != "" {
		applyEngineFile(c, database, "postgres", sql)
	}
	return database
}

func writeTemporalSchema(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyBuildsTemporalKeysE2E applies a schema file that declares a
// WITHOUT OVERLAPS primary key and unique constraint and a PERIOD foreign key to
// an empty PostgreSQL 18 database. The catalog holds the keys with
// pg_constraint.conperiod set and the definition the file's SQL gives, and the
// next apply is synced.
func TestSchemaApplyBuildsTemporalKeysE2E(t *testing.T) {
	c := qt.New(t)
	database := temporalKeysDatabase(c, "")
	schema := writeTemporalSchema(c, temporalKeysSchema)

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
	again := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(catalogReadBack(c, database, []string{temporalKeysReadBack}), qt.DeepEquals, [][]string{temporalKeysHeld})
}

// TestTemporalKeysRefuseWhatTheyAreMeantToRefuseE2E builds the keys through
// Ptah and then writes rows: an overlapping room row is refused by the primary
// key, a booking not covered by the room's ranges is refused by the foreign key,
// and a booking covered by two adjacent room rows is taken, which PERIOD allows
// and a plain foreign key cannot.
func TestTemporalKeysRefuseWhatTheyAreMeantToRefuseE2E(t *testing.T) {
	c := qt.New(t)
	database := temporalKeysDatabase(c, "")
	schema := writeTemporalSchema(c, temporalKeysSchema)
	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	conn := connectDevDialect(c, database)
	_, err := conn.ExecContext(c.Context(), `INSERT INTO rooms (id, valid_at) VALUES
  (1, '[2026-01-01,2026-02-01)'), (1, '[2026-02-01,2026-03-01)'), (2, '[2026-01-01,2026-02-01)')`)
	c.Assert(err, qt.IsNil, qt.Commentf("adjacent and disjoint ranges are not overlaps"))

	_, overlapping := conn.ExecContext(c.Context(), `INSERT INTO rooms (id, valid_at) VALUES (1, '[2026-01-15,2026-02-15)')`)
	_, otherRoom := conn.ExecContext(c.Context(), `INSERT INTO rooms (id, valid_at) VALUES (3, '[2026-01-15,2026-02-15)')`)
	_, covered := conn.ExecContext(c.Context(), `INSERT INTO bookings (id, room_id, valid_at) VALUES (1, 1, '[2026-01-20,2026-02-10)')`)
	_, uncovered := conn.ExecContext(c.Context(), `INSERT INTO bookings (id, room_id, valid_at) VALUES (2, 2, '[2026-01-20,2026-02-10)')`)
	_, doubleBooked := conn.ExecContext(c.Context(), `INSERT INTO bookings (id, room_id, valid_at) VALUES (3, 1, '[2026-02-05,2026-02-20)')`)

	c.Assert(overlapping, qt.ErrorMatches, `.*conflicting key value violates exclusion constraint "rooms_pkey".*`)
	c.Assert(otherRoom, qt.IsNil)
	c.Assert(covered, qt.IsNil)
	c.Assert(uncovered, qt.ErrorMatches, `.*insert or update on table "bookings" violates foreign key constraint "bookings_room_fk".*`)
	c.Assert(doubleBooked, qt.ErrorMatches, `.*conflicting key value violates exclusion constraint "bookings_uq".*`)
}

// TestSchemaApplyReplacesKeysWhenTheTemporalClauseChangesE2E moves a database
// between ordinary keys and temporal ones in both directions. PostgreSQL cannot
// change the clause in place, so the plan drops and adds the keys, and the
// catalog then holds exactly the keys the file's own SQL builds.
func TestSchemaApplyReplacesKeysWhenTheTemporalClauseChangesE2E(t *testing.T) {
	for _, direction := range []struct {
		name, from, to string
		held           []string
	}{
		{name: "ordinary to temporal", from: plainKeysSchema, to: temporalKeysSchema, held: temporalKeysHeld},
		{name: "temporal to ordinary", from: temporalKeysSchema, to: plainKeysSchema, held: plainKeysHeld},
	} {
		t.Run(direction.name, func(t *testing.T) {
			c := qt.New(t)
			database := temporalKeysDatabase(c, direction.from)
			schema := writeTemporalSchema(c, direction.to)

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
			again := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(catalogReadBack(c, database, []string{temporalKeysReadBack}), qt.DeepEquals, [][]string{direction.held})
		})
	}
}

// TestSchemaDiffFindsTemporalKeysSyncedE2E diffs the database the file's SQL
// built against the file: the keys the server built pair with the file's,
// clause and all. Diffed against the ordinary keys, the same database is not
// synced.
func TestSchemaDiffFindsTemporalKeysSyncedE2E(t *testing.T) {
	c := qt.New(t)
	source := temporalKeysDatabase(c, temporalKeysSchema)
	temporal := writeTemporalSchema(c, temporalKeysSchema)
	plain := writeTemporalSchema(c, plainKeysSchema)

	synced := runPtahIn(c, "schema", "diff", "--from", source, "--to", "file://"+temporal, "--server-version", "18")
	changed := runPtahIn(c, "schema", "diff", "--from", source, "--to", "file://"+plain, "--server-version", "18")

	c.Assert(synced.ExitCode, qt.Equals, 0, historyComment(synced))
	c.Assert(synced.Stdout, qt.Contains, "Schemas are synced")
	c.Assert(changed.Stdout, qt.Not(qt.Contains), "Schemas are synced")
	c.Assert(changed.Stdout, qt.Contains, `ADD CONSTRAINT "bookings_uq" UNIQUE ("room_id", "valid_at")`)
}

// TestSchemaInspectReadsTemporalKeysE2E reads the keys back from the catalog
// into SQL and HCL, and applies the HCL to an empty database: the keys come out
// as they went in.
func TestSchemaInspectReadsTemporalKeysE2E(t *testing.T) {
	c := qt.New(t)
	source := temporalKeysDatabase(c, temporalKeysSchema)

	sql := runPtahIn(c, "schema", "inspect", "--db-url", source, "--format", "sql")
	hcl := runPtahIn(c, "schema", "inspect", "--db-url", source, "--format", "hcl")

	c.Assert(sql.ExitCode, qt.Equals, 0, historyComment(sql))
	c.Assert(sql.Stdout, qt.Contains, `PRIMARY KEY ("id", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(sql.Stdout, qt.Contains, `CONSTRAINT "bookings_uq" UNIQUE ("room_id", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(sql.Stdout, qt.Contains, `FOREIGN KEY ("room_id", PERIOD "valid_at") REFERENCES "rooms"("id", PERIOD "valid_at")`)
	c.Assert(hcl.ExitCode, qt.Equals, 0, historyComment(hcl))
	c.Assert(hcl.Stdout, qt.Contains, "without_overlaps = true")
	c.Assert(hcl.Stdout, qt.Contains, "period = true")
	target := temporalKeysDatabase(c, "")
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte(hcl.Stdout), 0o600), qt.IsNil)
	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", document, "--auto-approve")
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(catalogReadBack(c, target, []string{temporalKeysReadBack}), qt.DeepEquals, [][]string{temporalKeysHeld})
}

// atlasTemporalKeysSchema is temporalKeysSchema in the shape the public Atlas
// HCL reference gives temporal keys: without_overlaps in primary_key and
// unique, period in foreign_key, and on the room key the `type = GIST` the
// reference says a temporal key may leave out.
const atlasTemporalKeysSchema = `schema "public" {}
extension "btree_gist" {
  schema = schema.public
}
table "rooms" {
  schema = schema.public
  column "id" {
    null = false
    type = integer
  }
  column "valid_at" {
    null = false
    type = daterange
  }
  column "label" {
    null = true
    type = text
  }
  primary_key {
    columns          = [column.id, column.valid_at]
    without_overlaps = true
    type             = GIST
  }
}
table "bookings" {
  schema = schema.public
  column "id" {
    null = false
    type = integer
  }
  column "room_id" {
    null = false
    type = integer
  }
  column "valid_at" {
    null = false
    type = daterange
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "bookings_room_fk" {
    columns     = [column.room_id, column.valid_at]
    ref_columns = [table.rooms.column.id, table.rooms.column.valid_at]
    period      = true
  }
  unique "bookings_uq" {
    columns          = [column.room_id, column.valid_at]
    without_overlaps = true
  }
}
`

// TestCompatSchemaApplyReadsAtlasTemporalKeysE2E applies the Atlas-shaped
// document through ptah-compat to an empty PostgreSQL 18 database. The catalog
// then holds the keys the SQL schema builds, and the next apply is synced.
func TestCompatSchemaApplyReadsAtlasTemporalKeysE2E(t *testing.T) {
	c := qt.New(t)
	database := temporalKeysDatabase(c, "")
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte(atlasTemporalKeysSchema), 0o600), qt.IsNil)
	to := "file://" + filepath.ToSlash(document)

	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "-u", database, "--to", to, "--auto-approve")
	again := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "-u", database, "--to", to, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made\n")
	c.Assert(catalogReadBack(c, database, []string{temporalKeysReadBack}), qt.DeepEquals, [][]string{temporalKeysHeld})
}

// temporalKeysModels declares the rooms and bookings of temporalKeysSchema as
// Go annotations.
const temporalKeysModels = `package models

//ptah:schema:extension name="btree_gist" if_not_exists="true"
//ptah:schema:table name="rooms" primary_key="id,valid_at" primary_key_without_overlaps="true"
type Room struct {
	//ptah:schema:field name="id" type="INTEGER" not_null="true"
	ID int
	//ptah:schema:field name="valid_at" type="DATERANGE" not_null="true"
	ValidAt string
	//ptah:schema:field name="label" type="TEXT"
	Label string
}

//ptah:schema:table name="bookings"
//ptah:schema:constraint name="bookings_uq" type="UNIQUE" columns="room_id,valid_at" without_overlaps="true"
//ptah:schema:constraint name="bookings_room_fk" type="FOREIGN KEY" columns="room_id,valid_at" foreign_table="rooms" foreign_columns="id,valid_at" period="true"
type Booking struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="room_id" type="INTEGER" not_null="true"
	RoomID int
	//ptah:schema:field name="valid_at" type="DATERANGE" not_null="true"
	ValidAt string
}
`

// TestMigrationsGenerateTemporalKeysFromGoAnnotationsE2E plans the annotated
// models into a versioned migration, runs it on an empty PostgreSQL 18
// database, and plans again. The catalog holds the keys the SQL schema builds,
// and the second plan writes no migration.
func TestMigrationsGenerateTemporalKeysFromGoAnnotationsE2E(t *testing.T) {
	c := qt.New(t)
	database := temporalKeysDatabase(c, "")
	models := filepath.Join(c.TempDir(), "models")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(models, "rooms.go"), []byte(temporalKeysModels), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "temporal")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)
	again := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "again")

	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Contains, "no migration files generated")
	c.Assert(catalogReadBack(c, database, []string{temporalKeysReadBack}), qt.DeepEquals, [][]string{temporalKeysHeld})
}

// temporalKeysOnlySchema is temporalKeysSchema without the extension, which
// CockroachDB does not have: what it asks of a server is the keys alone.
const temporalKeysOnlySchema = `CREATE TABLE rooms (id integer NOT NULL, valid_at daterange NOT NULL, label text,
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));
CREATE TABLE bookings (id integer PRIMARY KEY, room_id integer NOT NULL, valid_at daterange NOT NULL,
  CONSTRAINT bookings_uq UNIQUE (room_id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT bookings_room_fk FOREIGN KEY (room_id, PERIOD valid_at) REFERENCES rooms (id, PERIOD valid_at));
`

// TestSchemaApplyRefusesTemporalKeysWithoutTheCapabilityE2E applies the
// temporal keys to servers that cannot hold them: PostgreSQL 16, whose grammar
// has no WITHOUT OVERLAPS, and CockroachDB and YugabyteDB, whose grammars have
// no such clause either. Ptah reads the server, finds no temporal_keys
// capability and refuses by the key's name before any statement runs, so the
// database is left with no table. Written without the clause, the keys would
// accept the overlapping rows they exist to refuse.
func TestSchemaApplyRefusesTemporalKeysWithoutTheCapabilityE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		drop   string
		want   string
	}{
		{
			name: "postgresql 16", engine: dbtarget.PostgreSQL16, drop: "DROP DATABASE IF EXISTS %s WITH (FORCE)",
			want: `an unnamed key of table "rooms" declares a PRIMARY KEY with WITHOUT OVERLAPS, ` +
				`which requires target capability temporal_keys, unavailable on this postgres target`,
		},
		{
			name: "cockroachdb", engine: dbtarget.CockroachDB, drop: "DROP DATABASE IF EXISTS %s CASCADE",
			want: `an unnamed key of table "rooms" declares a PRIMARY KEY with WITHOUT OVERLAPS, ` +
				`which requires target capability temporal_keys, unavailable on this cockroachdb target`,
		},
		{
			name: "yugabytedb", engine: dbtarget.YugabyteDB, drop: "DROP DATABASE IF EXISTS %s",
			want: `an unnamed key of table "rooms" declares a PRIMARY KEY with WITHOUT OVERLAPS, ` +
				`which requires target capability temporal_keys, unavailable on this yugabytedb target`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := createdDevDialectDatabase(c, dbtarget.URL(c, test.engine), test.drop, renameInPath)
			schema := writeTemporalSchema(c, temporalKeysOnlySchema)

			got := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 2, historyComment(got))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(postgresFamilyObjectCount(c, c.Context(), database), qt.Equals, 0)
		})
	}
}

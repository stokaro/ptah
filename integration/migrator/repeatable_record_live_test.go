//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A run with repeatable work refuses what would stop its record before any
// migration runs, on a dry run as on an apply. Without that, the versioned
// migration commits and the run fails afterwards, at the record, with a
// preview that had reported nothing wrong.

// foreignMetadataRepeatableTableDDL is the record's own shape, so the squat is
// accepted by CREATE TABLE IF NOT EXISTS and by the read.
const foreignMetadataRepeatableTableDDL = `CREATE TABLE public.schema_migrations_repeatable (
  identity character varying(255) NOT NULL PRIMARY KEY,
  checksum character varying(64) NOT NULL DEFAULT '',
  state character varying(32) NOT NULL,
  attempt_checksum character varying(64) NOT NULL,
  attempt_order integer NOT NULL,
  transactional integer NOT NULL,
  run_id character varying(64) NOT NULL,
  attempted_at timestamp without time zone NOT NULL,
  applied_at timestamp without time zone,
  error text
)`

const repeatableRecordView = "CREATE OR REPLACE VIEW widget_ids AS SELECT id FROM widgets;\n"

// repeatableRecordMigrations is the fixture's versioned migration and the
// given repeatable migrations, keyed by file name.
func repeatableRecordMigrations(repeatables map[string]string) fstest.MapFS {
	fsys := foreignMetadataMigrations()
	for name, body := range repeatables {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// repeatableConnection is a connection of the migration role of its own.
func (f *foreignMetadataFixture) repeatableConnection(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), f.migrationURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// repeatableMigrator loads the repeatable migrations of fsys over conn.
func repeatableMigrator(c *qt.C, conn *dbschema.DatabaseConnection, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	return mig
}

// indexExists asks whether the public schema holds an index named name.
func (f *foreignMetadataFixture) indexExists(c *qt.C, name string) bool {
	c.Helper()
	var count int
	c.Assert(f.adminConn.QueryRowContext(context.Background(),
		`SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`, name).Scan(&count), qt.IsNil)
	return count > 0
}

func TestRepeatableRecordRefusalsLive_FailurePath(t *testing.T) {
	// 55 characters keep the revision table and the log under PostgreSQL's
	// 63-byte limit and put the record, with _repeatable, over it.
	long := strings.Repeat("c", 55)

	t.Run("a record name the target truncates is refused before the versioned migration runs", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		fsys := repeatableRecordMigrations(map[string]string{"R__widget_ids.sql": repeatableRecordView})
		mig := repeatableMigrator(c, fixture.repeatableConnection(c), fsys).WithMigrationsTable("", long)

		err := mig.MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorIs, migrator.ErrUnaddressableMetadataTable)
		c.Assert(err, qt.ErrorMatches, `(?s).*the repeatable migration record for migrations table "c+" is named "c+_repeatable".*`)
		c.Assert(fixture.relationExists(c, "widgets"), qt.IsFalse)
		applied, err := mig.GetAppliedMigrations(t.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(applied, qt.HasLen, 0)
	})

	t.Run("a dry run reports the same refusal", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		conn := fixture.repeatableConnection(c)
		conn.SchemaWriter().SetDryRun(true)
		fsys := repeatableRecordMigrations(map[string]string{"R__widget_ids.sql": repeatableRecordView})

		err := repeatableMigrator(c, conn, fsys).WithMigrationsTable("", long).
			MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorIs, migrator.ErrUnaddressableMetadataTable)
	})

	t.Run("a record table another role owns is refused before the versioned migration runs", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "schema_migrations_repeatable")
		fsys := repeatableRecordMigrations(map[string]string{"R__widget_ids.sql": repeatableRecordView})

		err := repeatableMigrator(c, fixture.repeatableConnection(c), fsys).
			MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
		c.Assert(fixture.relationExists(c, "widgets"), qt.IsFalse)
		c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
	})

	t.Run("a dry run refuses the foreign record table too", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "schema_migrations_repeatable")
		conn := fixture.repeatableConnection(c)
		conn.SchemaWriter().SetDryRun(true)
		fsys := repeatableRecordMigrations(map[string]string{"R__widget_ids.sql": repeatableRecordView})

		err := repeatableMigrator(c, conn, fsys).MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
		c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
	})

	// A missing CREATE privilege cannot be predicted without a write, so the
	// record is created before the first versioned statement instead: the
	// refusal then comes before anything changed. The versioned migration
	// needs no CREATE, so it is the record alone that the privilege stops.
	t.Run("a record the role may not create is refused before the versioned migration runs", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		fsys := fstest.MapFS{
			"0000000001_note.up.sql": &fstest.MapFile{
				Data: []byte("INSERT INTO public.ptah_squat_evidence (note) VALUES ('versioned');\n"),
			},
			"0000000001_note.down.sql": &fstest.MapFile{Data: []byte("DELETE FROM public.ptah_squat_evidence;\n")},
			"R__notes.sql": &fstest.MapFile{
				Data: []byte("CREATE OR REPLACE VIEW notes AS SELECT note FROM public.ptah_squat_evidence;\n"),
			},
		}
		mig := repeatableMigrator(c, fixture.repeatableConnection(c), fsys)
		c.Assert(mig.Initialize(t.Context()), qt.IsNil)
		fixture.adminExec(c, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
		fixture.adminExec(c, "REVOKE CREATE ON SCHEMA public FROM "+fixture.migrationRole)

		err := mig.MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorMatches, `(?s)failed to create the repeatable migration record: .*permission denied.*`)
		c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
		applied, err := mig.GetAppliedMigrations(t.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(applied, qt.HasLen, 0)
	})

	t.Run("CREATE INDEX CONCURRENTLY without no_transaction is refused before anything runs", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		fsys := repeatableRecordMigrations(map[string]string{
			"R__widget_index.sql": "CREATE INDEX CONCURRENTLY IF NOT EXISTS widgets_id_idx ON widgets (id);\n",
		})

		err := repeatableMigrator(c, fixture.repeatableConnection(c), fsys).
			MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.ErrorMatches, `(?s).*R__widget_index.sql cannot run inside a transaction: .*CONCURRENTLY.*`)
		c.Assert(fixture.relationExists(c, "widgets"), qt.IsFalse)
		c.Assert(fixture.relationExists(c, "schema_migrations_repeatable"), qt.IsFalse)
	})
}

func TestRepeatableRecordRefusalsLive_HappyPath(t *testing.T) {
	t.Run("the same migrations table name runs without a repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		mig := repeatableMigrator(c, fixture.repeatableConnection(c), repeatableRecordMigrations(nil)).
			WithMigrationsTable("", strings.Repeat("c", 55))

		c.Assert(mig.MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{}), qt.IsNil)
		c.Assert(fixture.relationExists(c, "widgets"), qt.IsTrue)
	})

	t.Run("CREATE INDEX CONCURRENTLY runs in a file marked no_transaction", func(t *testing.T) {
		c := qt.New(t)
		fixture := newForeignMetadataFixture(t, "")
		fsys := repeatableRecordMigrations(map[string]string{
			"R__widget_index.sql": "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS widgets_id_idx ON widgets (id);\n",
		})

		c.Assert(repeatableMigrator(c, fixture.repeatableConnection(c), fsys).
			MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{}), qt.IsNil)
		c.Assert(fixture.indexExists(c, "widgets_id_idx"), qt.IsTrue)
	})
}

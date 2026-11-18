package migrator_test

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// atlasRepeatableFiles is a directory in the Atlas layout: one versioned file
// and the repeatable migrations given.
func atlasRepeatableFiles(repeatables map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"1_users.sql": &fstest.MapFile{Data: []byte(repeatableVersionedUp)},
	}
	for name, body := range repeatables {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// runAtlasUp runs one up over the Atlas directory fsys into the Atlas
// revision table at dbPath, with the provider options given.
func runAtlasUp(c *qt.C, dbPath string, fsys fstest.MapFS, opts ...migrator.FSProviderOption) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	opts = append([]migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)}, opts...)
	mig, err := migrator.NewFSMigrator(conn, fsys, opts...)
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})
}

// atlasRevisionVersions reads the versions the Atlas revision table at dbPath
// records, in byte order.
func atlasRevisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestAtlasRepeatableMigrations_HappyPath(t *testing.T) {
	t.Run("the Atlas revision format keeps the record beside atlas_schema_revisions", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))

		err := mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).MigrateUpWithOptions(
			c.Context(), migrator.MigrateUpOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		c.Assert(repeatableTableCount(c, dbPath, "atlas_schema_revisions_repeatable"), qt.Equals, 1)
	})

	t.Run("an Atlas directory reads R__ as a repeatable migration that runs again when it changes", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		first := atlasRepeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		changed := atlasRepeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})

		c.Assert(runAtlasUp(c, dbPath, first, migrator.WithAtlasRepeatableMigrations()), qt.IsNil)
		c.Assert(runAtlasUp(c, dbPath, first, migrator.WithAtlasRepeatableMigrations()), qt.IsNil)
		c.Assert(runAtlasUp(c, dbPath, changed, migrator.WithAtlasRepeatableMigrations()), qt.IsNil)

		c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1"})
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names", "user_names"})
		c.Assert(repeatableTableCount(c, dbPath, "atlas_schema_revisions_repeatable"), qt.Equals, 1)
	})

	t.Run("without the option R__ stays the one-time version R", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		first := atlasRepeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})

		c.Assert(runAtlasUp(c, dbPath, first, migrator.WithRepeatableMigrations()), qt.IsNil)

		c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "R"})
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		c.Assert(repeatableTableCount(c, dbPath, "atlas_schema_revisions_repeatable"), qt.Equals, 0)
	})
}

func TestAtlasRepeatableMigrations_FailurePath(t *testing.T) {
	t.Run("a revision table that records version R is refused before anything runs", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := atlasRepeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		c.Assert(runAtlasUp(c, dbPath, fsys), qt.IsNil)

		err := runAtlasUp(c, dbPath, fsys, migrator.WithAtlasRepeatableMigrations())

		refusal, ok := errors.AsType[*migrator.AtlasRepeatableVersionError](err)
		c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
		c.Assert(refusal.Identities, qt.DeepEquals, []string{"user_names"})
		c.Assert(refusal.Description, qt.Equals, "user_names")
		c.Assert(err, qt.ErrorMatches, `the revision table "atlas_schema_revisions" records version R .* delete the row with version 'R' from "atlas_schema_revisions" .*`)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		c.Assert(repeatableTableCount(c, dbPath, "atlas_schema_revisions_repeatable"), qt.Equals, 0)
	})

	t.Run("a directory not read in the Atlas format is refused", func(t *testing.T) {
		c := qt.New(t)

		provider, err := migrator.NewFSMigrationProvider(
			repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.WithAtlasRepeatableMigrations(),
		)

		c.Assert(err, qt.ErrorMatches, `repeatable migrations in the Atlas reading need a directory read in the Atlas format, not ""`)
		c.Assert(provider, qt.IsNil)
	})
}

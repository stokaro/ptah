package migrator_test

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// spelledAtlasDirectory is an Atlas directory whose versions carry leading
// zeros, in the numbering akashi and most hand-numbered histories use. 010
// sorts after 002 both as a number and as a string, so the fixture cannot pass
// by an ordering accident.
func spelledAtlasDirectory() fstest.MapFS {
	return fstest.MapFS{
		"001_a.up.sql":   {Data: []byte("CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n")},
		"001_a.down.sql": {Data: []byte("DROP TABLE spelled_a;\n")},
		"002_b.up.sql":   {Data: []byte("CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n")},
		"002_b.down.sql": {Data: []byte("DROP TABLE spelled_b;\n")},
		"010_c.up.sql":   {Data: []byte("CREATE TABLE spelled_c (id INTEGER PRIMARY KEY);\n")},
		"010_c.down.sql": {Data: []byte("DROP TABLE spelled_c;\n")},
	}
}

// unspelledAtlasDirectory holds the same migrations as spelledAtlasDirectory
// under numbers without leading zeros. Applying it writes the rows 1, 2 and 10,
// which is the history the spelled directory then meets.
func unspelledAtlasDirectory() fstest.MapFS {
	return fstest.MapFS{
		"1_a.up.sql":    {Data: []byte("CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n")},
		"1_a.down.sql":  {Data: []byte("DROP TABLE spelled_a;\n")},
		"2_b.up.sql":    {Data: []byte("CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n")},
		"2_b.down.sql":  {Data: []byte("DROP TABLE spelled_b;\n")},
		"10_c.up.sql":   {Data: []byte("CREATE TABLE spelled_c (id INTEGER PRIMARY KEY);\n")},
		"10_c.down.sql": {Data: []byte("DROP TABLE spelled_c;\n")},
	}
}

func openSpellingDatabase(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "spelling.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newSpellingMigrator(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
	format migrator.RevisionTableFormat,
) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(format)
}

// storedAtlasVersions reads the version column back, which is the only place
// the spelling Atlas will compare against exists.
func storedAtlasVersions(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func storedNativeVersions(c *qt.C, conn *dbschema.DatabaseConnection) []int64 {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT version FROM schema_migrations ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func revisionKeys(revisions []migrator.MigrationRevision) []string {
	keys := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		keys = append(keys, revision.RevisionVersion())
	}
	return keys
}

// TestAtlasVersionSpelling_AtlasTableRecordsTheFileNameToken pins what the
// Atlas community binary writes for 001_a.sql: the token 001. Atlas compares
// versions as strings, so a history recorded as 1, 2, 10 reads 2 as its
// current version and refuses the next file as out of order.
func TestAtlasVersionSpelling_AtlasTableRecordsTheFileNameToken(t *testing.T) {
	c := qt.New(t)
	conn := openSpellingDatabase(c)
	m := newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatAtlas)

	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"001", "002", "010"})

	// A second run reads its own rows back under the same keys, so nothing is
	// pending and nothing is written twice.
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"001", "002", "010"})
	reconcile, err := m.VerifyAppliedChecksums(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)

	c.Assert(m.MigrateDown(c.Context()), qt.IsNil)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"001", "002"})
}

// TestAtlasVersionSpelling_NativeTableKeysRowsByTheFileSpelling covers an
// Atlas directory tracked in a native table, which stores only the number.
// The row for 1 has to compare under the file's key 001, or every verb that
// matches rows to files reads 001_a.up.sql as missing: a rollback refuses, and
// checksum verification reports a file the directory holds.
func TestAtlasVersionSpelling_NativeTableKeysRowsByTheFileSpelling(t *testing.T) {
	c := qt.New(t)
	conn := openSpellingDatabase(c)
	m := newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatPtah)

	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(storedNativeVersions(c, conn), qt.DeepEquals, []int64{1, 2, 10})

	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisionKeys(revisions), qt.DeepEquals, []string{"001", "002", "010"})
	reconcile, err := m.VerifyAppliedChecksums(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)

	c.Assert(m.MigrateDown(c.Context()), qt.IsNil)
	c.Assert(storedNativeVersions(c, conn), qt.DeepEquals, []int64{1, 2})
}

// TestAtlasVersionSpelling_NativeRowWithoutAFileKeepsItsNumber is the control
// for the test above: the file's spelling reaches only the rows a file
// accounts for, so a row the directory has no file for is still reported
// under its own number.
func TestAtlasVersionSpelling_NativeRowWithoutAFileKeepsItsNumber(t *testing.T) {
	c := qt.New(t)
	conn := openSpellingDatabase(c)
	c.Assert(
		newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatPtah).MigrateUp(c.Context()),
		qt.IsNil,
	)
	shorter := spelledAtlasDirectory()
	delete(shorter, "010_c.up.sql")
	delete(shorter, "010_c.down.sql")
	m := newSpellingMigrator(c, conn, shorter, migrator.RevisionTableFormatPtah)

	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisionKeys(revisions), qt.DeepEquals, []string{"001", "002", "10"})
	_, err = m.VerifyAppliedChecksums(c.Context())
	var missing *migrator.MissingMigrationError
	c.Assert(err, qt.ErrorAs, &missing)
	c.Assert(missing.RevisionKey, qt.Equals, "10")
}

// TestAtlasVersionSpelling_HistoryWithoutZerosIsNotReapplied covers a history
// whose rows read 1, 2, 10 against a directory that spells 001, 002, 010. No
// row names a file under its own key, and the apply must still
// not run the migrations a second time: each one would fail on a table it
// created, or, for a statement that can run twice, succeed twice.
func TestAtlasVersionSpelling_HistoryWithoutZerosIsNotReapplied(t *testing.T) {
	c := qt.New(t)
	conn := openSpellingDatabase(c)
	c.Assert(
		newSpellingMigrator(c, conn, unspelledAtlasDirectory(), migrator.RevisionTableFormatAtlas).MigrateUp(c.Context()),
		qt.IsNil,
	)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"1", "10", "2"})

	m := newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatAtlas)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"1", "10", "2"})
	status, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

// TestAtlasVersionSpelling_RepeatableSharesItsNumber is the control for the
// refusal below. An Atlas import of a Flyway directory names a repeatable after
// the version it follows, so 2R_views.sql beside 2_b.sql is one number carried
// by two identities, and neither is a spelling of the other.
func TestAtlasVersionSpelling_RepeatableSharesItsNumber(t *testing.T) {
	c := qt.New(t)
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"2_b.sql":      {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
			"2R_views.sql": {Data: []byte("CREATE VIEW v AS SELECT 1;\n")},
		},
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	keys := make([]string, 0, 2)
	for _, migration := range provider.Migrations() {
		keys = append(keys, migration.RevisionVersion())
	}
	slices.Sort(keys)
	c.Assert(keys, qt.DeepEquals, []string{"2", "2R"})
}

func TestAtlasVersionSpelling_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		fsys    fstest.MapFS
		wantErr string
	}{
		{
			name: "two migrations",
			fsys: fstest.MapFS{
				"1_a.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"001_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
			},
			wantErr: `Atlas migration files 001_b.sql and 1_a.sql spell version 1 two ways, "001" and "1": ` +
				`the revision table records the spelling and migrations run in numeric order, so one version needs one spelling`,
		},
		{
			name: "one migration split across spellings",
			fsys: fstest.MapFS{
				"01_a.up.sql":    {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
			},
			wantErr: `Atlas migration files 001_a.down.sql and 01_a.up.sql spell version 1 two ways, "001" and "01": .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider, err := migrator.NewFSMigrationProvider(
				test.fsys,
				migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(provider, qt.IsNil)
		})
	}
}

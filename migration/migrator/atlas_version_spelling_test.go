package migrator_test

import (
	"context"
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

// unspelledHistory applies unspelledAtlasDirectory to an Atlas-format table,
// which records the rows 1, 2 and 10 for the migrations spelledAtlasDirectory
// names 001, 002 and 010.
func unspelledHistory(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn := openSpellingDatabase(c)
	c.Assert(
		newSpellingMigrator(c, conn, unspelledAtlasDirectory(), migrator.RevisionTableFormatAtlas).MigrateUp(c.Context()),
		qt.IsNil,
	)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"1", "10", "2"})
	return conn
}

// spellingTables lists the tables the spelling fixtures create, so a test can
// see whether a refused command ran any of their migrations.
func spellingTables(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'spelled_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// TestAtlasVersionSpelling_RespelledHistoryIsRefused covers a history whose
// rows read 1, 2, 10 against a directory that spells 001, 002, 010. Atlas
// compares versions as text, so to it no row names a file, while the numbers
// make each row one of the files. Every command refuses the history rather
// than answer from one of the two readings: status would otherwise report each
// row as missing and its file as pending, and a rollback would run the down
// migration and leave the row behind, because it deletes the row by the
// file's spelling.
func TestAtlasVersionSpelling_RespelledHistoryIsRefused(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *migrator.Migrator) error
	}{
		{name: "status", run: func(ctx context.Context, m *migrator.Migrator) error {
			_, err := m.GetMigrationStatus(ctx)
			return err
		}},
		{name: "up", run: func(ctx context.Context, m *migrator.Migrator) error {
			return m.MigrateUp(ctx)
		}},
		{name: "down", run: func(ctx context.Context, m *migrator.Migrator) error {
			return m.MigrateDown(ctx)
		}},
		{name: "down to zero", run: func(ctx context.Context, m *migrator.Migrator) error {
			return m.MigrateDownTo(ctx, 0)
		}},
		{name: "verify checksums", run: func(ctx context.Context, m *migrator.Migrator) error {
			_, err := m.VerifyAppliedChecksums(ctx)
			return err
		}},
		{name: "set", run: func(ctx context.Context, m *migrator.Migrator) error {
			_, err := m.SetRevision(ctx, 2)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := unspelledHistory(c)
			m := newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatAtlas)

			err := test.run(c.Context(), m)

			var spelling *migrator.RevisionSpellingError
			c.Assert(err, qt.ErrorAs, &spelling)
			c.Assert(spelling.Rows, qt.DeepEquals, []migrator.RevisionSpelling{
				{Version: 1, Recorded: "1", File: "001"},
				{Version: 2, Recorded: "2", File: "002"},
				{Version: 10, Recorded: "10", File: "010"},
			})
			c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"1", "10", "2"})
			c.Assert(spellingTables(c, conn), qt.DeepEquals, []string{"spelled_a", "spelled_b", "spelled_c"})
		})
	}
}

// TestAtlasVersionSpelling_RefusalCarriesTheStatementsThatRespell runs the
// statements the refusal prints and then the commands it refused. After them
// the rows spell their files, status has nothing pending or missing, and a
// rollback removes the row it rolled back.
func TestAtlasVersionSpelling_RefusalCarriesTheStatementsThatRespell(t *testing.T) {
	c := qt.New(t)
	conn := unspelledHistory(c)
	m := newSpellingMigrator(c, conn, spelledAtlasDirectory(), migrator.RevisionTableFormatAtlas)

	_, err := m.GetMigrationStatus(c.Context())
	var spelling *migrator.RevisionSpellingError
	c.Assert(err, qt.ErrorAs, &spelling)
	c.Assert(spelling.Table, qt.Equals, `"atlas_schema_revisions"`)
	c.Assert(spelling.Statements, qt.DeepEquals, []string{
		`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1'`,
		`UPDATE "atlas_schema_revisions" SET version = '002' WHERE version = '2'`,
		`UPDATE "atlas_schema_revisions" SET version = '010' WHERE version = '10'`,
	})
	for _, statement := range spelling.Statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}

	status, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.MissingMigrations, qt.HasLen, 0)
	c.Assert(m.MigrateDown(c.Context()), qt.IsNil)
	c.Assert(storedAtlasVersions(c, conn), qt.DeepEquals, []string{"001", "002"})
	c.Assert(spellingTables(c, conn), qt.DeepEquals, []string{"spelled_a", "spelled_b"})
}

// TestAtlasVersionSpelling_RowWithoutAFileIsNotRespelled is the control for
// the refusal's reach: a row the directory has no file for spells nothing, so
// the refusal names only the rows a file carries the number of.
func TestAtlasVersionSpelling_RowWithoutAFileIsNotRespelled(t *testing.T) {
	c := qt.New(t)
	conn := unspelledHistory(c)
	shorter := spelledAtlasDirectory()
	delete(shorter, "010_c.up.sql")
	delete(shorter, "010_c.down.sql")
	m := newSpellingMigrator(c, conn, shorter, migrator.RevisionTableFormatAtlas)

	_, err := m.GetRevisions(c.Context())

	var spelling *migrator.RevisionSpellingError
	c.Assert(err, qt.ErrorAs, &spelling)
	c.Assert(spelling.Rows, qt.DeepEquals, []migrator.RevisionSpelling{
		{Version: 1, Recorded: "1", File: "001"},
		{Version: 2, Recorded: "2", File: "002"},
	})
}

func TestRevisionSpellingError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *migrator.RevisionSpellingError
		want string
	}{
		{
			name: "one row",
			err: &migrator.RevisionSpellingError{
				Table:      `"atlas_schema_revisions"`,
				Rows:       []migrator.RevisionSpelling{{Version: 1, Recorded: "1", File: "001"}},
				Statements: []string{`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1'`},
			},
			want: `revision table "atlas_schema_revisions" records 1 version under another spelling than its ` +
				`migration file: 1 for 001; Atlas compares versions as text, so each of these rows names a ` +
				`different revision than its file does. Respell the rows, then run the command again:` + "\n" +
				`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1';`,
		},
		{
			name: "several rows",
			err: &migrator.RevisionSpellingError{
				Table: `"atlas_schema_revisions"`,
				Rows: []migrator.RevisionSpelling{
					{Version: 1, Recorded: "1", File: "001"},
					{Version: 2, Recorded: "2", File: "002"},
				},
				Statements: []string{
					`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1'`,
					`UPDATE "atlas_schema_revisions" SET version = '002' WHERE version = '2'`,
				},
			},
			want: `revision table "atlas_schema_revisions" records 2 versions under another spelling than their ` +
				`migration files: 1 for 001 and 1 more; Atlas compares versions as text, so each of these rows ` +
				`names a different revision than its file does. Respell the rows, then run the command again:` + "\n" +
				`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1';` + "\n" +
				`UPDATE "atlas_schema_revisions" SET version = '002' WHERE version = '2';`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
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

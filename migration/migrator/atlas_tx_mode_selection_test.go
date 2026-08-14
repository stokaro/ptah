package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestAtlasTxModeValidation_SelectedFilesOnly(t *testing.T) {
	c := qt.New(t)
	conn, mig := newAtlasTxModeSelectionMigrator(c, migrator.MigrationTxModeFile)

	err := mig.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{Amount: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(atlasTxModeSelectionTableExists(c, conn, "first_valid"), qt.IsTrue)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 1)

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "2_invalid.sql"`)
	c.Assert(atlasTxModeSelectionTableExists(c, conn, "second_invalid"), qt.IsFalse)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 1)
}

func TestAtlasTxModeValidation_FirstFileErrorSkipsPreflight(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "first-invalid.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_invalid.sql": {
				Data: []byte("-- atlas:txmode bogus\n\nCREATE TABLE invalid (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	preflightCalled := false

	err = mig.MigrateUpWithPreflight(
		c.Context(),
		func(context.Context, migrator.MigrationPlan) error {
			preflightCalled = true
			return nil
		},
	)

	c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "1_invalid.sql"`)
	c.Assert(preflightCalled, qt.IsFalse)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 0)
}

func TestAtlasTxModeValidation_PrecedenceAcrossSelectedFiles(t *testing.T) {
	tests := []struct {
		name          string
		globalMode    migrator.MigrationTxMode
		wantFirst     bool
		wantRevisions int
	}{
		{
			name:          "file validates when reached",
			globalMode:    migrator.MigrationTxModeFile,
			wantFirst:     true,
			wantRevisions: 1,
		},
		{
			name:          "none validates when reached",
			globalMode:    migrator.MigrationTxModeNone,
			wantFirst:     true,
			wantRevisions: 1,
		},
		{
			name:       "all validates complete batch",
			globalMode: migrator.MigrationTxModeAll,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, mig := newAtlasTxModeSelectionMigrator(c, test.globalMode)
			err := mig.MigrateUp(c.Context())
			c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "2_invalid.sql"`)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "first_valid"), qt.Equals, test.wantFirst)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "second_invalid"), qt.IsFalse)
			c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, test.wantRevisions)
		})
	}
}

func TestAtlasTxModeProvider_PlainDirections(t *testing.T) {
	c := qt.New(t)
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"1_directional.up.sql": {
				Data: []byte("-- atlas:txmode file\n\nCREATE TABLE directional (id INTEGER PRIMARY KEY);\n"),
			},
			"1_directional.down.sql": {
				Data: []byte("-- atlas:txmode none\n\nDROP TABLE directional;\n"),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTxMode, qt.Equals, migrator.MigrationFileTxModeFile)
	c.Assert(migrations[0].DownTxMode, qt.Equals, migrator.MigrationFileTxModeNone)
}

func TestAtlasTxModeProvider_TxtarDirections(t *testing.T) {
	c := qt.New(t)
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"1_directional.sql": {
				Data: []byte(`-- atlas:txtar

-- migration.sql --
-- atlas:txmode file

CREATE TABLE directional (id INTEGER PRIMARY KEY);

-- down.sql --
-- atlas:txmode none

DROP TABLE directional;
`),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTxMode, qt.Equals, migrator.MigrationFileTxModeFile)
	c.Assert(migrations[0].DownTxMode, qt.Equals, migrator.MigrationFileTxModeNone)
}

func TestAtlasTxModeProvider_InvalidTxtarDownPreservesAppliedRevision(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "invalid-down.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_directional.sql": {
				Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE directional (id INTEGER PRIMARY KEY);

-- down.sql --
-- atlas:txmode bogus

DROP TABLE directional;
`),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	err = mig.MigrateDownTo(c.Context(), 0)
	c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "1_directional.sql#down.sql"`)
	c.Assert(atlasTxModeSelectionTableExists(c, conn, "directional"), qt.IsTrue)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 1)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func TestAtlasTxModeProvider_RejectsMisplacedTxtarDirective(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "directive before migration section",
			sql: `-- atlas:txmode none

-- atlas:txtar

-- migration.sql --
CREATE TABLE up_body (id INTEGER PRIMARY KEY);

-- down.sql --
CREATE TABLE down_body (id INTEGER PRIMARY KEY);
			`,
		},
		{
			name: "migration section before directive",
			sql: `-- migration.sql --
-- atlas:txtar
CREATE TABLE up_body (id INTEGER PRIMARY KEY);

-- down.sql --
CREATE TABLE down_body (id INTEGER PRIMARY KEY);
			`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(
				c.Context(),
				"sqlite://"+filepath.Join(c.TempDir(), "misplaced.sqlite"),
			)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			_, err = migrator.NewFSMigrator(
				conn,
				fstest.MapFS{"1_misplaced.sql": {Data: []byte(test.sql)}},
				migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
			)
			c.Assert(
				err,
				qt.ErrorMatches,
				`failed to load Atlas migration 1_misplaced.sql: invalid Atlas txtar migration 1_misplaced.sql: -- atlas:txtar must be the first non-empty line`,
			)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "up_body"), qt.IsFalse)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "down_body"), qt.IsFalse)
		})
	}
}

func TestAtlasTxModeProvider_DownValidationPrecedesRollback(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
	}{
		{
			name: "plain files",
			fsys: fstest.MapFS{
				"1_first.up.sql":   {Data: []byte("CREATE TABLE first_table (id INTEGER PRIMARY KEY);\n")},
				"1_first.down.sql": {Data: []byte("-- atlas:txmode bogus\n\nDROP TABLE first_table;\n")},
				"2_second.up.sql":  {Data: []byte("CREATE TABLE second_table (id INTEGER PRIMARY KEY);\n")},
				"2_second.down.sql": {
					Data: []byte("DROP TABLE second_table;\n"),
				},
			},
		},
		{
			name: "txtar files",
			fsys: fstest.MapFS{
				"1_first.sql": {Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE first_table (id INTEGER PRIMARY KEY);

-- down.sql --
-- atlas:txmode bogus

DROP TABLE first_table;
`)},
				"2_second.sql": {Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE second_table (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE second_table;
`)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(
				c.Context(),
				"sqlite://"+filepath.Join(c.TempDir(), "down-validation.sqlite"),
			)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			mig, err := migrator.NewFSMigrator(
				conn,
				test.fsys,
				migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
			)
			c.Assert(err, qt.IsNil)
			mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
			c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

			err = mig.MigrateDownTo(c.Context(), 0)
			c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive ".*"`)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "first_table"), qt.IsTrue)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "second_table"), qt.IsTrue)
			c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 2)

			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.CurrentVersion, qt.Equals, int64(2))
			c.Assert(status.DirtyRevision, qt.IsNil)
		})
	}
}

func TestNewMigrationFromSQLFiles_PreservesTxModePolicy(t *testing.T) {
	tests := []struct {
		name       string
		globalMode migrator.MigrationTxMode
		directive  string
		wantErr    string
		wantTable  bool
	}{
		{
			name:       "file overrides global none",
			globalMode: migrator.MigrationTxModeNone,
			directive:  "-- atlas:txmode file\n\n",
			wantErr:    `(?s).*missing_table.*`,
		},
		{
			name:       "none overrides global file",
			globalMode: migrator.MigrationTxModeFile,
			directive:  "-- atlas:txmode none\n\n",
			wantErr:    `(?s).*missing_table.*`,
			wantTable:  true,
		},
		{
			name:       "file conflicts with global all",
			globalMode: migrator.MigrationTxModeAll,
			directive:  "-- atlas:txmode file\n\n",
			wantErr:    `cannot set txmode directive to "file" in "migration.up.sql" when txmode "all" is set globally`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(
				c.Context(),
				"sqlite://"+filepath.Join(c.TempDir(), "constructor-policy.sqlite"),
			)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			migration, err := migrator.NewMigrationFromSQLFiles(
				1,
				"constructor policy",
				"migration.up.sql",
				"migration.down.sql",
				fstest.MapFS{
					"migration.up.sql": {
						Data: []byte(test.directive + "CREATE TABLE constructor_table (id INTEGER PRIMARY KEY);\nINSERT INTO missing_table (id) VALUES (1);\n"),
					},
					"migration.down.sql": {Data: []byte("DROP TABLE constructor_table;\n")},
				},
			)
			c.Assert(err, qt.IsNil)
			mig := migrator.NewMigrator(
				conn,
				migrator.NewRegisteredMigrationProvider(migration),
			).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
				WithTransactionMode(test.globalMode)

			err = mig.MigrateUp(c.Context())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(atlasTxModeSelectionTableExists(c, conn, "constructor_table"), qt.Equals, test.wantTable)
		})
	}
}

func TestAtlasTxModeProgrammatic_InvalidParsedModeLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "invalid-programmatic.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	migration := migrator.CreateMigrationFromSQL(
		1,
		"programmatic.sql",
		"-- atlas:txmode bogus\n\nCREATE TABLE invalid_programmatic (id INTEGER PRIMARY KEY);\n",
		"",
	)
	mig := migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(migration),
	).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "programmatic.sql"`)
	c.Assert(atlasTxModeSelectionTableExists(c, conn, "invalid_programmatic"), qt.IsFalse)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 0)
}

func TestAtlasTxModeProgrammatic_InvalidEnumLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "invalid-enum.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	migration := migrator.CreateMigrationFromSQL(
		1,
		"invalid enum",
		"CREATE TABLE invalid_enum (id INTEGER PRIMARY KEY);\n",
		"",
	)
	migration.UpTxMode = migrator.MigrationFileTxMode("bogus")
	mig := migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(migration),
	).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.ErrorMatches, `invalid migration file txmode "bogus": expected file, none, or empty`)
	c.Assert(atlasTxModeSelectionTableExists(c, conn, "invalid_enum"), qt.IsFalse)
	c.Assert(atlasTxModeSelectionRevisionCount(c, conn), qt.Equals, 0)
}

func newAtlasTxModeSelectionMigrator(
	c *qt.C,
	globalMode migrator.MigrationTxMode,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "atlas-tx-mode-selection.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_valid.sql": {
				Data: []byte("CREATE TABLE first_valid (id INTEGER PRIMARY KEY);\n"),
			},
			"2_invalid.sql": {
				Data: []byte("-- atlas:txmode bogus\n\nCREATE TABLE second_invalid (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	return conn, mig.
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(globalMode)
}

func atlasTxModeSelectionTableExists(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	name string,
) bool {
	var count int
	err := conn.QueryRowContext(
		c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		name,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func atlasTxModeSelectionRevisionCount(c *qt.C, conn *dbschema.DatabaseConnection) int {
	var count int
	err := conn.QueryRowContext(c.Context(), "SELECT count(*) FROM atlas_schema_revisions").Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

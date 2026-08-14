package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	atlasTxModeMatrixFilename = "1_tx_mode_matrix.sql"
	atlasTxModeMatrixTable    = "atlas_tx_mode_matrix_body"
	atlasTxModeMissingTable   = "atlas_tx_mode_matrix_missing"
	atlasTxModeMatrixBody     = `CREATE TABLE atlas_tx_mode_matrix_body (id INTEGER PRIMARY KEY);
INSERT INTO atlas_tx_mode_matrix_missing (id) VALUES (1);
`
)

func newAtlasTxModeMatrixMigrator(
	c *qt.C,
	globalMode migrator.MigrationTxMode,
	directive string,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "atlas-tx-mode-matrix.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			atlasTxModeMatrixFilename: {
				Data: []byte(directive + atlasTxModeMatrixBody),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	return conn, mig.
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(globalMode)
}

func atlasTxModeTableExists(c *qt.C, conn *dbschema.DatabaseConnection) bool {
	var count int
	err := conn.QueryRowContext(
		c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		atlasTxModeMatrixTable,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func atlasTxModeRevisionCount(c *qt.C, conn *dbschema.DatabaseConnection) int {
	var count int
	err := conn.QueryRowContext(
		c.Context(),
		"SELECT count(*) FROM atlas_schema_revisions WHERE version = '1'",
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func assertAtlasTxModeBodyFailure(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
	wantTable bool,
	wantApplied int,
) {
	err := mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no such table: "+atlasTxModeMissingTable)
	c.Assert(atlasTxModeTableExists(c, conn), qt.Equals, wantTable)
	c.Assert(atlasTxModeRevisionCount(c, conn), qt.Equals, 1)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[0].State, qt.Equals, "failed")
	c.Assert(revisions[0].Dirty, qt.IsTrue)
	c.Assert(revisions[0].Applied, qt.Equals, wantApplied)
	c.Assert(revisions[0].Total, qt.Equals, 2)
	c.Assert(revisions[0].Error, qt.Contains, "no such table: "+atlasTxModeMissingTable)
	c.Assert(revisions[0].ErrorStatement, qt.Contains, atlasTxModeMissingTable)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision.Dirty, qt.IsTrue)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, wantApplied)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
}

func assertAtlasTxModeRejectedBeforeWrites(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
	wantErr string,
) {
	err := mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	var txModeErr *migrator.AtlasTxModeDirectiveError
	c.Assert(err, qt.ErrorAs, &txModeErr)
	c.Assert(err.Error(), qt.Equals, wantErr)
	c.Assert(atlasTxModeTableExists(c, conn), qt.IsFalse)
	c.Assert(atlasTxModeRevisionCount(c, conn), qt.Equals, 0)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 0)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func TestAtlasTxModeMatrix_BodyFailurePath(t *testing.T) {
	// This matrix pins effective mode selection for #998.
	//
	// wantApplied and wantTable now agree in every row, and that agreement is
	// the point: the first statement creates the table, so applied is 1 exactly
	// when the table survived the failure and 0 when the body was rolled back.
	// Before #966 the four rolled-back rows still recorded applied=1, which a
	// retry would have read as "statement 1 is committed, resume at 2" and so
	// skipped a CREATE TABLE that never ran.
	tests := []struct {
		name        string
		globalMode  migrator.MigrationTxMode
		directive   string
		wantTable   bool
		wantApplied int
	}{
		{
			name:        "global file directive absent",
			globalMode:  migrator.MigrationTxModeFile,
			wantApplied: 0,
		},
		{
			name:        "global file directive file",
			globalMode:  migrator.MigrationTxModeFile,
			directive:   "-- atlas:txmode file\n\n",
			wantApplied: 0,
		},
		{
			name:        "global file directive none",
			globalMode:  migrator.MigrationTxModeFile,
			directive:   "-- atlas:txmode none\n\n",
			wantTable:   true,
			wantApplied: 1,
		},
		{
			name:        "global file misplaced directive is ignored",
			globalMode:  migrator.MigrationTxModeFile,
			directive:   "-- generated migration\n\n-- atlas:txmode none\n",
			wantApplied: 0,
		},
		{
			name:       "global all directive absent",
			globalMode: migrator.MigrationTxModeAll,
		},
		{
			name:        "global none directive absent",
			globalMode:  migrator.MigrationTxModeNone,
			wantTable:   true,
			wantApplied: 1,
		},
		{
			name:        "global none directive file",
			globalMode:  migrator.MigrationTxModeNone,
			directive:   "-- atlas:txmode file\n\n",
			wantApplied: 0,
		},
		{
			name:        "global none directive none",
			globalMode:  migrator.MigrationTxModeNone,
			directive:   "-- atlas:txmode none\n\n",
			wantTable:   true,
			wantApplied: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, mig := newAtlasTxModeMatrixMigrator(c, test.globalMode, test.directive)
			assertAtlasTxModeBodyFailure(c, conn, mig, test.wantTable, test.wantApplied)
		})
	}
}

func TestAtlasTxModeMatrix_DirectiveFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		globalMode migrator.MigrationTxMode
		directive  string
		wantErr    string
	}{
		{
			name:       "global file directive all",
			globalMode: migrator.MigrationTxModeFile,
			directive:  "-- atlas:txmode all\n\n",
			wantErr:    `txmode "all" is not allowed in file directive "1_tx_mode_matrix.sql". Use "file" instead`,
		},
		{
			name:       "global all directive file",
			globalMode: migrator.MigrationTxModeAll,
			directive:  "-- atlas:txmode file\n\n",
			wantErr:    `cannot set txmode directive to "file" in "1_tx_mode_matrix.sql" when txmode "all" is set globally`,
		},
		{
			name:       "global all directive none",
			globalMode: migrator.MigrationTxModeAll,
			directive:  "-- atlas:txmode none\n\n",
			wantErr:    `cannot set txmode directive to "none" in "1_tx_mode_matrix.sql" when txmode "all" is set globally`,
		},
		{
			name:       "global all directive all",
			globalMode: migrator.MigrationTxModeAll,
			directive:  "-- atlas:txmode all\n\n",
			wantErr:    `txmode "all" is not allowed in file directive "1_tx_mode_matrix.sql". Use "file" instead`,
		},
		{
			name:       "global none directive all",
			globalMode: migrator.MigrationTxModeNone,
			directive:  "-- atlas:txmode all\n\n",
			wantErr:    `txmode "all" is not allowed in file directive "1_tx_mode_matrix.sql". Use "file" instead`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, mig := newAtlasTxModeMatrixMigrator(c, test.globalMode, test.directive)
			assertAtlasTxModeRejectedBeforeWrites(c, conn, mig, test.wantErr)
		})
	}
}

func TestAtlasTxModeMalformedDirectives_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		wantErr   string
	}{
		{
			name:      "unknown",
			directive: "-- atlas:txmode statement\n\n",
			wantErr:   `unknown txmode "statement" found in file directive "1_tx_mode_matrix.sql"`,
		},
		{
			name:      "duplicate preserves occurrence order",
			directive: "-- atlas:txmode none\n-- atlas:txmode file\n\n",
			wantErr:   `multiple txmode values found in file "1_tx_mode_matrix.sql": ["none" "file"]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, mig := newAtlasTxModeMatrixMigrator(c, migrator.MigrationTxModeFile, test.directive)
			assertAtlasTxModeRejectedBeforeWrites(c, conn, mig, test.wantErr)
		})
	}
}

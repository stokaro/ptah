package migrator_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// bom is the UTF-8 byte order mark, EF BB BF, which an editor that marks its
// encoding writes at the start of a file.
const bom = "\xEF\xBB\xBF"

// A migration file that starts with a byte order mark loads as the SQL after
// it: the mark reaches no statement, and a directive on the first line still
// decides how the file runs.
func TestNewFSMigrationProvider_ByteOrderMark_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		fsys           fstest.MapFS
		format         migrationfile.DirFormat
		wantUpSQL      string
		wantTxMode     migrationfile.FileTxMode
		wantCheckpoint bool
	}{
		{
			name: "ptah file",
			fsys: fstest.MapFS{
				"0000000001_users.up.sql":   {Data: []byte(bom + "CREATE TABLE users (id INT);\n")},
				"0000000001_users.down.sql": {Data: []byte(bom + "DROP TABLE users;\n")},
			},
			format:    migrationfile.DirFormatPtah,
			wantUpSQL: "CREATE TABLE users (id INT);\n",
		},
		{
			name: "ptah directive on the first line",
			fsys: fstest.MapFS{
				"0000000001_users.up.sql":   {Data: []byte(bom + "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY i ON users (id);\n")},
				"0000000001_users.down.sql": {Data: []byte("DROP INDEX i;\n")},
			},
			format:     migrationfile.DirFormatPtah,
			wantUpSQL:  "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY i ON users (id);\n",
			wantTxMode: migrationfile.FileTxModeNone,
		},
		{
			name: "atlas txmode directive on the first line",
			fsys: fstest.MapFS{
				"20240101000000_users.sql": {Data: []byte(bom + "-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY i ON users (id);\n")},
			},
			format:     migrationfile.DirFormatAtlas,
			wantUpSQL:  "-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY i ON users (id);\n",
			wantTxMode: migrationfile.FileTxModeNone,
		},
		{
			name: "atlas checkpoint directive on the first line",
			fsys: fstest.MapFS{
				"20240101000000_users.sql": {Data: []byte(bom + "-- atlas:checkpoint\n\nCREATE TABLE users (id INT);\n")},
			},
			format:         migrationfile.DirFormatAtlas,
			wantUpSQL:      "-- atlas:checkpoint\n\nCREATE TABLE users (id INT);\n",
			wantCheckpoint: true,
		},
		{
			name: "atlas txtar header on the first line",
			fsys: fstest.MapFS{
				"20240101000000_users.sql": {Data: []byte(bom + "-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE users (id INT);\n\n-- down.sql --\nDROP TABLE users;\n")},
			},
			format:    migrationfile.DirFormatAtlas,
			wantUpSQL: "CREATE TABLE users (id INT);\n\n",
		},
		{
			name: "a mark later in the file stays",
			fsys: fstest.MapFS{
				"0000000001_users.up.sql":   {Data: []byte("SELECT 1;\n" + bom + "SELECT 2;\n")},
				"0000000001_users.down.sql": {Data: []byte("SELECT 1;\n")},
			},
			format:    migrationfile.DirFormatPtah,
			wantUpSQL: "SELECT 1;\n" + bom + "SELECT 2;\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			provider, err := migrator.NewFSMigrationProvider(tt.fsys, migrator.WithMigrationDirFormat(tt.format))
			c.Assert(err, qt.IsNil)

			migrations := provider.Migrations()
			c.Assert(migrations, qt.HasLen, 1)
			c.Assert(migrations[0].UpSQL, qt.Equals, tt.wantUpSQL)
			c.Assert(migrations[0].UpTxMode, qt.Equals, tt.wantTxMode)
			c.Assert(migrations[0].IsCheckpoint, qt.Equals, tt.wantCheckpoint)
		})
	}
}

// A malformed directive behind a byte order mark is read and refused, as it is
// without the mark. A mark that hid the directive would load the file and run
// it under the default transaction mode instead.
func TestNewFSMigrationProvider_ByteOrderMark_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		fsys    fstest.MapFS
		wantErr string
	}{
		{
			name: "versioned file",
			fsys: fstest.MapFS{
				"0000000001_users.up.sql":   {Data: []byte(bom + "-- +ptah no_transaction=maybe\nSELECT 1;\n")},
				"0000000001_users.down.sql": {Data: []byte("SELECT 1;\n")},
			},
			wantErr: `failed to load up migration 0000000001_users\.up\.sql: invalid migration directives in 0000000001_users\.up\.sql: invalid \+ptah no_transaction value "maybe": expected true or false`,
		},
		{
			name: "repeatable file",
			fsys: fstest.MapFS{
				"0000000001_users.up.sql":   {Data: []byte("SELECT 1;\n")},
				"0000000001_users.down.sql": {Data: []byte("SELECT 1;\n")},
				"R__users_view.sql":         {Data: []byte(bom + "-- +ptah no_transaction=maybe\nSELECT 1;\n")},
			},
			wantErr: `failed to load repeatable migration R__users_view\.sql: invalid migration directives in R__users_view\.sql: invalid \+ptah no_transaction value "maybe": expected true or false`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			provider, err := migrator.NewFSMigrationProvider(tt.fsys,
				migrator.WithMigrationDirFormat(migrationfile.DirFormatPtah), migrator.WithRepeatableMigrations())
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(provider, qt.IsNil)
		})
	}
}

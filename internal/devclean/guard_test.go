package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/devclean"
)

func TestReplayGuardSQLite_HappyPath(t *testing.T) {
	guard := devclean.NewReplayGuard(types.DBInfo{Dialect: platform.SQLite})
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "main table",
			statement: `CREATE TABLE main.users (id INTEGER PRIMARY KEY)`,
		},
		{
			name:      "ordinary data migration",
			statement: `INSERT INTO users (id) VALUES (1)`,
		},
		{
			name:      "temp word in value",
			statement: `UPDATE users SET label = 'temp.object'`,
		},
		{
			name:      "table named temp",
			statement: `CREATE TABLE temp (id INTEGER PRIMARY KEY)`,
		},
		{
			name:      "foreign keys pragma",
			statement: `PRAGMA foreign_keys = ON`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestReplayGuardSQLite_FailurePath(t *testing.T) {
	guard := devclean.NewReplayGuard(types.DBInfo{Dialect: platform.SQLite})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "attach database",
			statement: `ATTACH DATABASE 'other.db' AS aux`,
			wantErr:   `sqlite migration replay rejects ATTACH .*`,
		},
		{
			name:      "detach database",
			statement: `DETACH DATABASE aux`,
			wantErr:   `sqlite migration replay rejects DETACH .*`,
		},
		{
			name:      "temporary object",
			statement: `CREATE TEMPORARY TABLE users (id INTEGER)`,
			wantErr:   `sqlite migration replay rejects TEMP object .*`,
		},
		{
			name:      "qualified temp target",
			statement: `INSERT INTO "temp".users (id) VALUES (1)`,
			wantErr:   `sqlite migration replay rejects TEMP schema target .*`,
		},
		{
			name:      "bracketed temp target",
			statement: `DROP TABLE [temp].users`,
			wantErr:   `sqlite migration replay rejects TEMP schema target .*`,
		},
		{
			name:      "vacuum into",
			statement: `VACUUM main INTO 'backup.db'`,
			wantErr:   `sqlite migration replay rejects VACUUM INTO .*`,
		},
		{
			name:      "load extension",
			statement: `SELECT load_extension('unsafe')`,
			wantErr:   `sqlite migration replay rejects load_extension .*`,
		},
		{
			name:      "persistent pragma",
			statement: `PRAGMA user_version = 42`,
			wantErr:   `sqlite migration replay rejects state-changing or unsupported PRAGMA .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

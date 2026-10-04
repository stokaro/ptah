package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// failingStatement leaves every statement to the migrator but the one at
// failAt, counted from 1, which it fails with err without running it.
type failingStatement struct {
	failAt int
	err    error
	seen   int
}

func (f *failingStatement) ValidateDirectives(map[string]string) error { return nil }

func (f *failingStatement) ExecuteStatement(
	context.Context, *dbschema.DatabaseConnection, string, map[string]string,
) (bool, error) {
	f.seen++
	if f.seen == f.failAt {
		return false, f.err
	}
	return false, nil
}

// A statement that runs outside a transaction is marked in flight first. When
// it fails because the connection broke, the server may have applied it, so
// the mark stays and a resume refuses to guess; when the server refused it,
// the outcome is known and the failure replaces the mark. Inside a
// transaction, which rolls back, there is no mark to keep.
func TestMigrateUp_ConnectionFailureKeepsTheStatementOutcomeUnknown(t *testing.T) {
	tests := []struct {
		name        string
		txMode      migrator.MigrationTxMode
		err         error
		wantUnknown bool
		wantError   string
	}{
		{
			name:        "the connection broke outside a transaction",
			txMode:      migrator.MigrationTxModeNone,
			err:         fmt.Errorf("failed to receive message: %w", io.ErrUnexpectedEOF),
			wantUnknown: true,
			wantError:   "statement execution outcome is unknown after process interruption",
		},
		{
			name:      "the server refused the statement outside a transaction",
			txMode:    migrator.MigrationTxModeNone,
			err:       errors.New(`table "b" already exists`),
			wantError: `(?s)failed to execute migration SQL: table "b" already exists\nSQL: CREATE TABLE b .*`,
		},
		{
			name:      "the connection broke inside a transaction",
			txMode:    migrator.MigrationTxModeFile,
			err:       fmt.Errorf("failed to receive message: %w", io.ErrUnexpectedEOF),
			wantError: `(?s)failed to execute migration SQL: failed to receive message: unexpected EOF\nSQL: CREATE TABLE b .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = conn.Close() })
			provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
				"0000000001_tables.up.sql": &fstest.MapFile{Data: []byte(
					"CREATE TABLE a (id INTEGER);\nCREATE TABLE b (id INTEGER);\n")},
				"0000000001_tables.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE b;\nDROP TABLE a;\n")},
			}, migrator.WithStatementInterceptor(&failingStatement{failAt: 2, err: test.err}))
			c.Assert(err, qt.IsNil)
			m := migrator.NewMigrator(conn, provider).WithTransactionMode(test.txMode)

			runErr := m.MigrateUp(c.Context())

			c.Assert(runErr, qt.ErrorIs, test.err)
			revisions, err := m.GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(revisions, qt.HasLen, 1)
			c.Assert(revisions[0].StatementOutcomeUnknown(), qt.Equals, test.wantUnknown)
			c.Assert(revisions[0].Error, qt.Matches, test.wantError)
		})
	}
}

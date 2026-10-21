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

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
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
			name:        "the native adapter reported uncertainty",
			txMode:      migrator.MigrationTxModeNone,
			err:         ptaherr.ErrOutcomeUnknown,
			wantUnknown: true,
			wantError:   "statement execution outcome is unknown after process interruption",
		},
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
			conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "app.db")))
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

// uncertainAppliedStatement executes the SQL but loses its acknowledgment.
// This exercises recovery against a real write, not only an injected error.
type uncertainAppliedStatement struct{ calls int }

func (*uncertainAppliedStatement) ValidateDirectives(map[string]string) error { return nil }
func (f *uncertainAppliedStatement) ExecuteStatement(ctx context.Context, conn *dbschema.DatabaseConnection, statement string, _ map[string]string) (bool, error) {
	f.calls++
	if err := conn.Writer().ExecuteSQL(ctx, statement); err != nil {
		return true, err
	}
	return true, ptaherr.ErrOutcomeUnknown
}

func TestMigrateUp_NativeUncertaintyDoesNotReplayCommittedSQL(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "app.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	fault := &uncertainAppliedStatement{}
	provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
		"0000000001_receipt.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE receipt (id INTEGER);\n")},
		"0000000001_receipt.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE receipt;\n")},
	}, migrator.WithStatementInterceptor(fault))
	c.Assert(err, qt.IsNil)
	runner := migrator.NewMigrator(conn, provider).WithTransactionMode(migrator.MigrationTxModeNone)
	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'receipt'").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 1)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].StatementOutcomeUnknown(), qt.IsTrue)
	c.Assert(runner.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.ErrorMatches, "(?s).*outcome of statement 1 is unknown.*")
	c.Assert(fault.calls, qt.Equals, 1)
}

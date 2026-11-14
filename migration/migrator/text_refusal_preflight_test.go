package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A run asks every file it selected, before the first of them runs, each
// refusal the step that runs a file makes from the file's text and the run's
// options: transaction control in a body the run wraps in a transaction or
// runs without one, a directive the installed interceptor rejects, and a
// statement the installed validator rejects. These tests run three migrations
// on a fresh SQLite database whose second file is the one each test writes,
// with an interceptor and a validator installed on every run.

// refusedStatement is the one statement textRefusalValidator rejects.
const refusedStatement = "CREATE TABLE refused (id INTEGER PRIMARY KEY)"

// textRefusalInterceptor accepts the online_ddl_tool directive with the value
// ghost or none and rejects any other, as the online DDL executor does. It runs
// no statement itself, and records each statement it is offered, which is each
// statement the run sends.
type textRefusalInterceptor struct {
	offered []string
}

func (i *textRefusalInterceptor) ValidateDirectives(directives map[string]string) error {
	tool, ok := directives["online_ddl_tool"]
	if !ok || tool == "ghost" || tool == "none" {
		return nil
	}
	return fmt.Errorf("unknown online_ddl_tool directive value %q", tool)
}

func (i *textRefusalInterceptor) ExecuteStatement(
	_ context.Context,
	_ *dbschema.DatabaseConnection,
	stmt string,
	_ map[string]string,
) (bool, error) {
	i.offered = append(i.offered, stmt)
	return false, nil
}

// textRefusalValidator rejects refusedStatement and accepts every other.
type textRefusalValidator struct{}

var errRefusedStatement = errors.New("the statement is refused")

func (textRefusalValidator) ValidateStatement(stmt string) error {
	if stmt == refusedStatement {
		return errRefusedStatement
	}
	return nil
}

// textRefusalFiles are three migrations whose second up and down files are up
// and down.
func textRefusalFiles(up, down string) fstest.MapFS {
	return fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte(up)},
		"0000000002_second.down.sql": {Data: []byte(down)},
		"0000000003_third.up.sql":    {Data: []byte("CREATE TABLE third (id INTEGER PRIMARY KEY);\n")},
		"0000000003_third.down.sql":  {Data: []byte("DROP TABLE third;\n")},
	}
}

// textRefusalMigrator returns a fresh SQLite database and a migrator over
// files, with the interceptor it installed.
func textRefusalMigrator(
	c *qt.C,
	files fstest.MapFS,
) (*dbschema.DatabaseConnection, *migrator.Migrator, *textRefusalInterceptor) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "refusal.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	interceptor := &textRefusalInterceptor{}
	m, err := migrator.NewFSMigrator(conn, files,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithStatementValidator(textRefusalValidator{}),
	)
	c.Assert(err, qt.IsNil)
	return conn, m, interceptor
}

// textRefusalAppliedMigrator is textRefusalMigrator with all three migrations
// applied, and the interceptor's record of the statements they sent cleared.
func textRefusalAppliedMigrator(
	c *qt.C,
	down string,
) (*dbschema.DatabaseConnection, *migrator.Migrator, *textRefusalInterceptor) {
	c.Helper()
	conn, m, interceptor := textRefusalMigrator(c,
		textRefusalFiles("CREATE TABLE second (id INTEGER PRIMARY KEY);\n", down))
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	interceptor.offered = nil
	return conn, m, interceptor
}

// The refusal comes before the run sends a statement of any file: the first
// migration does not run, no revision is written, and the second file's
// revision is not left failed with nothing applied.
func TestMigrateUp_TextRefusalComesBeforeTheFirstFile_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		up      string
		mode    migrator.MigrationTxMode
		wantErr string
	}{
		{
			name:    "transaction control in a file run in a transaction",
			up:      "BEGIN;\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCOMMIT;\n",
			mode:    migrator.MigrationTxModeFile,
			wantErr: `migration 2 cannot run tx-mode file up statement 1 because it controls transaction state; .*`,
		},
		{
			name:    "transaction control under tx-mode all",
			up:      "BEGIN;\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCOMMIT;\n",
			mode:    migrator.MigrationTxModeAll,
			wantErr: `migration 2 cannot run tx-mode all up statement 1 because it controls transaction state; .*`,
		},
		{
			name: "transaction control in a no_transaction file",
			up:   "-- +ptah no_transaction\nBEGIN;\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCOMMIT;\n",
			mode: migrator.MigrationTxModeFile,
			wantErr: `migration 2 cannot run up statement 1 without a transaction because "BEGIN" controls ` +
				`transaction state; .*`,
		},
		{
			name: "transaction control under tx-mode none",
			up:   "BEGIN;\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCOMMIT;\n",
			mode: migrator.MigrationTxModeNone,
			wantErr: `migration 2 cannot run up statement 1 without a transaction because "BEGIN" controls ` +
				`transaction state; .*`,
		},
		{
			name: "a directive the interceptor rejects",
			up:   "-- +ptah online_ddl_tool=bogus\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeFile,
			wantErr: `invalid migration directives in 0000000002_second\.up\.sql: ` +
				`unknown online_ddl_tool directive value "bogus"`,
		},
		{
			name: "a directive the interceptor rejects, under tx-mode none",
			up:   "-- +ptah online_ddl_tool=bogus\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeNone,
			wantErr: `invalid migration directives in 0000000002_second\.up\.sql: ` +
				`unknown online_ddl_tool directive value "bogus"`,
		},
		{
			name: "a directive the interceptor rejects, under tx-mode all",
			up:   "-- +ptah online_ddl_tool=bogus\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeAll,
			wantErr: `invalid migration directives in 0000000002_second\.up\.sql: ` +
				`unknown online_ddl_tool directive value "bogus"`,
		},
		{
			name: "a statement the validator rejects",
			up:   "CREATE TABLE second (id INTEGER PRIMARY KEY);\n" + refusedStatement + ";\n",
			mode: migrator.MigrationTxModeFile,
			wantErr: `(?s)failed to validate migration SQL in 0000000002_second\.up\.sql: the statement is refused` +
				`\nSQL: ` + regexp.QuoteMeta(refusedStatement),
		},
		{
			name: "a statement the validator rejects, under tx-mode all",
			up:   "CREATE TABLE second (id INTEGER PRIMARY KEY);\n" + refusedStatement + ";\n",
			mode: migrator.MigrationTxModeAll,
			wantErr: `(?s)failed to validate migration SQL in 0000000002_second\.up\.sql: the statement is refused` +
				`\nSQL: ` + regexp.QuoteMeta(refusedStatement),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m, interceptor := textRefusalMigrator(c, textRefusalFiles(test.up, "DROP TABLE second;\n"))

			err := m.WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(interceptor.offered, qt.HasLen, 0)
			c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": false, "second": false, "third": false})
			c.Assert(revisionStates(c, m), qt.DeepEquals, make(map[int64]string))
		})
	}
}

// A dry run is refused before it sends a statement too, so it does not print
// the files before the refused one as if they would apply.
func TestMigrateUp_TextRefusalComesBeforeTheFirstFile_DryRun(t *testing.T) {
	tests := []struct {
		name    string
		up      string
		wantErr string
	}{
		{
			name:    "transaction control",
			up:      "BEGIN;\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCOMMIT;\n",
			wantErr: `migration 2 cannot run tx-mode file up statement 1 because it controls transaction state; .*`,
		},
		{
			name:    "a directive the interceptor rejects",
			up:      "-- +ptah online_ddl_tool=bogus\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			wantErr: `invalid migration directives in 0000000002_second\.up\.sql: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m, interceptor := textRefusalMigrator(c, textRefusalFiles(test.up, "DROP TABLE second;\n"))
			conn.SchemaWriter().SetDryRun(true)

			err := m.MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(interceptor.offered, qt.HasLen, 0)
		})
	}
}

// The controls: a directive the interceptor accepts, a statement the validator
// accepts, and a file that runs without a transaction and controls none, so
// the whole run applies.
func TestMigrateUp_TextRefusalComesBeforeTheFirstFile_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		up   string
		mode migrator.MigrationTxMode
	}{
		{
			name: "a directive the interceptor accepts",
			up:   "-- +ptah online_ddl_tool=ghost\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeFile,
		},
		{
			name: "a no_transaction file that controls no transaction",
			up:   "-- +ptah no_transaction\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeFile,
		},
		{
			name: "tx-mode none over a file that controls no transaction",
			up:   "CREATE TABLE second (id INTEGER PRIMARY KEY);\n",
			mode: migrator.MigrationTxModeNone,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m, interceptor := textRefusalMigrator(c, textRefusalFiles(test.up, "DROP TABLE second;\n"))

			err := m.WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(interceptor.offered, qt.HasLen, 3)
			c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": true, "second": true, "third": true})
			c.Assert(revisionStates(c, m), qt.DeepEquals, map[int64]string{1: "applied", 2: "applied", 3: "applied"})
		})
	}
}

// A down file is refused before the rollback sends a statement of any file:
// the third migration's rollback, which comes first, does not run, and every
// revision still records its migration applied rather than the second one
// failed with nothing rolled back.
func TestMigrateDownTo_TextRefusalComesBeforeTheRollback_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		down    string
		wantErr string
	}{
		{
			name:    "transaction control in a file run in a transaction",
			down:    "BEGIN;\nDROP TABLE second;\nCOMMIT;\n",
			wantErr: `migration 2 cannot run tx-mode file down statement 1 because it controls transaction state; .*`,
		},
		{
			name: "transaction control in a no_transaction file",
			down: "-- +ptah no_transaction\nBEGIN;\nDROP TABLE second;\nCOMMIT;\n",
			wantErr: `migration 2 cannot run down statement 1 without a transaction because "BEGIN" controls ` +
				`transaction state; .*`,
		},
		{
			name: "a directive the interceptor rejects",
			down: "-- +ptah online_ddl_tool=bogus\nDROP TABLE second;\n",
			wantErr: `invalid migration directives in 0000000002_second\.down\.sql: ` +
				`unknown online_ddl_tool directive value "bogus"`,
		},
		{
			name: "a statement the validator rejects",
			down: "DROP TABLE second;\n" + refusedStatement + ";\n",
			wantErr: `(?s)failed to validate migration SQL in 0000000002_second\.down\.sql: the statement is refused` +
				`\nSQL: ` + regexp.QuoteMeta(refusedStatement),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m, interceptor := textRefusalAppliedMigrator(c, test.down)

			err := m.MigrateDownTo(c.Context(), 0)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(interceptor.offered, qt.HasLen, 0)
			c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": true, "second": true, "third": true})
			c.Assert(revisionStates(c, m), qt.DeepEquals, map[int64]string{1: "applied", 2: "applied", 3: "applied"})
		})
	}
}

// A rollback's dry run is refused before it sends a statement too.
func TestMigrateDownTo_TextRefusalComesBeforeTheRollback_DryRun(t *testing.T) {
	c := qt.New(t)
	conn, m, interceptor := textRefusalAppliedMigrator(c, "-- +ptah online_ddl_tool=bogus\nDROP TABLE second;\n")
	conn.SchemaWriter().SetDryRun(true)

	err := m.MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.ErrorMatches, `invalid migration directives in 0000000002_second\.down\.sql: .*`)
	c.Assert(interceptor.offered, qt.HasLen, 0)
}

// The control: a down file whose directive the interceptor accepts rolls back
// with the rest of the run.
func TestMigrateDownTo_TextRefusalComesBeforeTheRollback_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, m, interceptor := textRefusalAppliedMigrator(c, "-- +ptah online_ddl_tool=none\nDROP TABLE second;\n")

	err := m.MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.offered, qt.HasLen, 3)
	c.Assert(existingTables(c, conn), qt.DeepEquals, map[string]bool{"first": false, "second": false, "third": false})
	c.Assert(revisionStates(c, m), qt.DeepEquals, make(map[int64]string))
}

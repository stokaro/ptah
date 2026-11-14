package migrator_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// SQLite's migrator reads no body as one that opens transactions of its own,
// so a down file holding BEGIN and COMMIT is refused under the golang-migrate
// runner as under the default one. Neither the predicate the recovery hints
// read nor the repair refusal names that runner there: it would send the
// operator to a second refusal. The PostgreSQL side, where the runner reads
// such a file, is measured by the live and e2e tests under integration/.

// editedRollbackMigrator rolls a SQLite database back over a no_transaction
// down file that fails at its second statement, which leaves the revision
// failed:down with one of two statements committed. It returns a migrator,
// loaded with opts, whose down file has since been edited to open its own
// transaction, the only way such a body reaches a dirty revision on SQLite.
func editedRollbackMigrator(c *qt.C, opts ...migrator.FSProviderOption) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "edited.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	files := fstest.MapFS{
		"0000000001_gears.up.sql": {Data: []byte("CREATE TABLE gears (id INTEGER PRIMARY KEY);\n")},
		"0000000001_gears.down.sql": {
			Data: []byte("-- +ptah no_transaction\nDROP TABLE gears;\nDROP TABLE gear_parts;\n"),
		},
	}
	m, err := migrator.NewFSMigrator(conn, files)
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(m.MigrateDownTo(c.Context(), 0), qt.ErrorMatches, `(?s).*no such table: gear_parts.*`)

	files["0000000001_gears.down.sql"] = &fstest.MapFile{
		Data: []byte("BEGIN;\nDROP TABLE gears;\nDROP TABLE gear_parts;\nCOMMIT;\n"),
	}
	edited, err := migrator.NewFSMigrator(conn, files, opts...)
	c.Assert(err, qt.IsNil)
	return edited
}

// revisionProgress is every revision row as "version:state applied/total",
// with the direction a rollback records.
func revisionProgress(c *qt.C, m *migrator.Migrator) []string {
	c.Helper()
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	rows := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		rows = append(rows, fmt.Sprintf("%d:%s:%s %d/%d",
			revision.Version, revision.State, revision.Direction, revision.Applied, revision.Total))
	}
	return rows
}

func TestRevisionBodyOpensOwnTransactions_OnAnEngineThatReadsNone(t *testing.T) {
	c := qt.New(t)
	m := editedRollbackMigrator(c, migrator.WithOwnTransactions())
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)

	c.Assert(m.RevisionBodyOpensOwnTransactions(revisions[0]), qt.IsFalse)
}

func TestRepairMigration_TransactionControlNoRunnerReads_FailurePath(t *testing.T) {
	const wantErr = `migration 1 cannot be repaired safely: migration 1 cannot run down statement 1 without a ` +
		`transaction because "BEGIN" controls transaction state; remove the transaction control and let Ptah ` +
		`checkpoint each statement independently`
	tests := []struct {
		name    string
		options []migrator.FSProviderOption
	}{
		{name: "under the default runner"},
		{name: "under the golang-migrate runner", options: []migrator.FSProviderOption{migrator.WithOwnTransactions()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m := editedRollbackMigrator(c, test.options...)

			err := m.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1})

			c.Assert(err, qt.ErrorMatches, wantErr)
			c.Assert(revisionProgress(c, m), qt.DeepEquals, []string{"1:failed:down 1/2"})
		})
	}
}

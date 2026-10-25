//go:build integration

package migrationreplay_test

import (
	"os"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
)

// openMeterPrelude creates the tables the two OpenMeter files read, with the
// columns they touch and one row where a file backfills.
const openMeterPrelude = `
CREATE TABLE addon_rate_cards (id integer PRIMARY KEY);
CREATE TABLE plan_rate_cards (id integer PRIMARY KEY);
CREATE TABLE charge_credit_purchases (id integer PRIMARY KEY, filters jsonb, feature_filters jsonb);
CREATE TABLE ledger_sub_account_routes (id integer PRIMARY KEY, filters jsonb, features jsonb);
INSERT INTO charge_credit_purchases VALUES (1, NULL, '["a"]'), (2, NULL, '[]');
INSERT INTO ledger_sub_account_routes VALUES (1, NULL, '["b"]');
`

func readOpenMeterFile(c *qt.C, name string) []byte {
	c.Helper()
	data, err := os.ReadFile("testdata/openmeter/" + name)
	c.Assert(err, qt.IsNil)
	return data
}

// notInTransaction reports whether the session runs outside a transaction
// block. LOCK TABLE is refused outside one, so a session that answers it is
// inside a block the replay left open.
func notInTransaction(c *qt.C, conn *dbschema.DatabaseConnection) bool {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), "LOCK TABLE charge_credit_purchases")
	return err != nil && strings.Contains(err.Error(), "can only be used in transaction blocks")
}

func replayRealm(c *qt.C, snapshot fstest.MapFS, consume func(*dbschema.DatabaseConnection, devclean.Baseline) error) (*dbschema.DatabaseConnection, error) {
	c.Helper()
	realm, err := dbschema.ConnectToDatabase(c.Context(), newPostgresReplayRealmDatabase(c))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(realm) })
	if consume == nil {
		consume = func(*dbschema.DatabaseConnection, devclean.Baseline) error { return nil }
	}
	return realm, migrationreplay.WithReplayedSnapshot(c.Context(), realm, snapshot, migrationfile.DirFormatAtlas, consume)
}

func permanentTables(c *qt.C, realm *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(realm.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").Scan(&count), qt.IsNil)
	return count
}

// A history written for golang-migrate sends each file as it is, so a file
// opens and closes its own transaction. The replay runs it outside a
// transaction of its own, applies what the real files do, and leaves the
// session outside a transaction for the next file and for the cleanup.
func TestPostgresOwnTransactionReplay_OpenMeterFiles(t *testing.T) {
	c := qt.New(t)
	snapshot := fstest.MapFS{
		"20260101000000_prelude.sql":                   {Data: []byte(openMeterPrelude)},
		"20260814113138_add_rate_card_annotations.sql": {Data: readOpenMeterFile(c, "add_rate_card_annotations.up.sql")},
		"20260921133354_credit_filter_cutover.sql":     {Data: readOpenMeterFile(c, "credit_filter_cutover.up.sql")},
	}

	realm, err := replayRealm(c, snapshot, func(conn *dbschema.DatabaseConnection, _ devclean.Baseline) error {
		var annotations int
		c.Assert(conn.QueryRowContext(c.Context(),
			`SELECT count(*) FROM information_schema.columns
			 WHERE column_name = 'annotations' AND table_name IN ('addon_rate_cards', 'plan_rate_cards')`).Scan(&annotations), qt.IsNil)
		c.Assert(annotations, qt.Equals, 2)

		var filled, notNull int
		c.Assert(conn.QueryRowContext(c.Context(),
			"SELECT count(*) FROM charge_credit_purchases WHERE filters IS NOT NULL").Scan(&filled), qt.IsNil)
		c.Assert(filled, qt.Equals, 2)
		c.Assert(conn.QueryRowContext(c.Context(),
			`SELECT count(*) FROM information_schema.columns
			 WHERE column_name = 'filters' AND is_nullable = 'NO'
			   AND table_name IN ('charge_credit_purchases', 'ledger_sub_account_routes')`).Scan(&notNull), qt.IsNil)
		c.Assert(notNull, qt.Equals, 2)

		c.Assert(notInTransaction(c, conn), qt.IsTrue)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(permanentTables(c, realm), qt.Equals, 0)
}

// A file whose transaction control is not balanced is refused before its first
// statement runs, so nothing of it is applied.
func TestPostgresOwnTransactionReplay_MalformedBoundariesAreRefused(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "never committed",
			sql:     "BEGIN;\nCREATE TABLE leaked (id integer);\n",
			wantErr: "never closed by the file",
		},
		{
			name:    "commit without begin",
			sql:     "CREATE TABLE leaked (id integer);\nCOMMIT;\n",
			wantErr: "no transaction is open to close",
		},
		{
			name:    "nested begin",
			sql:     "BEGIN;\nBEGIN;\nCREATE TABLE leaked (id integer);\nCOMMIT;\nCOMMIT;\n",
			wantErr: "already open since statement 1",
		},
		{
			name:    "rollback",
			sql:     "BEGIN;\nCREATE TABLE leaked (id integer);\nROLLBACK;\n",
			wantErr: "ROLLBACK takes the file's own work back",
		},
		{
			name:    "savepoint",
			sql:     "BEGIN;\nSAVEPOINT s;\nCREATE TABLE leaked (id integer);\nCOMMIT;\n",
			wantErr: "SAVEPOINT is partial transaction state",
		},
		{
			name:    "commit and chain",
			sql:     "BEGIN;\nCREATE TABLE leaked (id integer);\nCOMMIT AND CHAIN;\n",
			wantErr: "COMMIT AND CHAIN starts a transaction",
		},
		{
			name:    "prepared transaction",
			sql:     "BEGIN;\nCREATE TABLE leaked (id integer);\nPREPARE TRANSACTION 'x';\n",
			wantErr: "PREPARE TRANSACTION leaves a prepared transaction",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			snapshot := fstest.MapFS{
				"1_prelude.sql":   {Data: []byte("CREATE TABLE kept (id integer);")},
				"2_malformed.sql": {Data: []byte(test.sql)},
			}

			realm, err := replayRealm(c, snapshot, nil)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "migration replay rejects transaction control")
			c.Assert(err.Error(), qt.Contains, test.wantErr)
			c.Assert(permanentTables(c, realm), qt.Equals, 0)
		})
	}
}

// A statement that fails inside the file's transaction rolls it back on the
// session. The cleanup then runs on a usable session, nothing is left behind,
// and the next replay on the same database is unaffected.
func TestPostgresOwnTransactionReplay_FailureRollsBackAndLeavesNoState(t *testing.T) {
	c := qt.New(t)
	// ledger_sub_account_routes is missing, so the file fails after its first
	// UPDATE has run inside the transaction.
	failing := fstest.MapFS{
		"20260101000000_prelude.sql": {Data: []byte(`
CREATE TABLE charge_credit_purchases (id integer PRIMARY KEY, filters jsonb, feature_filters jsonb);
INSERT INTO charge_credit_purchases VALUES (1, NULL, '["a"]');`)},
		"20260921133354_credit_filter_cutover.sql": {Data: readOpenMeterFile(c, "credit_filter_cutover.up.sql")},
	}
	good := fstest.MapFS{
		"20260101000000_prelude.sql":               {Data: []byte(openMeterPrelude)},
		"20260921133354_credit_filter_cutover.sql": {Data: readOpenMeterFile(c, "credit_filter_cutover.up.sql")},
	}
	realm, err := dbschema.ConnectToDatabase(c.Context(), newPostgresReplayRealmDatabase(c))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(realm)
	noop := func(*dbschema.DatabaseConnection, devclean.Baseline) error { return nil }

	err = migrationreplay.WithReplayedSnapshot(c.Context(), realm, failing, migrationfile.DirFormatAtlas, noop)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `relation "ledger_sub_account_routes" does not exist`)
	// A session left in an aborted transaction would fail the cleanup too.
	c.Assert(err.Error(), qt.Not(qt.Contains), "clean dev database")
	c.Assert(permanentTables(c, realm), qt.Equals, 0)

	err = migrationreplay.WithReplayedSnapshot(c.Context(), realm, good, migrationfile.DirFormatAtlas,
		func(conn *dbschema.DatabaseConnection, _ devclean.Baseline) error {
			var filled int
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT count(*) FROM charge_credit_purchases WHERE filters IS NOT NULL").Scan(&filled), qt.IsNil)
			c.Assert(filled, qt.Equals, 2)
			return nil
		})
	c.Assert(err, qt.IsNil)
	c.Assert(permanentTables(c, realm), qt.Equals, 0)
}

// A file that names an enclosing transaction and opens its own cannot have both.
func TestPostgresOwnTransactionReplay_EnclosingTransactionDirectiveConflicts(t *testing.T) {
	c := qt.New(t)
	snapshot := fstest.MapFS{
		"1_conflict.sql": {Data: []byte("-- atlas:txmode file\n\nBEGIN;\nCREATE TABLE leaked (id integer);\nCOMMIT;\n")},
	}

	realm, err := replayRealm(c, snapshot, nil)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "asks for a transaction around the file and opens its own")
	c.Assert(permanentTables(c, realm), qt.Equals, 0)
}

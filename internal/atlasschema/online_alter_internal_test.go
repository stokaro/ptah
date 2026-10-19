package atlasschema

// White-box testing required: refuseOnlineAlterInOneTransaction is the refusal
// of an online apply whose NOT VALID constraint and its validation would share
// a transaction, and every path that reaches it from the exported API first
// connects to a live database of the dialect and plans against it. The rule
// itself reads only the policy, the transaction mode and the dialect's
// descriptor, so it is asserted here for every built-in dialect.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

func TestRefuseOnlineAlterInOneTransaction_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		policy  DiffPolicy
		txMode  migrator.MigrationTxMode
		dialect string
	}{
		{name: "online alter off on postgres", txMode: migrator.MigrationTxModeFile, dialect: "postgres"},
		{name: "statements commit on their own", policy: DiffPolicy{OnlineAlter: true}, txMode: migrator.MigrationTxModeNone, dialect: "postgres"},
		{name: "mysql has no NOT VALID grammar", policy: DiffPolicy{OnlineAlter: true}, txMode: migrator.MigrationTxModeFile, dialect: "mysql"},
		{name: "sqlite has no online grammar", policy: DiffPolicy{OnlineAlter: true}, txMode: migrator.MigrationTxModeFile, dialect: "sqlite"},
		{name: "an unknown dialect", policy: DiffPolicy{OnlineAlter: true}, txMode: migrator.MigrationTxModeFile, dialect: "nonesuch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseOnlineAlterInOneTransaction(test.policy, test.txMode, test.dialect), qt.IsNil)
		})
	}
}

func TestRefuseOnlineAlterInOneTransaction_FailurePath(t *testing.T) {
	for _, dialect := range []string{"postgres", "pgx", "cockroachdb", "yugabytedb", "spanner"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			err := refuseOnlineAlterInOneTransaction(DiffPolicy{OnlineAlter: true}, migrator.MigrationTxModeFile, dialect)
			c.Assert(err, qt.ErrorMatches, `online alter needs each statement to commit on its own on `+dialect+`: .*--tx-mode none.*`)
		})
	}
}

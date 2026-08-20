package migrator_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestRuntimePolicies_ReachEveryTargetTheirCapabilityClaims states the two
// policies as capability rows rather than as dialect lists.
//
// Both used to be a switch over names, and the lists had drifted from the
// engines: CockroachDB and YugabyteDB answered "migration timeouts are not
// supported" while both accept `SET LOCAL statement_timeout` and
// `SET LOCAL lock_timeout` -- measured on CockroachDB v25.4.0 and on
// YugabyteDB 2026.1, which reports itself as PostgreSQL 15.12-YB. A timeout is
// the safety belt on a migration that takes a lock, and those are the two
// PostgreSQL-shaped deployments where a long lock hurts most
// (stokaro/ptah#1713).
//
// The table is the contract the migrator now reads, so a preset that changes
// one of these values changes a runtime policy and says so here.
func TestRuntimePolicies_ReachEveryTargetTheirCapabilityClaims(t *testing.T) {
	tests := []struct {
		dialect      string
		wantTimeouts bool
		wantTxAll    bool
	}{
		{dialect: platform.Postgres, wantTimeouts: true, wantTxAll: true},
		// The two the dialect switch excluded, both measured live.
		{dialect: platform.CockroachDB, wantTimeouts: true, wantTxAll: true},
		{dialect: platform.YugabyteDB, wantTimeouts: true, wantTxAll: true},
		// The MySQL family sets and restores session variables, and commits
		// DDL as it runs -- so it carries one policy and not the other.
		{dialect: platform.MySQL, wantTimeouts: true, wantTxAll: false},
		{dialect: platform.MariaDB, wantTimeouts: true, wantTxAll: false},
		// SQLite is the mirror image: one transaction spans the run, and there
		// is no session timeout Ptah sets around a migration.
		{dialect: platform.SQLite, wantTimeouts: false, wantTxAll: true},
		{dialect: platform.SQLServer, wantTimeouts: false, wantTxAll: false},
		{dialect: platform.ClickHouse, wantTimeouts: false, wantTxAll: false},
		{dialect: platform.Spanner, wantTimeouts: false, wantTxAll: false},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForDialect(test.dialect)

			c.Assert(caps.Has(capability.MigrationTimeouts), qt.Equals, test.wantTimeouts)
			c.Assert(caps.Has(capability.TransactionalDDL), qt.Equals, test.wantTxAll)
		})
	}
}

// TestMigrationTimeouts_ParseKeepsWorkingForEveryTarget is the control that
// separates the two halves of the change.
//
// Parsing a timeout directive is dialect-neutral and always was; what moved is
// which targets can APPLY one. A test that only asserted the capability rows
// could pass on a build that stopped reading the directive at all.
func TestMigrationTimeouts_ParseKeepsWorkingForEveryTarget(t *testing.T) {
	c := qt.New(t)

	timeouts, err := migrator.ParseMigrationTimeouts("200ms", "5s")

	c.Assert(err, qt.IsNil)
	c.Assert(timeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(timeouts.LockTimeout, qt.Equals, 200*time.Millisecond)
	c.Assert(timeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(timeouts.StatementTimeout, qt.Equals, 5*time.Second)
}

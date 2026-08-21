package sqliterebuild_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// TestDDLInsideTransaction_IsDeclaredPerTarget states which engines take a
// schema statement inside an explicit transaction.
//
// The key exists because Spanner's PostgreSQL interface does not, and every
// declarative apply against it failed on its first statement:
//
//	ERROR: DDL statements are only allowed outside explicit transactions.
//	       (SQLSTATE 25000)
//
// measured on the Cloud Spanner emulator behind PGAdapter 0.55.2. The same DDL
// applies unwrapped, so what the server refuses is the wrapper
// (stokaro/ptah#1793).
//
// It is deliberately NOT capability.TransactionalDDL. That key answers whether
// a failed migration rolls back as a unit, and it is false for MySQL -- which
// commits DDL implicitly and still takes the wrapper perfectly well. Folding
// the two together would stop wrapping MySQL for a reason that does not apply
// to it.
func TestDDLInsideTransaction_IsDeclaredPerTarget(t *testing.T) {
	tests := []struct {
		dialect string
		want    bool
	}{
		{dialect: platform.Postgres, want: true},
		{dialect: platform.CockroachDB, want: true},
		{dialect: platform.YugabyteDB, want: true},
		{dialect: platform.SQLite, want: true},
		{dialect: platform.SQLServer, want: true},
		// Commits DDL implicitly and still takes the statement inside a
		// transaction: the two questions come apart here.
		{dialect: platform.MySQL, want: true},
		{dialect: platform.MariaDB, want: true},
		// No cross-statement transaction for a statement to be inside of; its
		// writer has answered BeginTransaction with a no-op for that reason
		// since long before this key.
		{dialect: platform.ClickHouse, want: false},
		// Measured: SQLSTATE 25000.
		{dialect: platform.Spanner, want: false},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			caps := capability.ForDialect(test.dialect)

			c.Assert(caps.Has(capability.DDLInsideTransaction), qt.Equals, test.want)
		})
	}
}

// TestDDLInsideTransaction_IsNotTransactionalDDL is the control that keeps the
// two keys from being folded together.
//
// They agree on most targets, which is why a test that only checked the values
// above would pass on a build that read the wrong one. MySQL is where they
// disagree, and it is the disagreement that matters: reading TransactionalDDL
// here would stop wrapping MySQL, whose server has no objection to the wrapper.
func TestDDLInsideTransaction_IsNotTransactionalDDL(t *testing.T) {
	c := qt.New(t)

	mysql := capability.ForDialect(platform.MySQL)

	c.Assert(mysql.Has(capability.DDLInsideTransaction), qt.IsTrue)
	c.Assert(mysql.Has(capability.TransactionalDDL), qt.IsFalse)
}

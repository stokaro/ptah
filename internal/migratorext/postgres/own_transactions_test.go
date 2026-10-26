package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratorext/postgres"
)

// PostgreSQL offers the reading of a body's own transactions, whose words are
// its own, in the form a runner that sends the body as it is runs it: a COMMIT
// outside a block is accepted. CockroachDB and YugabyteDB take the same words
// with other transaction semantics and offer none, so no body of theirs is
// read as one with transactions of its own.
func TestServices_OwnTransactions(t *testing.T) {
	c := qt.New(t)

	blocks, err := postgres.Postgres().OwnTransactions([]string{"BEGIN", "SELECT 1", "COMMIT", "SELECT 2", "COMMIT"})

	c.Assert(err, qt.IsNil)
	c.Assert(blocks.Boundaries(), qt.DeepEquals, []int{0, 2})
	c.Assert(blocks.Controls(), qt.DeepEquals, []int{0, 2, 4})
	c.Assert(postgres.Family().OwnTransactions, qt.IsNil)
}

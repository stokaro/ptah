package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

var ownTransactionsServer = catalog.ServerInfo{
	Dialect: platform.Postgres,
	URL:     "postgres://postgres@own-transactions-postgres:5432/dev",
	Schema:  "dev",
}

// The guard reads no boundary unless the caller that replays an existing
// history asked for it, so every other caller keeps refusing BEGIN and COMMIT.
func TestReplayGuard_TransactionBoundariesRequireTheOption(t *testing.T) {
	c := qt.New(t)
	statements := []string{"BEGIN", "SELECT 1", "COMMIT"}

	strict := devclean.NewDevReplayGuard(ownTransactionsServer).WithSessionScope()
	boundaries, err := strict.TransactionBoundaries(statements)
	c.Assert(err, qt.IsNil)
	c.Assert(boundaries, qt.IsNil)
	c.Assert(strict.ValidateStatement("BEGIN"), qt.IsNotNil)

	open := strict.WithOwnTransactions()
	boundaries, err = open.TransactionBoundaries(statements)
	c.Assert(err, qt.IsNil)
	c.Assert(boundaries, qt.DeepEquals, []int{0, 2})
	// The option does not accept the statement on its own.
	c.Assert(open.ValidateStatement("BEGIN"), qt.IsNotNil)
	// The option returns a copy; the guard it was called on is unchanged.
	boundaries, err = strict.TransactionBoundaries(statements)
	c.Assert(err, qt.IsNil)
	c.Assert(boundaries, qt.IsNil)
}

func TestReplayGuard_OwnTransactionsRefuseAMalformedFile(t *testing.T) {
	c := qt.New(t)
	open := devclean.NewDevReplayGuard(ownTransactionsServer).WithSessionScope().WithOwnTransactions()

	boundaries, err := open.TransactionBoundaries([]string{"BEGIN", "SELECT 1"})

	c.Assert(boundaries, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `postgres migration replay rejects transaction control at statement 1: .*never closed.*`)
}

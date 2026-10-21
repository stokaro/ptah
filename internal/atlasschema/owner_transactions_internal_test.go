package atlasschema

// White-box testing required: whether a target opens a transaction is read
// from the connection's capabilities, which a test cannot set on a live
// connection. The decision is tested here with that answer given.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/migration/migrator"
)

type ownerTransactionRow struct {
	name        string
	requirement plangraph.Transaction
	mode        migrator.MigrationTxMode
	opens       bool
}

// A target whose schema statements run unwrapped opens no transaction in the
// all and file modes, so a statement its owner forbids a transaction runs as
// it must there, and one that requires a transaction cannot.
func TestOwnerTransactionsAreJudgedByTheTransactionTheTargetOpens(t *testing.T) {
	for _, test := range []ownerTransactionRow{
		{"forbidden/file/unwrapped", plangraph.TransactionForbidden, migrator.MigrationTxModeFile, false},
		{"forbidden/all/unwrapped", plangraph.TransactionForbidden, migrator.MigrationTxModeAll, false},
		{"forbidden/none/unwrapped", plangraph.TransactionForbidden, migrator.MigrationTxModeNone, false},
		{"required/file/wrapped", plangraph.TransactionRequired, migrator.MigrationTxModeFile, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			requirements := [][]plangraph.Transaction{nil, {test.requirement}}
			c.Assert(checkOwnerTransactionsOpening(requirements, test.mode, test.opens), qt.IsNil)
		})
	}
}

func TestOwnerTransactionsRefuseTheTransactionTheTargetCannotGive(t *testing.T) {
	for _, test := range []ownerTransactionRow{
		{"required/file/unwrapped", plangraph.TransactionRequired, migrator.MigrationTxModeFile, false},
		{"required/all/unwrapped", plangraph.TransactionRequired, migrator.MigrationTxModeAll, false},
		{"forbidden/file/wrapped", plangraph.TransactionForbidden, migrator.MigrationTxModeFile, true},
		{"forbidden/all/wrapped", plangraph.TransactionForbidden, migrator.MigrationTxModeAll, true},
		{"required/none/wrapped", plangraph.TransactionRequired, migrator.MigrationTxModeNone, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			requirements := [][]plangraph.Transaction{nil, {test.requirement}}
			err := checkOwnerTransactionsOpening(requirements, test.mode, test.opens)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*planned statement 2 .*")
		})
	}
}

// Without a connection nothing says the target runs unwrapped, so the mode
// decides, as it did before the target was consulted.
func TestOwnerTransactionsWithoutAConnectionAssumeTheModesTransaction(t *testing.T) {
	c := qt.New(t)
	requirements := [][]plangraph.Transaction{{plangraph.TransactionForbidden}}
	c.Assert(checkOwnerTransactions(requirements, migrator.MigrationTxModeFile, nil), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(checkOwnerTransactions(requirements, migrator.MigrationTxModeNone, nil), qt.IsNil)
}

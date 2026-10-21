package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/txrequire"
)

func TestOwnerRequirementsPreserveStatementProvenance(t *testing.T) {
	c := qt.New(t)
	node := &ast.ExtensionStatement{Transaction: plangraph.TransactionRequired}
	result, err := txrequire.OwnerRequirements([]ast.Node{ast.NewComment("prefix"), node}, []int{0, 1, 1, -1})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, [][]plangraph.Transaction{nil, {plangraph.TransactionRequired}, {plangraph.TransactionRequired}, nil})
	node.Transaction = plangraph.TransactionForbidden
	result[1][0] = plangraph.TransactionAllowed
	c.Assert(result[2][0], qt.Equals, plangraph.TransactionRequired)
}

func TestOwnerRequirementsRefuseInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name        string
		index       int
		requirement plangraph.Transaction
	}{
		{"index too large", 1, plangraph.TransactionRequired},
		{"index below comment sentinel", -2, plangraph.TransactionRequired},
		{"invalid requirement", 0, "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := txrequire.OwnerRequirements([]ast.Node{&ast.ExtensionStatement{Transaction: test.requirement}}, []int{test.index})
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestCheckRequirementsAcceptsCompatibleModes(t *testing.T) {
	for _, test := range []struct {
		requirement   plangraph.Transaction
		transactional bool
	}{
		{plangraph.TransactionRequired, true}, {plangraph.TransactionForbidden, false},
		{plangraph.TransactionAllowed, true}, {plangraph.TransactionAllowed, false},
		{plangraph.TransactionUnknown, true}, {plangraph.TransactionUnknown, false},
	} {
		t.Run(string(test.requirement), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txrequire.CheckRequirements([][]plangraph.Transaction{{test.requirement}}, test.transactional), qt.IsNil)
		})
	}
}

func TestCheckRequirementsRefusesIncompatibleModes(t *testing.T) {
	for _, test := range []struct {
		requirement   plangraph.Transaction
		transactional bool
	}{
		{plangraph.TransactionRequired, false}, {plangraph.TransactionForbidden, true},
	} {
		t.Run(string(test.requirement), func(t *testing.T) {
			c := qt.New(t)
			err := txrequire.CheckRequirements([][]plangraph.Transaction{nil, {test.requirement}}, test.transactional)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*statement 2 requires.*")
		})
	}
	c := qt.New(t)
	c.Assert(txrequire.CheckRequirements([][]plangraph.Transaction{{plangraph.TransactionRequired}, {"invalid"}}, false), qt.ErrorIs, renderer.ErrInvalidResult)
}

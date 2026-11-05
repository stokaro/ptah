package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/txrequire"
)

// An engine a provider serves runs every statement unwrapped where the
// capabilities its provider states for offline planning establish
// transactional_ddl false, so an owner operation that forbids a transaction
// stays where it is. Where they establish true, or state nothing, it leaves
// the transactional file.
func TestNodeRequiresAutocommit_AProvidersEngine(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterDefaults(capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false}), "unwrapped-engine"), qt.IsNil)
	c.Assert(capability.RegisterDefaults(capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true}), "wrapped-engine"), qt.IsNil)
	node := &ast.ExtensionStatement{Transaction: plangraph.TransactionForbidden}
	for _, test := range []struct {
		dialect string
		want    bool
	}{
		{dialect: "unwrapped-engine", want: false},
		{dialect: "wrapped-engine", want: true},
		{dialect: "unstated-engine", want: true},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txrequire.NodeRequiresAutocommit(test.dialect, node), qt.Equals, test.want)
		})
	}
}

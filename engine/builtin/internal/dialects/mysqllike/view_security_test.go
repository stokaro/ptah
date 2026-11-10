package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

func TestCreateView_SQLSecurity(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			r := newRenderer(dialect)
			node := ast.NewCreateView("v").SetBody("SELECT 1")
			node.Security = "INVOKER"
			c.Assert(r.VisitNode(node), qt.IsNil)
			c.Assert(r.Output(), qt.Contains, "CREATE SQL SECURITY INVOKER VIEW `v` AS")
		})
	}
}

func TestCreateView_RefusesUnknownSecurityBeforeWriting(t *testing.T) {
	c := qt.New(t)
	r := newRenderer("mysql")
	node := ast.NewCreateView("v").SetBody("SELECT 1").SetComment("sensitive")
	node.Security = "INVKOER"
	c.Assert(r.VisitNode(node), qt.ErrorMatches, `view v: invalid view SQL SECURITY mode "INVKOER": want DEFINER or INVOKER`)
	c.Assert(r.Output(), qt.Equals, "")
}

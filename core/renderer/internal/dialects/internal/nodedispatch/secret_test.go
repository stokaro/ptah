package nodedispatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
)

// Every renderer but YDB's refuses a secret node by the secrets key, naming
// the node it was handed and the renderer.
func TestRefuseSecret(t *testing.T) {
	tests := []struct {
		node ast.Node
		want string
	}{
		{node: ast.NewCreateSecret("pw", "PTAH_SECRET_PW"), want: "secret pw: the mysql renderer writes no secret; a secret needs target capability secrets, which only YDB has"},
		{node: ast.NewAlterSecret("pw", "PTAH_SECRET_PW"), want: "ALTER SECRET pw: the mysql renderer writes no secret; a secret needs target capability secrets, which only YDB has"},
		{node: ast.NewDropSecret("pw"), want: "DROP SECRET pw: the mysql renderer writes no secret; a secret needs target capability secrets, which only YDB has"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			err := nodedispatch.RefuseSecret("mysql", test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

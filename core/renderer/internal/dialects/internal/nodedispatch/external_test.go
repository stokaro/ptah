package nodedispatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
)

// Every renderer but YDB's refuses an external data source or table node by
// the external_data_sources key, naming the node it was handed.
func TestRefuseExternal(t *testing.T) {
	const tail = ": the sqlite renderer writes no external data source or external table; each needs target " +
		"capability external_data_sources, which only YDB has"
	tests := []struct {
		node ast.Node
		want string
	}{
		{node: &ast.CreateExternalDataSourceNode{Name: "ext.s3"}, want: "external data source ext.s3" + tail},
		{node: ast.NewDropExternalDataSource("ext.s3"), want: "DROP EXTERNAL DATA SOURCE ext.s3" + tail},
		{node: &ast.CreateExternalTableNode{Name: "ext.events"}, want: "external table ext.events" + tail},
		{node: ast.NewDropExternalTable("ext.events"), want: "DROP EXTERNAL TABLE ext.events" + tail},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			err := nodedispatch.RefuseExternal("sqlite", test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

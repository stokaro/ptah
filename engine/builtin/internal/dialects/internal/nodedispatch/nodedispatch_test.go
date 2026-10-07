package nodedispatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/engine/builtin/internal/dialects/mariadb"
	"ptah.run/engine/builtin/internal/dialects/mssql"
	"ptah.run/engine/builtin/internal/dialects/mysql"
	"ptah.run/engine/builtin/internal/dialects/oracle"
	"ptah.run/engine/builtin/internal/dialects/postgres"
	"ptah.run/engine/builtin/internal/dialects/sqlite"
)

// TestVisitNode_RefusesASerialSequenceOnEveryOtherTarget drives each dialect
// renderer but YDB's with a change to a Serial's sequence. The public renderer
// refuses the node before a dialect sees it; this holds the dialect's own arm,
// which answers a caller that reaches the renderer some other way, to a
// refusal rather than a statement for some other object or a comment that
// lets an apply exit 0.
func TestVisitNode_RefusesASerialSequenceOnEveryOtherTarget(t *testing.T) {
	node := &ast.AlterSerialSequenceNode{Table: "orders", Column: "id", Path: "/local/orders/_serial_column_id",
		Start: 100, Increment: 1}
	for _, test := range []struct {
		name    string
		visitor ast.Visitor
		wantErr string
	}{
		{name: "clickhouse", visitor: clickhouse.New(), wantErr: `.* unavailable on this clickhouse target`},
		{name: "sqlite", visitor: sqlite.New(), wantErr: `.* unavailable on this sqlite target`},
		{name: "oracle", visitor: oracle.New(), wantErr: `.* unavailable on this oracle target`},
		{name: "sqlserver", visitor: mssql.New(), wantErr: `.* unavailable on this sqlserver target`},
		{name: "mysql", visitor: mysql.New(), wantErr: `.* unavailable on this mysql target`},
		{name: "mariadb", visitor: mariadb.New(), wantErr: `.* unavailable on this mariadb target`},
		{name: "postgres", visitor: postgres.New(), wantErr: `.* unavailable on this postgres target`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := test.visitor.VisitNode(node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `changing the sequence of Serial column "id" of table "orders", which requires `+
				`target capability serial_sequence_options,`+test.wantErr)
		})
	}
}

package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/yqlparse"
)

func TestDesiredYQLReplicationRendersItsPathsAndLambda(t *testing.T) {
	c := qt.New(t)
	source := "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (ENDPOINT='source:2136', DATABASE='/remote'); CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };"
	statements, err := yqlparse.Parse(source)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("ydb", capability.YDB262(), statements)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (CONNECTION_STRING = 'grpc://source:2136/?database=/remote');")
	c.Assert(sql, qt.Contains, "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };")
}

// A source patch cannot be ignored in favor of a complete Spec beside it.
// Resolve it against the source declarations before asking any renderer.
func TestUnresolvedYQLReplicationChangesCannotRender(t *testing.T) {
	for _, node := range []ast.Node{
		&ast.AlterAsyncReplicationNode{Name: "mirror", Spec: ast.AsyncReplicationSpec{Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://source:2136/?database=/remote"}, Items: []ast.AsyncReplicationItem{{Source: "src", Target: "dst"}}}, SourceSettings: map[string]string{"database": "/changed"}},
		&ast.AlterTransferNode{Name: "ingest", Spec: ast.TransferSpec{Source: "topic", Target: "table", Lambda: "($msg) -> { RETURN []; }"}, SourceSettings: map[string]string{"batch_size_bytes": "4096"}},
	} {
		c := qt.New(t)
		_, err := renderer.RenderSQLWithCapabilities("ydb", capability.YDB262(), node)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err.Error(), qt.Contains, "resolve the desired declaration")
	}
}

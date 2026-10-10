package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/yqlparse"
)

func TestDesiredYQLReplicationRendersItsPathsAndLambda(t *testing.T) {
	c := qt.New(t)
	source := "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (ENDPOINT='source:2136', DATABASE='/remote'); CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };"
	statements, err := yqlparse.Parse(source)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities("ydb", capability.YDB262(), statements)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (CONNECTION_STRING = 'grpc://source:2136/?database=/remote');")
	c.Assert(sql, qt.Contains, "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };")
}

// TestUnresolvedYQLReplicationChangesCannotRender refuses an ALTER of a
// desired YQL schema that reaches a renderer unresolved: the YQL source folds
// it into the earlier declaration, and no renderer writes the patch alone.
func TestUnresolvedYQLReplicationChangesCannotRender(t *testing.T) {
	for _, node := range []ast.Node{
		&yqlparse.ReplicationSettings{Kind: "replication", Name: "mirror", Settings: map[string]string{"database": "/changed"}},
		&yqlparse.ReplicationSettings{Kind: "transfer", Name: "ingest", Settings: map[string]string{"batch_size_bytes": "4096"}},
	} {
		c := qt.New(t)
		sql, err := builtin.RenderSQLWithCapabilities("ydb", capability.YDB262(), node)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(sql, qt.Equals, "")
	}
}

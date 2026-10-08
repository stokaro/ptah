package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

// replicationSchema declares a table with a changefeed, the table a transfer
// of that changefeed writes, a replication of another database's table and
// the transfer.
func replicationSchema(t *testing.T) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Orders", Name: "orders", PrimaryKey: []string{"id"}},
			{StructName: "Log", Name: "order_log", PrimaryKey: []string{"id"}},
		},
		FeatureObjects: testChangefeedObjects(t, "", "orders", ydbschema.ChangefeedSpec{Name: "feed", Mode: "NEW_IMAGE", Format: "JSON"}),
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "int", Primary: true},
			{StructName: "Log", Name: "id", Type: "int", Primary: true},
		},
		AsyncReplications: []schemamodel.AsyncReplication{{
			Name: "mirror",
			Spec: ast.AsyncReplicationSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
					TokenSecretName: "token"},
				Items: []ast.AsyncReplicationItem{{Source: "accounts", Target: "replica/accounts"}},
			},
		}},
		Transfers: []schemamodel.Transfer{{
			Name: "ingest",
			Spec: ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: "($m) -> { return []; }"},
		}},
	}
}

// TestRender_AsyncReplication_HappyPath writes the replication and the
// transfer after every table on YDB, the transfer after the changefeed whose
// topic it reads.
func TestRender_AsyncReplication_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(replicationSchema(t), platform.YDB,
		capability.YDB262())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `orders` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n" +
			"ALTER TABLE `orders` ADD CHANGEFEED `feed` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON');\n",
		"CREATE TABLE `order_log` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
		"CREATE ASYNC REPLICATION `mirror` FOR `accounts` AS `replica/accounts` WITH (" +
			"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', TOKEN_SECRET_NAME = 'token');\n",
		"CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; };\n",
	})
}

// TestRender_AsyncReplication_FailurePath refuses a replication and a
// transfer on every target without the key, through the whole-schema render
// and through each node alike: written nowhere, the declaration would have no
// effect and nothing would report it.
func TestRender_AsyncReplication_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, caps: capability.Oracle23()},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.AsyncReplication, false).
			With(capability.Transfers, false)},
	}
	replication := ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ast.AsyncReplicationItem{{Source: "a", Target: "ra"}},
	}
	transfer := ast.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	nodes := []struct {
		node ast.Node
		want string
	}{
		{node: ast.NewCreateAsyncReplication("mirror", replication),
			want: `.*async replication mirror, which requires target capability async_replication, .*`},
		{node: ast.NewAlterAsyncReplication("mirror", replication, replication),
			want: `.*ALTER ASYNC REPLICATION mirror, which requires target capability async_replication, .*`},
		{node: ast.NewDropAsyncReplication("mirror", true),
			want: `.*DROP ASYNC REPLICATION mirror, which requires target capability async_replication, .*`},
		{node: ast.NewCreateTransfer("ingest", transfer),
			want: `.*transfer ingest, which requires target capability transfers, .*`},
		{node: ast.NewAlterTransfer("ingest", transfer, transfer),
			want: `.*ALTER TRANSFER ingest, which requires target capability transfers, .*`},
		{node: ast.NewDropTransfer("ingest"),
			want: `.*DROP TRANSFER ingest, which requires target capability transfers, .*`},
	}

	// The changefeed the transfer reads is left out, so the refusal measured is
	// the replication's rather than the changefeed's, which comes first.
	schema := replicationSchema(t)
	schema.FeatureObjects = testChangefeedObjects(t, "", "orders")

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches,
				`.*async replication mirror, which requires target capability async_replication, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			for _, node := range nodes {
				sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, node.node)
				c.Assert(err, qt.ErrorMatches, node.want)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// TestRender_AsyncReplication_RefusesWhatALineCannotHold refuses, before any
// statement, a declaration the YDB line refuses or a table YDB would collide
// with.
func TestRender_AsyncReplication_RefusesWhatALineCannotHold(t *testing.T) {
	bySecretPath := replicationSchema(t)
	bySecretPath.AsyncReplications[0].Spec.Connection.TokenSecretName = ""
	bySecretPath.AsyncReplications[0].Spec.Connection.TokenSecretPath = "secrets/token"
	atAReplica := replicationSchema(t)
	atAReplica.Tables = append(atAReplica.Tables, schemamodel.Table{StructName: "Accounts", Name: "accounts",
		Schema: "replica", PrimaryKey: []string{"id"}})
	atAReplica.Fields = append(atAReplica.Fields, schemamodel.Field{StructName: "Accounts", Name: "id",
		Type: "int", Primary: true})
	tests := []struct {
		name   string
		schema *schemamodel.Database
		caps   capability.Capabilities
		want   string
	}{
		{name: "a transfer on 25.1", schema: replicationSchema(t), caps: capability.YDB251(),
			want: `transfer ingest, which requires target capability transfers, unavailable on this ydb target`},
		{name: "a secret path on 25.3", schema: bySecretPath, caps: capability.YDB253(),
			want: `async replication mirror names a secret by its path, which requires target capability ` +
				`replication_secret_paths, unavailable on this ydb target`},
		{name: "a table at a replica's path", schema: atAReplica, caps: capability.YDB262(),
			want: `.*table replica.accounts lies at a target of async replication mirror, which creates its ` +
				`replica tables itself; declare the replication without the table`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, platform.YDB, test.caps)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRenderSQL_YDBReplicationChanges writes what moves a replication and a
// transfer in place, and a drop with CASCADE only where the node asks for it.
func TestRenderSQL_YDBReplicationChanges(t *testing.T) {
	previous := ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ast.AsyncReplicationItem{{Source: "a", Target: "ra"}},
	}
	moved := previous.Clone()
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	transfer := ast.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	relambda := transfer
	relambda.Lambda = "($m) -> { return [<| id: 1 |>]; }"
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a connection", node: ast.NewAlterAsyncReplication("mirror", moved, previous),
			want: "ALTER ASYNC REPLICATION `mirror` SET (CONNECTION_STRING = 'grpc://standby:2136/?database=/prod');\n"},
		{name: "a lambda", node: ast.NewAlterTransfer("ingest", relambda, transfer),
			want: "ALTER TRANSFER `ingest` SET USING ($m) -> { return [<| id: 1 |>]; };\n"},
		{name: "a drop with its replicas", node: ast.NewDropAsyncReplication("mirror", true),
			want: "DROP ASYNC REPLICATION `mirror` CASCADE;\n"},
		{name: "a drop keeping its replicas", node: ast.NewDropAsyncReplication("mirror", false),
			want: "DROP ASYNC REPLICATION `mirror`;\n"},
		{name: "a transfer drop", node: ast.NewDropTransfer("etl.ingest"),
			want: "DROP TRANSFER `etl/ingest`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_YDBReplicationRefusesACreateOnlyChange refuses a node that
// asks YDB to change what it changes in no replication or transfer.
func TestRenderSQL_YDBReplicationRefusesACreateOnlyChange(t *testing.T) {
	previous := ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ast.AsyncReplicationItem{{Source: "a", Target: "ra"}},
	}
	global := previous.Clone()
	global.ConsistencyLevel = "global"
	transfer := ast.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	retargeted := transfer
	retargeted.Target = "u"
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a level", node: ast.NewAlterAsyncReplication("mirror", global, previous),
			want: `async replication mirror: its consistency_level differ from the database's, .*`},
		{name: "a target", node: ast.NewAlterTransfer("ingest", retargeted, transfer),
			want: `transfer ingest: its target differ from the database's, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderSQL_YDBReplicationRefusesAnUnnamedDrop refuses a drop that names
// no object, which would render as a statement about the empty path.
func TestRenderSQL_YDBReplicationRefusesAnUnnamedDrop(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "replication", node: ast.NewDropAsyncReplication(" ", true),
			want: `DROP ASYNC REPLICATION: it names no replication`},
		{name: "transfer", node: ast.NewDropTransfer(""), want: `DROP TRANSFER: it names no transfer`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

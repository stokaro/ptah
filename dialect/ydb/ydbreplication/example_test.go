package ydbreplication_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ExampleDesiredReplicationObject declares the replication app/mirror of a
// directory of another database and plans it against a database that holds
// none. The source describes every replication, so the read's empty answer is
// the absence the comparison needs before it plans CREATE ASYNC REPLICATION.
func ExampleDesiredReplicationObject() {
	ctx := context.Background()
	runtime := must.Must(builtin.New())
	caps := capability.YDB262()
	complete := schemaext.Knowledge{State: schemaext.Complete}
	spec := ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod", TokenSecretName: "token"},
		Items:      []ydbreplication.Item{{Source: "orders", Target: "replica/orders"}},
	}
	desired := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbreplication.DesiredReplicationObject("app", "mirror", "", spec))),
		FeatureCoverage: must.Must(ydbreplication.ReplicationCoverage(schemaext.Desired, complete, nil)),
	}
	current := &catalog.Database{FeatureCoverage: must.Must(ydbreplication.ReplicationCoverage(schemaext.Observed, complete, nil))}

	diff, err := schemadiff.CompareWithDatabaseInfo(ctx, desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, diff, "ydb", planner.Options{Capabilities: caps})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range statements {
		fmt.Println(statement)
	}
	// Output:
	// CREATE ASYNC REPLICATION `app/mirror` FOR `orders` AS `replica/orders` WITH (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', TOKEN_SECRET_NAME = 'token')
}

// ExampleReplicationsEqual compares a declared directory item with the read
// that reports one item per table under it: YDB keeps the replication as
// declared, so the two are the same replication, while an item of another
// source is not.
func ExampleReplicationsEqual() {
	connection := ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"}
	declared := ydbreplication.ReplicationSpec{Connection: connection, Items: []ydbreplication.Item{{Source: "dir", Target: "replica"}}}
	read := ydbreplication.ReplicationSpec{Connection: connection, ConsistencyLevel: ydbreplication.ConsistencyRow,
		Items: []ydbreplication.Item{{Source: "/prod/dir/t1", Target: "replica/t1"}, {Source: "/prod/dir/sub/t2", Target: "replica/sub/t2"}}}
	other := ydbreplication.ReplicationSpec{Connection: connection, Items: []ydbreplication.Item{{Source: "other", Target: "replica"}}}
	fmt.Println(ydbreplication.ReplicationsEqual(declared, read))
	fmt.Println(ydbreplication.ReplicationsEqual(other, read))
	// Output:
	// true
	// false
}

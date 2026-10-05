package ydbpool_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpool"
)

func TestCreatePoolStatement(t *testing.T) {
	tests := []struct {
		name string
		spec ast.ResourcePoolSpec
		want string
	}{
		{
			// A WITH clause needs an option, and YQL writes no negative
			// number in one, so "-1" is YDB's own spelling of no limit.
			name: "no setting",
			spec: ast.ResourcePoolSpec{},
			want: "CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = \"-1\");",
		},
		{
			// A fraction is a string literal, which YDB reads as the number;
			// a whole number of a percentage is written bare.
			name: "every setting, in the order YDB lists them",
			spec: ast.ResourcePoolSpec{
				ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(0)),
				DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
				QueryCPULimitPercentPerNode: new(0.001), TotalCPULimitPercentPerNode: new(100.0),
				ResourceWeight: new(2.25),
			},
			want: "CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = 10, QUEUE_SIZE = 0, " +
				"DATABASE_LOAD_CPU_THRESHOLD = '80.5', QUERY_MEMORY_LIMIT_PERCENT_PER_NODE = 25, " +
				"QUERY_CPU_LIMIT_PERCENT_PER_NODE = '0.001', TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 100, " +
				"RESOURCE_WEIGHT = '2.25');",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpool.CreatePoolStatement("batch", test.spec), qt.Equals, test.want)
		})
	}
}

func TestAlterPoolStatement(t *testing.T) {
	tests := []struct {
		name             string
		desired, current ast.ResourcePoolSpec
		want             string
	}{
		{
			name:    "the same settings need no statement",
			desired: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(5))},
			current: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(5))},
			want:    "",
		},
		{
			// A setting another statement left alone is not named: YDB
			// changes only what a SET names.
			name:    "one setting changes",
			desired: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(20)), QueueSize: new(int32(7))},
			current: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(7))},
			want:    "ALTER RESOURCE POOL `batch` SET (CONCURRENT_QUERY_LIMIT = 20);",
		},
		{
			// YDB checks the pool a statement leaves behind as a whole, so the
			// limit and the queue that waits for it go in one statement.
			name:    "a limit and its queue go together",
			desired: ast.ResourcePoolSpec{},
			current: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(7))},
			want:    "ALTER RESOURCE POOL `batch` RESET (CONCURRENT_QUERY_LIMIT, QUEUE_SIZE);",
		},
		{
			name:    "a set and a reset in one statement",
			desired: ast.ResourcePoolSpec{ResourceWeight: new(3.0)},
			current: ast.ResourcePoolSpec{QueryMemoryLimitPercentPerNode: new(12.5)},
			want:    "ALTER RESOURCE POOL `batch` SET (RESOURCE_WEIGHT = 3), RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpool.AlterPoolStatement("batch", test.desired, test.current), qt.Equals, test.want)
		})
	}
}

func TestClassifierStatements(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name: "create with a member",
			statement: ydbpool.CreateClassifierStatement("etl_users",
				ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 100}),
			want: "CREATE RESOURCE POOL CLASSIFIER `etl_users` WITH (RESOURCE_POOL = 'batch', RANK = 100, " +
				"MEMBER_NAME = 'etl');",
		},
		{
			name: "create for every query, escaping a quote",
			statement: ydbpool.CreateClassifierStatement("all",
				ast.ResourcePoolClassifierSpec{ResourcePool: `a'b`, Rank: 0}),
			want: "CREATE RESOURCE POOL CLASSIFIER `all` WITH (RESOURCE_POOL = 'a\\'b', RANK = 0);",
		},
		{
			// The pool and the rank are named whether or not they change: YDB
			// takes a rank the classifier already holds.
			name: "alter names the whole classifier",
			statement: ydbpool.AlterClassifierStatement("c",
				ast.ResourcePoolClassifierSpec{ResourcePool: "b", MemberName: "u2", Rank: 5},
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", MemberName: "u", Rank: 5}),
			want: "ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'b', RANK = 5, MEMBER_NAME = 'u2');",
		},
		{
			name: "alter resets a member the classifier stops naming",
			statement: ydbpool.AlterClassifierStatement("c",
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", Rank: 6},
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", MemberName: "u", Rank: 5}),
			want: "ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'a', RANK = 6), RESET (MEMBER_NAME);",
		},
		{
			name: "alter resets no member the classifier never named",
			statement: ydbpool.AlterClassifierStatement("c",
				ast.ResourcePoolClassifierSpec{ResourcePool: "b", Rank: 5},
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", Rank: 5}),
			want: "ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'b', RANK = 5);",
		},
		{
			name: "alter of the same classifier is nothing",
			statement: ydbpool.AlterClassifierStatement("c",
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", Rank: 5},
				ast.ResourcePoolClassifierSpec{ResourcePool: "a", Rank: 5}),
			want: "",
		},
		{
			name:      "drop a pool",
			statement: ydbpool.DropPoolStatement("batch"),
			want:      "DROP RESOURCE POOL `batch`;",
		},
		{
			name:      "drop a classifier",
			statement: ydbpool.DropClassifierStatement("c"),
			want:      "DROP RESOURCE POOL CLASSIFIER `c`;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.statement, qt.Equals, test.want)
		})
	}
}

func TestPoolsEqual(t *testing.T) {
	tests := []struct {
		name           string
		left, right    ast.ResourcePoolSpec
		wantEquivalent bool
	}{
		{name: "both unset", wantEquivalent: true},
		{
			name: "values, not pointers, are compared",
			left: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(3)), ResourceWeight: new(2.5)},
			right: ast.ResourcePoolSpec{
				ConcurrentQueryLimit: new(int32(3)), ResourceWeight: new(2.5),
			},
			wantEquivalent: true,
		},
		{
			name:  "zero is not unset",
			left:  ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(0))},
			right: ast.ResourcePoolSpec{},
		},
		{
			name:  "a fraction that differs",
			left:  ast.ResourcePoolSpec{DatabaseLoadCPUThreshold: new(80.5)},
			right: ast.ResourcePoolSpec{DatabaseLoadCPUThreshold: new(80.0)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpool.PoolsEqual(test.left, test.right), qt.Equals, test.wantEquivalent)
			c.Assert(ydbpool.PoolsEqual(test.right, test.left), qt.Equals, test.wantEquivalent)
		})
	}
}

package ydbpool_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbpool"
)

func TestParsePool_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ast.ResourcePoolSpec
	}{
		{
			name:   "a pool with no setting has no limit",
			values: map[string]string{"name": "batch"},
			want:   ast.ResourcePoolSpec{},
		},
		{
			name: "every setting, a fraction among them",
			values: map[string]string{
				"name": "batch", "concurrent_query_limit": "10", "queue_size": "20",
				"database_load_cpu_threshold": "80.5", "query_memory_limit_percent_per_node": "25",
				"query_cpu_limit_percent_per_node": "30", "total_cpu_limit_percent_per_node": "70",
				"resource_weight": "0",
			},
			want: ast.ResourcePoolSpec{
				ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(20)),
				DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
				QueryCPULimitPercentPerNode: new(30.0), TotalCPULimitPercentPerNode: new(70.0),
				ResourceWeight: new(0.0),
			},
		},
		{
			name:   "a limit of zero is a limit, not an unset one",
			values: map[string]string{"name": "frozen", "concurrent_query_limit": "0"},
			want:   ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(0))},
		},
		{
			name:   "a queue beside a load threshold rather than a query limit",
			values: map[string]string{"name": "q", "queue_size": "5", "database_load_cpu_threshold": "90"},
			want:   ast.ResourcePoolSpec{QueueSize: new(int32(5)), DatabaseLoadCPUThreshold: new(90.0)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbpool.ParsePool(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(name, qt.Equals, test.values["name"])
			c.Assert(spec, qt.DeepEquals, test.want)
		})
	}
}

func TestParsePool_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		values        map[string]string
		wantAttribute string
		wantErr       string
	}{
		{
			name:          "no name",
			values:        map[string]string{"concurrent_query_limit": "1"},
			wantAttribute: "name",
			wantErr:       `invalid name "": a resource pool needs a name`,
		},
		{
			name:          "a slash in the name",
			values:        map[string]string{"name": "a/b"},
			wantAttribute: "name",
			wantErr:       `invalid name "a/b": a name holds printable ASCII characters other than a slash and a space, as YDB requires`,
		},
		{
			name:          "a space in the name",
			values:        map[string]string{"name": "a b"},
			wantAttribute: "name",
			wantErr:       `invalid name "a b": .*`,
		},
		{
			name:          "a name outside ASCII",
			values:        map[string]string{"name": "пул"},
			wantAttribute: "name",
			wantErr:       `invalid name "пул": .*`,
		},
		{
			name:          "a negative limit, which YQL cannot write",
			values:        map[string]string{"name": "p", "concurrent_query_limit": "-1"},
			wantAttribute: "concurrent_query_limit",
			wantErr:       `invalid concurrent_query_limit "-1": takes a whole number from 0; leave it out for no limit`,
		},
		{
			name:          "a fraction where a whole number goes",
			values:        map[string]string{"name": "p", "concurrent_query_limit": "1.5"},
			wantAttribute: "concurrent_query_limit",
			wantErr:       `invalid concurrent_query_limit "1.5": .*`,
		},
		{
			name:          "a percentage above 100",
			values:        map[string]string{"name": "p", "query_memory_limit_percent_per_node": "150"},
			wantAttribute: "query_memory_limit_percent_per_node",
			wantErr:       `invalid query_memory_limit_percent_per_node "150": takes a percentage from 0 to 100; leave it out for no limit`,
		},
		{
			name:          "a weight above 100, which YDB keeps as a percentage",
			values:        map[string]string{"name": "p", "resource_weight": "1000000"},
			wantAttribute: "resource_weight",
			wantErr:       `invalid resource_weight "1000000": .*`,
		},
		{
			name:          "a queue with nothing to wait for",
			values:        map[string]string{"name": "p", "queue_size": "5"},
			wantAttribute: "queue_size",
			wantErr:       `invalid queue_size "5": a queue needs concurrent_query_limit or database_load_cpu_threshold beside it .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbpool.ParsePool(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declared *ydbpool.DeclarationError
			c.Assert(err, qt.ErrorAs, &declared)
			c.Assert(declared.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(name, qt.Equals, "")
			c.Assert(spec, qt.DeepEquals, ast.ResourcePoolSpec{})
		})
	}
}

func TestParseClassifier_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ast.ResourcePoolClassifierSpec
	}{
		{
			name:   "a member and a rank",
			values: map[string]string{"name": "c", "resource_pool": "batch", "member_name": "etl", "rank": "100"},
			want:   ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 100},
		},
		{
			name:   "every query, at rank zero, to the pool default",
			values: map[string]string{"name": "c", "resource_pool": "default", "rank": "0"},
			want:   ast.ResourcePoolClassifierSpec{ResourcePool: "default"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbpool.ParseClassifier(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(name, qt.Equals, "c")
			c.Assert(spec, qt.DeepEquals, test.want)
		})
	}
}

func TestParseClassifier_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		values        map[string]string
		wantAttribute string
		wantErr       string
	}{
		{
			name:          "no pool",
			values:        map[string]string{"name": "c", "rank": "1"},
			wantAttribute: "resource_pool",
			wantErr:       `invalid resource_pool "": a classifier names the pool it sends queries to .*`,
		},
		{
			name:          "no rank, which YDB would choose from the database",
			values:        map[string]string{"name": "c", "resource_pool": "p"},
			wantAttribute: "rank",
			wantErr:       `invalid rank "": a classifier needs a rank; .*`,
		},
		{
			name:          "a negative rank",
			values:        map[string]string{"name": "c", "resource_pool": "p", "rank": "-1"},
			wantAttribute: "rank",
			wantErr:       `invalid rank "-1": takes a whole number from 0`,
		},
		{
			name:          "a slash in the name",
			values:        map[string]string{"name": "c/1", "resource_pool": "p", "rank": "1"},
			wantAttribute: "name",
			wantErr:       `invalid name "c/1": .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbpool.ParseClassifier(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declared *ydbpool.DeclarationError
			c.Assert(err, qt.ErrorAs, &declared)
			c.Assert(declared.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(name, qt.Equals, "")
			c.Assert(spec, qt.DeepEquals, ast.ResourcePoolClassifierSpec{})
		})
	}
}

// withPools is a YDB line on a cluster whose EnableResourcePools flag is on.
func withPools() capability.Capabilities {
	return capability.YDB262().With(capability.ResourcePools, true)
}

func TestCheckPool_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbpool.CheckPool("batch", ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(1))}, withPools()),
		qt.IsNil)
	c.Assert(ydbpool.CheckPoolDrop("batch", withPools()), qt.IsNil)
	c.Assert(ydbpool.CheckClassifier("c", ast.ResourcePoolClassifierSpec{ResourcePool: "batch"}, withPools()),
		qt.IsNil)
}

func TestCheckPool_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		refusal *ydbpool.Refusal
		want    ydbpool.Refusal
	}{
		{
			name:    "a line whose flag is off",
			refusal: ydbpool.CheckPool("batch", ast.ResourcePoolSpec{}, capability.YDB262()),
			want: ydbpool.Refusal{
				Subject: `resource pool "batch"`, Key: capability.ResourcePools, Reason: ydbpool.FlagHint,
			},
		},
		{
			name:    "a classifier on a line whose flag is off",
			refusal: ydbpool.CheckClassifier("c", ast.ResourcePoolClassifierSpec{ResourcePool: "p"}, capability.YDB251()),
			want: ydbpool.Refusal{
				Subject: `resource pool classifier "c"`, Key: capability.ResourcePools, Reason: ydbpool.FlagHint,
			},
		},
		{
			name:    "a percentage built by hand out of range",
			refusal: ydbpool.CheckPool("p", ast.ResourcePoolSpec{ResourceWeight: new(101.0)}, withPools()),
			want: ydbpool.Refusal{
				Subject: `resource pool "p"`, Reason: "RESOURCE_WEIGHT takes a percentage from 0 to 100",
			},
		},
		{
			name:    "a negative limit built by hand",
			refusal: ydbpool.CheckPool("p", ast.ResourcePoolSpec{QueueSize: new(int32(-1))}, withPools()),
			want:    ydbpool.Refusal{Subject: `resource pool "p"`, Reason: "QUEUE_SIZE takes a whole number from 0"},
		},
		{
			name:    "a queue built by hand with nothing to wait for",
			refusal: ydbpool.CheckPool("p", ast.ResourcePoolSpec{QueueSize: new(int32(1))}, withPools()),
			want: ydbpool.Refusal{Subject: `resource pool "p"`, Reason: "a queue needs concurrent_query_limit or " +
				"database_load_cpu_threshold beside it (`queue_size unsupported without concurrent_query_limit or " +
				"database_load_cpu_threshold`)"},
		},
		{
			name:    "dropping the pool default",
			refusal: ydbpool.CheckPoolDrop("default", withPools()),
			want: ydbpool.Refusal{Subject: "DROP RESOURCE POOL default", Reason: "it is the pool YDB runs every " +
				"query in that no classifier sends elsewhere, and after it is dropped every query of the database " +
				"fails with `Resource pool default not found`"},
		},
		{
			name:    "a classifier built by hand with no pool",
			refusal: ydbpool.CheckClassifier("c", ast.ResourcePoolClassifierSpec{}, withPools()),
			want: ydbpool.Refusal{Subject: `resource pool classifier "c"`,
				Reason: "it names no pool (`Missing required property resource_pool`)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.refusal, qt.IsNotNil)
			c.Assert(*test.refusal, qt.DeepEquals, test.want)
		})
	}
}

func TestCheckRouting_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbpool.CheckRouting([]string{"batch"}, []ydbpool.Classifier{
		{Name: "a", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 1}},
		{Name: "b", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 2}},
	}), qt.IsNil)
}

func TestCheckRouting_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		pools       []string
		classifiers []ydbpool.Classifier
		want        ydbpool.Refusal
	}{
		{
			name:  "two classifiers on one rank",
			pools: []string{"batch"},
			classifiers: []ydbpool.Classifier{
				{Name: "a", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 7}},
				{Name: "b", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 7}},
			},
			want: ydbpool.Refusal{Subject: `resource pool classifier "b"`,
				Reason: `its rank 7 is the rank of classifier "a", and YDB keeps one classifier per rank`},
		},
		{
			name:  "a pool nobody declared, which YDB would route to default without a word",
			pools: []string{"batch"},
			classifiers: []ydbpool.Classifier{
				{Name: "a", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "Batch", Rank: 1}},
			},
			want: ydbpool.Refusal{Subject: `resource pool classifier "a"`, Reason: `it names resource pool "Batch", ` +
				`which is not declared; YDB runs the queries of a classifier whose pool does not exist in the pool ` +
				`"default" without a word`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := ydbpool.CheckRouting(test.pools, test.classifiers)
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(*refusal, qt.DeepEquals, test.want)
		})
	}
}

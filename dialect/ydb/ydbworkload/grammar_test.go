package ydbworkload_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestParsePool_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbworkload.PoolSpec
	}{
		{
			name:   "a pool with no setting has no limit",
			values: map[string]string{"name": "batch"},
			want:   ydbworkload.PoolSpec{},
		},
		{
			name: "every setting, a fraction among them",
			values: map[string]string{
				"name": "batch", "concurrent_query_limit": "10", "queue_size": "20",
				"database_load_cpu_threshold": "80.5", "query_memory_limit_percent_per_node": "25",
				"query_cpu_limit_percent_per_node": "30", "total_cpu_limit_percent_per_node": "70",
				"resource_weight": "0",
			},
			want: ydbworkload.PoolSpec{
				ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(20)),
				DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
				QueryCPULimitPercentPerNode: new(30.0), TotalCPULimitPercentPerNode: new(70.0),
				ResourceWeight: new(0.0),
			},
		},
		{
			name:   "a limit of zero is a limit, not an unset one",
			values: map[string]string{"name": "frozen", "concurrent_query_limit": "0"},
			want:   ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))},
		},
		{
			name:   "a queue beside a load threshold rather than a query limit",
			values: map[string]string{"name": "q", "queue_size": "5", "database_load_cpu_threshold": "90"},
			want:   ydbworkload.PoolSpec{QueueSize: new(int32(5)), DatabaseLoadCPUThreshold: new(90.0)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbworkload.ParsePool(test.values)
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
			name:          "a limit on the pool default, which YDB keeps unlimited",
			values:        map[string]string{"name": "default", "concurrent_query_limit": "5", "queue_size": "5"},
			wantAttribute: "concurrent_query_limit",
			wantErr:       `invalid concurrent_query_limit "5": the pool default takes no concurrent_query_limit: YDB keeps it unlimited \(` + "`" + `Can not change property concurrent_query_limit for default pool` + "`" + `\)`,
		},
		{
			name:          "a load threshold on the pool default",
			values:        map[string]string{"name": "default", "database_load_cpu_threshold": "80"},
			wantAttribute: "database_load_cpu_threshold",
			wantErr:       `invalid database_load_cpu_threshold "80": the pool default takes no database_load_cpu_threshold: .*`,
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
			name, spec, err := ydbworkload.ParsePool(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declared *ydbworkload.DeclarationError
			c.Assert(err, qt.ErrorAs, &declared)
			c.Assert(declared.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(name, qt.Equals, "")
			c.Assert(spec, qt.DeepEquals, ydbworkload.PoolSpec{})
		})
	}
}

func TestParseClassifier_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbworkload.ClassifierSpec
	}{
		{
			name:   "a member and a rank",
			values: map[string]string{"name": "c", "resource_pool": "batch", "member_name": "etl", "rank": "100"},
			want:   ydbworkload.ClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 100},
		},
		{
			name:   "every query, at rank zero, to the pool default",
			values: map[string]string{"name": "c", "resource_pool": "default", "rank": "0"},
			want:   ydbworkload.ClassifierSpec{ResourcePool: "default"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			name, spec, err := ydbworkload.ParseClassifier(test.values)
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
			name, spec, err := ydbworkload.ParseClassifier(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declared *ydbworkload.DeclarationError
			c.Assert(err, qt.ErrorAs, &declared)
			c.Assert(declared.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(name, qt.Equals, "")
			c.Assert(spec, qt.DeepEquals, ydbworkload.ClassifierSpec{})
		})
	}
}

// withPools is a YDB line on a cluster whose EnableResourcePools flag is on.
func withPools() capability.Capabilities {
	return capability.YDB262().With(capability.ResourcePools, true)
}

func TestCheckPool_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbworkload.CheckPool("batch", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(1))}, withPools()),
		qt.IsNil)
	c.Assert(ydbworkload.CheckPool("default", ydbworkload.PoolSpec{
		QueryMemoryLimitPercentPerNode: new(50.0), QueryCPULimitPercentPerNode: new(50.0),
		TotalCPULimitPercentPerNode: new(50.0), ResourceWeight: new(30.0),
	}, withPools()), qt.IsNil)
	c.Assert(ydbworkload.CheckPoolDrop("batch", withPools()), qt.IsNil)
	c.Assert(ydbworkload.CheckClassifier("c", ydbworkload.ClassifierSpec{ResourcePool: "batch"}, withPools()),
		qt.IsNil)
}

func TestCheckPool_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		refusal *ydbworkload.Refusal
		want    ydbworkload.Refusal
	}{
		{
			name:    "a line whose flag is off",
			refusal: ydbworkload.CheckPool("batch", ydbworkload.PoolSpec{}, capability.YDB262()),
			want: ydbworkload.Refusal{
				Subject: `resource pool "batch"`, Key: capability.ResourcePools, Reason: ydbworkload.FlagHint,
			},
		},
		{
			name:    "a classifier on a line whose flag is off",
			refusal: ydbworkload.CheckClassifier("c", ydbworkload.ClassifierSpec{ResourcePool: "p"}, capability.YDB251()),
			want: ydbworkload.Refusal{
				Subject: `resource pool classifier "c"`, Key: capability.ResourcePools, Reason: ydbworkload.FlagHint,
			},
		},
		{
			name:    "a percentage built by hand out of range",
			refusal: ydbworkload.CheckPool("p", ydbworkload.PoolSpec{ResourceWeight: new(101.0)}, withPools()),
			want: ydbworkload.Refusal{
				Subject: `resource pool "p"`, Reason: "RESOURCE_WEIGHT takes a percentage from 0 to 100",
			},
		},
		{
			name:    "a negative limit built by hand",
			refusal: ydbworkload.CheckPool("p", ydbworkload.PoolSpec{QueueSize: new(int32(-1))}, withPools()),
			want:    ydbworkload.Refusal{Subject: `resource pool "p"`, Reason: "QUEUE_SIZE takes a whole number from 0"},
		},
		{
			name: "a limit on the pool default built by hand",
			refusal: ydbworkload.CheckPool("default",
				ydbworkload.PoolSpec{DatabaseLoadCPUThreshold: new(80.0), ResourceWeight: new(30.0)}, withPools()),
			want: ydbworkload.Refusal{Subject: `resource pool "default"`, Reason: "the pool default takes no " +
				"database_load_cpu_threshold: YDB keeps it unlimited (`Can not change property " +
				"database_load_cpu_threshold for default pool`)"},
		},
		{
			name:    "a queue built by hand with nothing to wait for",
			refusal: ydbworkload.CheckPool("p", ydbworkload.PoolSpec{QueueSize: new(int32(1))}, withPools()),
			want: ydbworkload.Refusal{Subject: `resource pool "p"`, Reason: "a queue needs concurrent_query_limit or " +
				"database_load_cpu_threshold beside it (`queue_size unsupported without concurrent_query_limit or " +
				"database_load_cpu_threshold`)"},
		},
		{
			name:    "dropping the pool default",
			refusal: ydbworkload.CheckPoolDrop("default", withPools()),
			want: ydbworkload.Refusal{Subject: "DROP RESOURCE POOL default", Reason: "it is the pool YDB runs every " +
				"query in that no classifier sends elsewhere, and after it is dropped every query of the database " +
				"fails with `Resource pool default not found`"},
		},
		{
			name:    "a classifier built by hand with no pool",
			refusal: ydbworkload.CheckClassifier("c", ydbworkload.ClassifierSpec{}, withPools()),
			want: ydbworkload.Refusal{Subject: `resource pool classifier "c"`,
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
	c.Assert(ydbworkload.CheckRouting([]string{"batch"}, []ydbworkload.Classifier{
		{Name: "a", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1}},
		{Name: "b", Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 2}},
	}), qt.IsNil)
}

func TestCheckRouting_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		pools       []string
		classifiers []ydbworkload.Classifier
		want        ydbworkload.Refusal
	}{
		{
			name:  "two classifiers on one rank",
			pools: []string{"batch"},
			classifiers: []ydbworkload.Classifier{
				{Name: "a", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 7}},
				{Name: "b", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 7}},
			},
			want: ydbworkload.Refusal{Subject: `resource pool classifier "b"`,
				Reason: `its rank 7 is the rank of classifier "a", and YDB keeps one classifier per rank`},
		},
		{
			name:  "a pool nobody declared, which YDB would route to default without a word",
			pools: []string{"batch"},
			classifiers: []ydbworkload.Classifier{
				{Name: "a", Spec: ydbworkload.ClassifierSpec{ResourcePool: "Batch", Rank: 1}},
			},
			want: ydbworkload.Refusal{Subject: `resource pool classifier "a"`, Reason: `it names resource pool "Batch", ` +
				`which is not declared; YDB runs the queries of a classifier whose pool does not exist in the pool ` +
				`"default" without a word`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := ydbworkload.CheckRouting(test.pools, test.classifiers)
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(*refusal, qt.DeepEquals, test.want)
		})
	}
}

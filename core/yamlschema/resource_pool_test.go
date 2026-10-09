package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbworkload"
)

// TestParse_ResourcePool_HappyPath reads YDB resource pools and classifiers
// in YAML, keyed by name, under the keys the annotations read.
func TestParse_ResourcePool_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
resource_pools:
  reporting:
    concurrent_query_limit: 10
    queue_size: 20
    query_memory_limit_percent_per_node: 25.5
  default:
    resource_weight: 30
resource_pool_classifiers:
  reporters:
    resource_pool: reporting
    member_name: analysts
    rank: 100
  everyone:
    resource_pool: default
    rank: 0
`))

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ResourceWeight: new(30.0)}),
		ydbworkload.DesiredPoolObject("reporting", "", ydbworkload.PoolSpec{
			ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(20)), QueryMemoryLimitPercentPerNode: new(25.5),
		}),
		ydbworkload.DesiredClassifierObject("everyone", "", ydbworkload.ClassifierSpec{ResourcePool: "default"}),
		ydbworkload.DesiredClassifierObject("reporters", "", ydbworkload.ClassifierSpec{ResourcePool: "reporting", MemberName: "analysts", Rank: 100}),
	})
}

// TestParse_ResourcePool_FailurePath refuses what the annotations refuse,
// naming the pool or the classifier.
func TestParse_ResourcePool_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "a queue with nothing to wait for",
			yaml:    "resource_pools:\n  batch:\n    queue_size: 5\n",
			wantErr: `resource pool "batch": invalid queue_size "5": a queue needs .*`,
		},
		{
			name:    "a classifier without a rank, which YDB would choose from the database",
			yaml:    "resource_pool_classifiers:\n  c:\n    resource_pool: default\n",
			wantErr: `resource pool classifier "c": invalid rank "": a classifier needs a rank; .*`,
		},
		{
			name:    "a classifier without a pool",
			yaml:    "resource_pool_classifiers:\n  c:\n    rank: 1\n",
			wantErr: `resource pool classifier "c": invalid resource_pool "": .*`,
		},
		{
			name:    "an empty setting, which is not an unset one",
			yaml:    "resource_pools:\n  batch:\n    concurrent_query_limit: \"\"\n",
			wantErr: `resource pool "batch": invalid concurrent_query_limit "": .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(test.yaml))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestParse_YDBTablePartitioning_HappyPath reads a YDB row table's settings in
// YAML under the keys the annotation reads, with the same spellings.
func TestParse_YDBTablePartitioning_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
tables:
  items:
    auto_partitioning_by_size: disabled
    auto_partitioning_by_load: ENABLED
    auto_partitioning_min_partitions_count: 3
    auto_partitioning_max_partitions_count: 9
    read_replicas_settings: ANY_AZ:2
    key_bloom_filter: enabled
    partition_at_keys: "(10, 'a'), 20"
    columns:
      id:
        type: bigint
        primary: true
  plain:
    columns:
      id:
        type: bigint
        primary: true
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Name, qt.Equals, "items")
	declared, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](db.Tables[0].Facets, ydbschema.TablePartitioningKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(declared.TablePartitioning, qt.DeepEquals, ydbschema.TablePartitioning{
		BySize: new(false), ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "ANY_AZ:2",
		KeyBloomFilter: new(true), PartitionAtKeys: [][]string{{"10", "a"}, {"20"}},
	})
	c.Assert(db.Tables[1].Facets.Kinds(), qt.HasLen, 0)
}

// TestParse_YDBTablePartitioning_FailurePath refuses a value YDB would refuse,
// an empty one included, naming the table.
func TestParse_YDBTablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		setting string
		wantErr string
	}{
		{name: "zero uniform partitions", setting: "uniform_partitions: 0",
			wantErr: `table "items": invalid uniform_partitions "0": write a whole number of at least 1; .*`},
		{name: "an empty filter", setting: `key_bloom_filter: ""`,
			wantErr: `table "items": invalid key_bloom_filter "": write ENABLED or DISABLED`},
		{name: "an empty split point list", setting: `partition_at_keys: ""`,
			wantErr: `table "items": invalid partition_at_keys "": name at least one split point, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(`
tables:
  items:
    ` + test.setting + `
    columns:
      id:
        type: bigint
        primary: true
`))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

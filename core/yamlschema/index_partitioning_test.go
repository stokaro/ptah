package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_YDBGlobalIndex_HappyPath reads a YDB global index in YAML: its
// covered columns under include, its kind under type, and its partitioning
// under the keys the annotation reads, with the same spellings.
func TestParse_YDBGlobalIndex_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
tables:
  items:
    columns:
      id:
        type: bigint
        primary: true
      kind:
        type: text
      price:
        type: bigint
    indexes:
      idx_items_price:
        fields: [price]
        include: [kind]
        type: async
        auto_partitioning_by_size: disabled
        auto_partitioning_by_load: ENABLED
        auto_partitioning_min_partitions_count: 3
        auto_partitioning_max_partitions_count: 9
        read_replicas_settings: ANY_AZ:2
      idx_items_kind:
        fields: kind
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.DeepEquals, []schemamodel.Index{
		{StructName: "items", TableName: "items", Name: "idx_items_price", Fields: []string{"price"}, IncludeColumns: []string{"kind"}, Type: "async",
			Partitioning: &ast.IndexPartitioningSpec{
				BySize: new(false), ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "ANY_AZ:2",
			}},
		{StructName: "items", TableName: "items", Name: "idx_items_kind", Fields: []string{"kind"}},
	})
}

// TestParse_YDBGlobalIndex_FailurePath refuses a partitioning value YDB would
// refuse, an empty one included, and an empty covered column.
func TestParse_YDBGlobalIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		index   string
		wantErr string
	}{
		{name: "a count of zero", index: "auto_partitioning_partition_size_mb: 0",
			wantErr: `.*index "i": invalid auto_partitioning_partition_size_mb "0": write a whole number of at least 1; .*`},
		{name: "an empty switch", index: `auto_partitioning_by_size: ""`,
			wantErr: `.*index "i": invalid auto_partitioning_by_size "": write ENABLED or DISABLED`},
		{name: "a list of read replicas", index: "read_replicas_settings: PER_AZ:1,ANY_AZ:1",
			wantErr: `.*index "i": invalid read_replicas_settings "PER_AZ:1,ANY_AZ:1": .*`},
		{name: "an empty covered column", index: "include: [kind, '']",
			wantErr: `.*index "i" names an empty include column`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(`
tables:
  items:
    columns:
      id:
        type: bigint
        primary: true
      kind:
        type: text
    indexes:
      i:
        fields: [kind]
        ` + test.index + `
`))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

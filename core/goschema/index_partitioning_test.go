package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// partitionedIndexSource is an entity whose index carries attributes.
func partitionedIndexSource(attributes string) string {
	return `package entities

//ptah:schema:table name="items"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="kind" type="TEXT"
	//ptah:schema:index name="idx_items_kind" fields="kind" ` + attributes + `
	Kind string
}
`
}

// TestParseSource_IndexPartitioning_HappyPath reads the partitioning of a YDB
// global index, spelled as YDB spells the settings, into the index's spec:
// the switches in either case, the counts, and the read replicas written back
// in capitals, or as nothing for a count of zero.
func TestParseSource_IndexPartitioning_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       *ast.IndexPartitioningSpec
	}{
		{name: "none", attributes: `type="async"`, want: nil},
		{
			name: "every setting",
			attributes: `auto_partitioning_by_size="enabled" auto_partitioning_partition_size_mb="512" ` +
				`auto_partitioning_by_load="ENABLED" auto_partitioning_min_partitions_count="3" ` +
				`auto_partitioning_max_partitions_count="9" read_replicas_settings="per_az:1"`,
			want: &ast.IndexPartitioningSpec{
				BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9,
				ReadReplicas: "PER_AZ:1",
			},
		},
		{name: "a switch turned off", attributes: `auto_partitioning_by_size="DISABLED"`,
			want: &ast.IndexPartitioningSpec{BySize: new(false)}},
		{name: "no read replicas", attributes: `read_replicas_settings="ANY_AZ:0"`, want: &ast.IndexPartitioningSpec{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", partitionedIndexSource(test.attributes))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Partitioning, qt.DeepEquals, test.want)
		})
	}
}

// TestParseSource_IndexPartitioning_FailurePath refuses a value YDB would
// refuse where it was written: a switch other than ENABLED or DISABLED, a
// count of zero, which a declaration could otherwise not tell from none, and
// read replicas in a form YDB does not take.
func TestParseSource_IndexPartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
	}{
		{name: "a boolean for a switch", attributes: `auto_partitioning_by_load="true"`,
			wantErr: `.*invalid auto_partitioning_by_load "true": write ENABLED or DISABLED on //ptah:schema:index at Item`},
		{name: "a count of zero", attributes: `auto_partitioning_min_partitions_count="0"`,
			wantErr: `.*invalid auto_partitioning_min_partitions_count "0": write a whole number of at least 1; .*`},
		{name: "a count that is not a number", attributes: `auto_partitioning_partition_size_mb="2GB"`,
			wantErr: `.*invalid auto_partitioning_partition_size_mb "2GB": .*`},
		{name: "read replicas with no mode", attributes: `read_replicas_settings="2"`,
			wantErr: `.*invalid read_replicas_settings "2": write PER_AZ:<n> .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", partitionedIndexSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

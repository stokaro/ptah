package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
)

// partitionedTableSource is an entity whose table directive carries
// attributes.
func partitionedTableSource(attributes string) string {
	return `package entities

//ptah:schema:table name="items" ` + attributes + `
type Item struct {
	//ptah:schema:field name="id" type="BIGINT UNSIGNED" primary="true"
	ID uint64
}
`
}

// declaredPartitioning is the settings a table's YDB owner facet declares, or
// nil where the table states none.
func declaredPartitioning(c *qt.C, table schemamodel.Table) *ydbschema.TablePartitioning {
	c.Helper()
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](table.Facets, ydbschema.TablePartitioningKind)
	c.Assert(err, qt.IsNil)
	if !found {
		return nil
	}
	return &value.TablePartitioning
}

// TestParseSource_TablePartitioning_HappyPath reads a YDB row table's settings,
// spelled as YDB spells them, into the YDB owner's facet: the switches in either
// case, the counts, the read replicas written back in capitals, and the split
// points in YQL's own list.
func TestParseSource_TablePartitioning_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       *ydbschema.TablePartitioning
	}{
		{name: "none", attributes: `comment="x"`, want: nil},
		{
			name: "every setting but the split points",
			attributes: `auto_partitioning_by_size="enabled" auto_partitioning_partition_size_mb="512" ` +
				`auto_partitioning_by_load="ENABLED" auto_partitioning_min_partitions_count="3" ` +
				`auto_partitioning_max_partitions_count="9" read_replicas_settings="per_az:1" ` +
				`key_bloom_filter="Enabled" uniform_partitions="4"`,
			want: &ydbschema.TablePartitioning{
				BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9,
				ReadReplicas: "PER_AZ:1", KeyBloomFilter: new(true), UniformPartitions: 4,
			},
		},
		{name: "split points", attributes: `partition_at_keys="(10, 'a'), 20"`,
			want: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10", "a"}, {"20"}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(ydbOwners, "items.go", partitionedTableSource(test.attributes))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(declaredPartitioning(c, db.Tables[0]), qt.DeepEquals, test.want)
		})
	}
}

// TestParseSource_TablePartitioning_FailurePath refuses a value YDB would
// refuse where it was written, naming the attribute.
func TestParseSource_TablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
	}{
		{name: "a boolean for the filter", attributes: `key_bloom_filter="true"`,
			wantErr: `.*invalid key_bloom_filter "true": write ENABLED or DISABLED on //ptah:schema:table at Item`},
		{name: "zero uniform partitions", attributes: `uniform_partitions="0"`,
			wantErr: `.*invalid uniform_partitions "0": write a whole number of at least 1; .*`},
		{name: "an unclosed split point", attributes: `partition_at_keys="(10, 20"`,
			wantErr: `.*invalid partition_at_keys "\(10, 20": expected a comma or a closing parenthesis at offset 7 .*`},
		{name: "a count of zero", attributes: `auto_partitioning_max_partitions_count="0"`,
			wantErr: `.*invalid auto_partitioning_max_partitions_count "0": .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(ydbOwners, "items.go", partitionedTableSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

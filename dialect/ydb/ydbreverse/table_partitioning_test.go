package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
)

func settingsRecord(after ydbschema.TablePartitioning, before *ydbschema.TablePartitioning) schemaext.ChangeRecord {
	change := &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedTablePartitioning{TablePartitioning: *before}
	}
	return schemaext.ChangeRecord{Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("items"), Value: change}
}

// TestReverseTablePartitioning_RestoresEverySettingHeld pins each reversal: a
// declaration naming every setting the table held, read over what the
// forward change leaves, so a setting at YDB's default is restored too; the
// forward state is what the change leaves, as a read writes it; and a
// maximum YDB cannot remove is reported.
func TestReverseTablePartitioning_RestoresEverySettingHeld(t *testing.T) {
	tests := []struct {
		name            string
		record          schemaext.ChangeRecord
		wantAfter       ydbschema.TablePartitioning
		wantBefore      *ydbschema.ObservedTablePartitioning
		wantLimitations []string
	}{
		{
			name:   "a minimum raised from the default",
			record: settingsRecord(ydbschema.TablePartitioning{MinPartitions: 4}, nil),
			wantAfter: ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1,
				ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)},
			wantBefore: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 4}},
		},
		{
			name:   "a maximum set where the table had none",
			record: settingsRecord(ydbschema.TablePartitioning{MaxPartitions: 9}, &ydbschema.TablePartitioning{ByLoad: new(true)}),
			wantAfter: ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(true), MinPartitions: 1,
				ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)},
			wantBefore:      &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true), MaxPartitions: 9}},
			wantLimitations: []string{"YDB cannot remove a maximum partition count, so the rollback keeps AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, which the forward change set."},
		},
		{
			name:   "a maximum changed where the table had one",
			record: settingsRecord(ydbschema.TablePartitioning{MaxPartitions: 9}, &ydbschema.TablePartitioning{MaxPartitions: 5}),
			wantAfter: ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1,
				MaxPartitions: 5, ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)},
			wantBefore: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MaxPartitions: 9}},
		},
		{
			name:   "settings declared back to the defaults",
			record: settingsRecord(ydbschema.TablePartitioning{KeyBloomFilter: new(false)}, &ydbschema.TablePartitioning{KeyBloomFilter: new(true)}),
			wantAfter: ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1,
				ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(true)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbreverse.TablePartitioningService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{test.record}})

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			reversed := result[0].Change.Value.(*ydbdiff.TablePartitioning)
			c.Assert(reversed.After.TablePartitioning, qt.DeepEquals, test.wantAfter)
			c.Assert(reversed.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(result[0].ForwardState[0].Kind, qt.Equals, ydbschema.TablePartitioningKind)
			c.Assert(result[0].Strategy, qt.Equals, "restore every setting the table held in place")
			c.Assert(result[0].Limitations, qt.DeepEquals, test.wantLimitations)
		})
	}
}

func TestReverseTablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
		wantErr string
	}{
		{name: "another target", request: schemaext.ReversalRequest{Target: "postgres"}, wantErr: `.*YDB table partitioning reversal on "postgres".*`},
		{name: "a side YDB could not hold", request: schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{
			settingsRecord(ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}, nil)}},
			wantErr: `.*auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled.*`},
		{name: "no declaration", request: schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{
			{Subject: settingsRecord(ydbschema.TablePartitioning{}, nil).Subject, Value: &ydbdiff.TablePartitioning{}}}},
			wantErr: `.*requires the settings the declaration states`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbreverse.TablePartitioningService{}.ReverseChanges(t.Context(), test.request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}

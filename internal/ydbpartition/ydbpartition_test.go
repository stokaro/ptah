package ydbpartition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbpartition"
)

// TestDeclaredOver reads a declaration over what a table holds: each setting
// it names, and the held value of each it leaves out.
func TestDeclaredOver(t *testing.T) {
	held := ydbpartition.Settings{
		BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
		ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
	}
	notSplitting := ydbpartition.Settings{MinPartitions: 3, MaxPartitions: 20}

	tests := []struct {
		name     string
		declared ydbpartition.Declared
		held     ydbpartition.Settings
		want     ydbpartition.Settings
	}{
		{name: "nothing declared keeps every held setting", held: held, want: held},
		{
			name:     "a declared minimum keeps the rest",
			declared: ydbpartition.Declared{MinPartitions: 2},
			held:     held,
			want: ydbpartition.Settings{BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 2, MaxPartitions: 20,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1}},
		},
		{
			name:     "splitting by size on keeps the held size",
			declared: ydbpartition.Declared{BySize: new(true)},
			held:     held,
			want:     held,
		},
		{
			name:     "splitting by size on with no size held takes YDB's",
			declared: ydbpartition.Declared{BySize: new(true)},
			held:     notSplitting,
			want:     ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 3, MaxPartitions: 20},
		},
		{
			name:     "a size alone splits by size",
			declared: ydbpartition.Declared{PartitionSizeMB: 64},
			held:     notSplitting,
			want:     ydbpartition.Settings{BySize: true, PartitionSizeMB: 64, MinPartitions: 3, MaxPartitions: 20},
		},
		{
			name:     "splitting by size off drops the size",
			declared: ydbpartition.Declared{BySize: new(false)},
			held:     held,
			want: ydbpartition.Settings{ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1}},
		},
		{
			name:     "replicas declared as none",
			declared: ydbpartition.Declared{ReadReplicas: "PER_AZ:0"},
			held:     held,
			want: ydbpartition.Settings{BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.declared.Over(test.held)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDeclaredOver_FailurePath refuses a size beside splitting by size
// declared off, which YDB refuses whatever the table holds.
func TestDeclaredOver_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := ydbpartition.Declared{BySize: new(false), PartitionSizeMB: 100}.Over(ydbpartition.DefaultSettings())
	c.Assert(err, qt.ErrorMatches, `auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`)
	c.Assert(got, qt.Equals, ydbpartition.Settings{})
}

// TestDeclaredCreateClause writes each setting a declaration names for an
// object YDB has just created, and nothing for one it leaves out.
func TestDeclaredCreateClause(t *testing.T) {
	tests := []struct {
		name     string
		declared ydbpartition.Declared
		want     []string
	}{
		{name: "nothing declared", want: nil},
		{
			name: "every setting",
			declared: ydbpartition.Declared{BySize: new(true), PartitionSizeMB: 64, ByLoad: new(false), MinPartitions: 2,
				MaxPartitions: 8, ReadReplicas: "ANY_AZ:2"},
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 64",
				"AUTO_PARTITIONING_BY_LOAD = DISABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 8",
				`READ_REPLICAS_SETTINGS = "ANY_AZ:2"`,
			},
		},
		{name: "no replicas are not written", declared: ydbpartition.Declared{ReadReplicas: "PER_AZ:0"}, want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.declared.CreateClause(), qt.DeepEquals, test.want)
		})
	}
}

// TestClause names every splitting setting a table or an index takes
// whenever it names any, because setting one resets others, and clears read
// replicas only where it holds some.
func TestClause(t *testing.T) {
	defaults := ydbpartition.DefaultSettings()
	tuned := ydbpartition.Settings{
		BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 9,
		ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
	}
	noSplitting := ydbpartition.Settings{MinPartitions: 3}

	tests := []struct {
		name             string
		desired, current ydbpartition.Settings
		want             []string
	}{
		{name: "equal settings write nothing", desired: tuned, current: tuned, want: nil},
		{
			name:    "replicas of zero equal none",
			desired: defaults,
			current: ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 1, ReadReplicas: ydbpartition.Replicas{PerAZ: true}},
			want:    nil,
		},
		{
			name: "tuned settings", desired: tuned, current: defaults,
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 100",
				"AUTO_PARTITIONING_BY_LOAD = ENABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9",
				`READ_REPLICAS_SETTINGS = "PER_AZ:1"`,
			},
		},
		{
			name: "back to the defaults clears the replicas", desired: ydbpartition.Settings{
				BySize: true, PartitionSizeMB: 2048, MinPartitions: 1, MaxPartitions: 9,
			}, current: tuned,
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048",
				"AUTO_PARTITIONING_BY_LOAD = DISABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9",
				`READ_REPLICAS_SETTINGS = "PER_AZ:0"`,
			},
		},
		{
			// The minimum is the same on both sides and is named anyway: setting
			// AUTO_PARTITIONING_BY_LOAD alone would reset it to 1.
			name:    "a setting that resets another names the other too",
			desired: ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, ByLoad: true, MinPartitions: 5},
			current: ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 5},
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048",
				"AUTO_PARTITIONING_BY_LOAD = ENABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5",
			},
		},
		{
			name: "no splitting by size names no size", desired: noSplitting, current: defaults,
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = DISABLED",
				"AUTO_PARTITIONING_BY_LOAD = DISABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpartition.Clause(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

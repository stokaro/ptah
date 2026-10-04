package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
)

// TestResolve_HappyPath reads each declaration as the settings an index takes
// from it: a setting left out is YDB's default, and a partition size has no
// value on an index that does not split by size.
func TestResolve_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.IndexPartitioningSpec
		want ydbindex.Settings
	}{
		{name: "nil is the defaults", spec: nil, want: ydbindex.DefaultSettings()},
		{name: "empty is the defaults", spec: &ast.IndexPartitioningSpec{}, want: ydbindex.DefaultSettings()},
		{
			name: "every setting",
			spec: &ast.IndexPartitioningSpec{
				BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true),
				MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "per_az:2",
			},
			want: ydbindex.Settings{
				BySize: true, PartitionSizeMB: 512, ByLoad: true, MinPartitions: 3, MaxPartitions: 9,
				ReadReplicas: ydbindex.Replicas{PerAZ: true, Count: 2},
			},
		},
		{
			name: "not splitting by size keeps no size",
			spec: &ast.IndexPartitioningSpec{BySize: new(false)},
			want: ydbindex.Settings{MinPartitions: 1},
		},
		{
			name: "replicas in all zones together",
			spec: &ast.IndexPartitioningSpec{ReadReplicas: "ANY_AZ:3"},
			want: ydbindex.Settings{
				BySize: true, PartitionSizeMB: 2048, MinPartitions: 1,
				ReadReplicas: ydbindex.Replicas{Count: 3},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.Resolve(test.spec)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestResolve_FailurePath refuses a declaration YDB refuses, which could never
// be read back and would plan the same change on every run.
func TestResolve_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		spec    *ast.IndexPartitioningSpec
		wantErr string
	}{
		{
			name:    "a size without splitting by size",
			spec:    &ast.IndexPartitioningSpec{BySize: new(false), PartitionSizeMB: 100},
			wantErr: `auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`,
		},
		{
			name:    "replicas with no mode",
			spec:    &ast.IndexPartitioningSpec{ReadReplicas: "2"},
			wantErr: `read replicas "2" are not one YDB takes: .*`,
		},
		{
			name:    "replicas in an unknown mode",
			spec:    &ast.IndexPartitioningSpec{ReadReplicas: "EVERY_AZ:2"},
			wantErr: `read replicas "EVERY_AZ:2" are not one YDB takes: .*`,
		},
		{
			name:    "a list of replica settings",
			spec:    &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:1,ANY_AZ:1"},
			wantErr: `read replicas "PER_AZ:1,ANY_AZ:1" are not one YDB takes: .*`,
		},
		{
			name:    "a negative replica count",
			spec:    &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:-1"},
			wantErr: `read replicas "PER_AZ:-1" are not one YDB takes: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.Resolve(test.spec)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbindex.Settings{})
		})
	}
}

// TestSettings_Spec writes what differs from the defaults, which is what the
// reader reports, and resolving it gives the settings back.
func TestSettings_Spec(t *testing.T) {
	tests := []struct {
		name     string
		settings ydbindex.Settings
		want     *ast.IndexPartitioningSpec
	}{
		{name: "the defaults are nil", settings: ydbindex.DefaultSettings(), want: nil},
		{
			name:     "replicas of zero are none",
			settings: ydbindex.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 1, ReadReplicas: ydbindex.Replicas{PerAZ: true}},
			want:     nil,
		},
		{
			name: "every setting off its default",
			settings: ydbindex.Settings{
				BySize: true, PartitionSizeMB: 64, ByLoad: true, MinPartitions: 7, MaxPartitions: 9,
				ReadReplicas: ydbindex.Replicas{Count: 2},
			},
			want: &ast.IndexPartitioningSpec{
				PartitionSizeMB: 64, ByLoad: new(true), MinPartitions: 7, MaxPartitions: 9, ReadReplicas: "ANY_AZ:2",
			},
		},
		{
			name:     "not splitting by size",
			settings: ydbindex.Settings{MinPartitions: 1},
			want:     &ast.IndexPartitioningSpec{BySize: new(false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := test.settings.Spec()
			c.Assert(spec, qt.DeepEquals, test.want)
			resolved, err := ydbindex.Resolve(spec)
			c.Assert(err, qt.IsNil)
			c.Assert(resolved.Equal(test.settings), qt.IsTrue)
		})
	}
}

// TestChangeRefusal refuses the one change YDB cannot make in place: removing
// a maximum partition count.
func TestChangeRefusal(t *testing.T) {
	defaults := ydbindex.DefaultSettings()
	capped := defaults
	capped.MaxPartitions = 9
	recapped := defaults
	recapped.MaxPartitions = 4

	tests := []struct {
		name             string
		desired, current ydbindex.Settings
		want             string
	}{
		{name: "nothing changes", desired: defaults, current: defaults, want: ""},
		{name: "a maximum is set", desired: capped, current: defaults, want: ""},
		{name: "a maximum changes", desired: recapped, current: capped, want: ""},
		{
			name: "a maximum is removed", desired: defaults, current: capped,
			want: "its maximum of 9 partitions cannot be removed in place (`Can't set max partition count to 0`, and no RESET)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.ChangeRefusal(test.desired, test.current), qt.Equals, test.want)
		})
	}
}

// TestClause names every setting the index takes whenever it names any,
// because setting one resets others, and clears read replicas only where the
// index holds some.
func TestClause(t *testing.T) {
	defaults := ydbindex.DefaultSettings()
	tuned := ydbindex.Settings{
		BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 9,
		ReadReplicas: ydbindex.Replicas{PerAZ: true, Count: 1},
	}
	noSplitting := ydbindex.Settings{MinPartitions: 3}

	tests := []struct {
		name             string
		desired, current ydbindex.Settings
		want             []string
	}{
		{name: "equal settings write nothing", desired: tuned, current: tuned, want: nil},
		{
			name:    "replicas of zero equal none",
			desired: defaults,
			current: ydbindex.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 1, ReadReplicas: ydbindex.Replicas{PerAZ: true}},
			want:    nil,
		},
		{
			name: "a tuned index", desired: tuned, current: defaults,
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
			name: "back to the defaults clears the replicas", desired: ydbindex.Settings{
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
			c.Assert(ydbindex.Clause(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

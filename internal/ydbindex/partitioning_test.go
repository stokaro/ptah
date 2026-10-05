package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// tuned is an index's own table holding a setting off every default.
var tuned = ydbpartition.Settings{
	BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
	ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
}

// TestResolve_HappyPath reads each declaration over what the index holds: a
// setting it names, and the held value of each it leaves out, so a plan never
// changes a setting nobody declared.
func TestResolve_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.IndexPartitioningSpec
		held ydbpartition.Settings
		want ydbpartition.Settings
	}{
		{name: "nil keeps what the index holds", spec: nil, held: tuned, want: tuned},
		{name: "empty keeps what the index holds", spec: &ast.IndexPartitioningSpec{}, held: tuned, want: tuned},
		{
			name: "every setting",
			spec: &ast.IndexPartitioningSpec{
				BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true),
				MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "per_az:2",
			},
			held: ydbpartition.DefaultSettings(),
			want: ydbpartition.Settings{
				BySize: true, PartitionSizeMB: 512, ByLoad: true, MinPartitions: 3, MaxPartitions: 9,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 2},
			},
		},
		{
			name: "one setting keeps the others held",
			spec: &ast.IndexPartitioningSpec{ByLoad: new(false)},
			held: tuned,
			want: ydbpartition.Settings{
				BySize: true, PartitionSizeMB: 100, MinPartitions: 6, MaxPartitions: 20,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
			},
		},
		{
			name: "not splitting by size keeps no size",
			spec: &ast.IndexPartitioningSpec{BySize: new(false)},
			held: tuned,
			want: ydbpartition.Settings{
				ByLoad: true, MinPartitions: 6, MaxPartitions: 20, ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
			},
		},
		{
			name: "a size alone splits by size",
			spec: &ast.IndexPartitioningSpec{PartitionSizeMB: 64},
			held: ydbpartition.Settings{MinPartitions: 1},
			want: ydbpartition.Settings{BySize: true, PartitionSizeMB: 64, MinPartitions: 1},
		},
		{
			name: "splitting by size turned on takes YDB's size",
			spec: &ast.IndexPartitioningSpec{BySize: new(true)},
			held: ydbpartition.Settings{MinPartitions: 1},
			want: ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 1},
		},
		{
			name: "replicas declared as none",
			spec: &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:0"},
			held: tuned,
			want: ydbpartition.Settings{BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true}},
		},
		{
			name: "replicas in all zones together",
			spec: &ast.IndexPartitioningSpec{ReadReplicas: "ANY_AZ:3"},
			held: ydbpartition.DefaultSettings(),
			want: ydbpartition.Settings{
				BySize: true, PartitionSizeMB: 2048, MinPartitions: 1,
				ReadReplicas: ydbpartition.Replicas{Count: 3},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.Resolve(test.spec, test.held)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestExplicit names every setting, so the declaration resolves to the
// settings it was written from over an index holding others. The index it is
// read over has no maximum, because no declaration names the absence of one:
// YDB cannot remove a maximum.
func TestExplicit(t *testing.T) {
	tests := []struct {
		name     string
		settings ydbpartition.Settings
		want     *ast.IndexPartitioningSpec
	}{
		{
			name:     "the defaults",
			settings: ydbpartition.DefaultSettings(),
			want: &ast.IndexPartitioningSpec{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1,
				ReadReplicas: "PER_AZ:0"},
		},
		{
			name:     "not splitting by size",
			settings: ydbpartition.Settings{ByLoad: true, MinPartitions: 2, MaxPartitions: 4, ReadReplicas: ydbpartition.Replicas{Count: 1}},
			want: &ast.IndexPartitioningSpec{BySize: new(false), ByLoad: new(true), MinPartitions: 2, MaxPartitions: 4,
				ReadReplicas: "ANY_AZ:1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ydbindex.Explicit(test.settings)
			c.Assert(spec, qt.DeepEquals, test.want)
			other := ydbpartition.Settings{BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6,
				ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1}}
			resolved, err := ydbindex.Resolve(spec, other)
			c.Assert(err, qt.IsNil)
			c.Assert(resolved.Equal(test.settings), qt.IsTrue, qt.Commentf("%+v", resolved))
		})
	}
}

// TestCreateClause writes each setting a new index's declaration names, and
// nothing for one it leaves out, which the index takes from YDB.
func TestCreateClause(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.IndexPartitioningSpec
		want []string
	}{
		{name: "nothing declared", spec: nil, want: nil},
		{name: "one setting", spec: &ast.IndexPartitioningSpec{ByLoad: new(true)}, want: []string{"AUTO_PARTITIONING_BY_LOAD = ENABLED"}},
		{
			name: "a setting at YDB's default is written as declared",
			spec: &ast.IndexPartitioningSpec{MinPartitions: 1, ReadReplicas: "PER_AZ:0"},
			want: []string{"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.CreateClause(test.spec), qt.DeepEquals, test.want)
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
			got, err := ydbindex.Resolve(test.spec, ydbpartition.DefaultSettings())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbpartition.Settings{})
		})
	}
}

// TestSpec writes what differs from the defaults, which is what the
// reader reports, and reading the report gives the settings back.
func TestSpec(t *testing.T) {
	tests := []struct {
		name     string
		settings ydbpartition.Settings
		want     *ast.IndexPartitioningSpec
	}{
		{name: "the defaults are nil", settings: ydbpartition.DefaultSettings(), want: nil},
		{
			name:     "replicas of zero are none",
			settings: ydbpartition.Settings{BySize: true, PartitionSizeMB: 2048, MinPartitions: 1, ReadReplicas: ydbpartition.Replicas{PerAZ: true}},
			want:     nil,
		},
		{
			name: "every setting off its default",
			settings: ydbpartition.Settings{
				BySize: true, PartitionSizeMB: 64, ByLoad: true, MinPartitions: 7, MaxPartitions: 9,
				ReadReplicas: ydbpartition.Replicas{Count: 2},
			},
			want: &ast.IndexPartitioningSpec{
				PartitionSizeMB: 64, ByLoad: new(true), MinPartitions: 7, MaxPartitions: 9, ReadReplicas: "ANY_AZ:2",
			},
		},
		{
			name:     "not splitting by size",
			settings: ydbpartition.Settings{MinPartitions: 1},
			want:     &ast.IndexPartitioningSpec{BySize: new(false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ydbindex.Spec(test.settings)
			c.Assert(spec, qt.DeepEquals, test.want)
			held, err := ydbindex.Held(spec)
			c.Assert(err, qt.IsNil)
			c.Assert(held.Equal(test.settings), qt.IsTrue)
		})
	}
}

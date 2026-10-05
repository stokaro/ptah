package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// TestParseDeclaration_HappyPath reads the partitioning attributes out of an
// index declaration's values and ignores every other key; a declaration with
// none of them has no partitioning.
func TestParseDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *ast.IndexPartitioningSpec
	}{
		{name: "no partitioning", values: map[string]string{"name": "i", "fields": "a"}, want: nil},
		{
			name: "every attribute",
			values: map[string]string{
				"name": "i", "auto_partitioning_by_size": " Enabled ", "auto_partitioning_partition_size_mb": "64",
				"auto_partitioning_by_load": "disabled", "auto_partitioning_min_partitions_count": "2",
				"auto_partitioning_max_partitions_count": "8", "read_replicas_settings": "any_az:3",
			},
			want: &ast.IndexPartitioningSpec{
				BySize: new(true), PartitionSizeMB: 64, ByLoad: new(false), MinPartitions: 2, MaxPartitions: 8, ReadReplicas: "ANY_AZ:3",
			},
		},
		{name: "no replicas is a declaration of none", values: map[string]string{"read_replicas_settings": "per_az:0"},
			want: &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:0"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ParseDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseDeclaration_FailurePath refuses each attribute's value YDB would
// refuse, naming the attribute, and reports the first in attribute order.
func TestParseDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		values        map[string]string
		wantAttribute string
		wantErr       string
	}{
		{name: "a boolean", values: map[string]string{"auto_partitioning_by_size": "yes"},
			wantAttribute: "auto_partitioning_by_size", wantErr: `invalid auto_partitioning_by_size "yes": write ENABLED or DISABLED`},
		{name: "a zero size", values: map[string]string{"auto_partitioning_partition_size_mb": "0"},
			wantAttribute: "auto_partitioning_partition_size_mb", wantErr: `invalid auto_partitioning_partition_size_mb "0": .*`},
		{name: "a negative count", values: map[string]string{"auto_partitioning_max_partitions_count": "-1"},
			wantAttribute: "auto_partitioning_max_partitions_count", wantErr: `invalid auto_partitioning_max_partitions_count "-1": .*`},
		{name: "replicas in an unknown mode", values: map[string]string{"read_replicas_settings": "ALL_AZ:1"},
			wantAttribute: "read_replicas_settings", wantErr: `invalid read_replicas_settings "ALL_AZ:1": write PER_AZ:<n> .*`},
		{name: "the first of two in attribute order",
			values:        map[string]string{"auto_partitioning_min_partitions_count": "0", "auto_partitioning_by_load": "on"},
			wantAttribute: "auto_partitioning_by_load", wantErr: `invalid auto_partitioning_by_load "on": .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ParseDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declaration *ydbpartition.DeclarationError
			c.Assert(err, qt.ErrorAs, &declaration)
			c.Assert(declaration.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(got, qt.IsNil)
		})
	}
}

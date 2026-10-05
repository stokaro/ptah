package ydbpartition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpartition"
)

// TestParseTableDeclaration_HappyPath reads a table's settings out of its
// declaration and ignores every other key; a declaration with none of them
// has no settings.
func TestParseTableDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *ast.YDBTablePartitioningSpec
	}{
		{name: "no settings", values: map[string]string{"name": "t"}, want: nil},
		{
			name: "every attribute but a split point list",
			values: map[string]string{
				"name": "t", "auto_partitioning_by_size": "enabled", "auto_partitioning_partition_size_mb": "64",
				"auto_partitioning_by_load": " ENABLED ", "auto_partitioning_min_partitions_count": "2",
				"auto_partitioning_max_partitions_count": "8", "read_replicas_settings": "per_az:1",
				"key_bloom_filter": "Enabled", "uniform_partitions": "4",
			},
			want: &ast.YDBTablePartitioningSpec{
				BySize: new(true), PartitionSizeMB: 64, ByLoad: new(true), MinPartitions: 2, MaxPartitions: 8,
				ReadReplicas: "PER_AZ:1", KeyBloomFilter: new(true), UniformPartitions: 4,
			},
		},
		{
			name:   "split points",
			values: map[string]string{"partition_at_keys": "(10, 'a'), (20)"},
			want:   &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10", "a"}, {"20"}}},
		},
		{
			name:   "a filter switched off is a declaration",
			values: map[string]string{"key_bloom_filter": "disabled"},
			want:   &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ParseTableDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseTableDeclaration_FailurePath refuses each table attribute's value
// YDB would refuse, naming the attribute.
func TestParseTableDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		values        map[string]string
		wantAttribute string
		wantErr       string
	}{
		{name: "a filter that is not a switch", values: map[string]string{"key_bloom_filter": "true"},
			wantAttribute: "key_bloom_filter", wantErr: `invalid key_bloom_filter "true": write ENABLED or DISABLED`},
		{name: "zero uniform partitions", values: map[string]string{"uniform_partitions": "0"},
			wantAttribute: "uniform_partitions", wantErr: `invalid uniform_partitions "0": write a whole number of at least 1; .*`},
		{name: "an empty split point list", values: map[string]string{"partition_at_keys": " "},
			wantAttribute: "partition_at_keys", wantErr: `invalid partition_at_keys " ": name at least one split point, .*`},
		{name: "a shared attribute", values: map[string]string{"auto_partitioning_min_partitions_count": "0"},
			wantAttribute: "auto_partitioning_min_partitions_count", wantErr: `invalid auto_partitioning_min_partitions_count "0": .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ParseTableDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declaration *ydbpartition.DeclarationError
			c.Assert(err, qt.ErrorAs, &declaration)
			c.Assert(declaration.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestResolveTable_HappyPath reads each declaration over what the table
// holds: each setting it names, and the held value of each it leaves out, so
// removing a declaration changes nothing. A starting layout sets the minimum
// the way YDB does: the declared one, else the partitions the layout creates.
func TestResolveTable_HappyPath(t *testing.T) {
	defaults := ydbpartition.DefaultTableSettings()
	held := ydbpartition.TableSettings{
		Settings: ydbpartition.Settings{
			BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
			ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
		},
		KeyBloomFilter: true,
	}
	withMinimum := func(settings ydbpartition.TableSettings, minimum uint64) ydbpartition.TableSettings {
		settings.MinPartitions = minimum
		return settings
	}
	tests := []struct {
		name string
		spec *ast.YDBTablePartitioningSpec
		held ydbpartition.TableSettings
		want ydbpartition.TableSettings
	}{
		{name: "nil keeps what the table holds", spec: nil, held: held, want: held},
		{
			name: "a filter declared off over one held on",
			spec: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(false)},
			held: held,
			want: ydbpartition.TableSettings{Settings: held.Settings},
		},
		{
			name: "a filter left out keeps the held one",
			spec: &ast.YDBTablePartitioningSpec{MinPartitions: 2},
			held: held,
			want: withMinimum(held, 2),
		},
		{
			name: "every setting",
			spec: &ast.YDBTablePartitioningSpec{
				BySize: new(true), PartitionSizeMB: 64, ByLoad: new(true), MinPartitions: 2, MaxPartitions: 8,
				ReadReplicas: "any_az:3", KeyBloomFilter: new(true),
			},
			held: defaults,
			want: ydbpartition.TableSettings{
				Settings: ydbpartition.Settings{
					BySize: true, PartitionSizeMB: 64, ByLoad: true, MinPartitions: 2, MaxPartitions: 8,
					ReadReplicas: ydbpartition.Replicas{Count: 3},
				},
				KeyBloomFilter: true,
			},
		},
		{name: "uniform partitions set the minimum", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, held: defaults,
			want: withMinimum(defaults, 4)},
		{
			name: "split points set the minimum",
			spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10"}, {"20"}, {"30"}}},
			held: defaults,
			want: withMinimum(defaults, 4),
		},
		{
			name: "a declared minimum wins over the layout",
			spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4, MinPartitions: 2},
			held: held,
			want: withMinimum(held, 2),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ResolveTable(test.spec, test.held)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestExplicitTable names every setting a table holds, so the declaration
// resolves to them over a table holding others.
func TestExplicitTable(t *testing.T) {
	c := qt.New(t)
	settings := ydbpartition.TableSettings{Settings: ydbpartition.Settings{ByLoad: true, MinPartitions: 3}}
	spec := settings.Explicit()
	c.Assert(spec, qt.DeepEquals, &ast.YDBTablePartitioningSpec{
		BySize: new(false), ByLoad: new(true), MinPartitions: 3, ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false),
	})
	other := ydbpartition.TableSettings{
		Settings: ydbpartition.Settings{BySize: true, PartitionSizeMB: 100, MinPartitions: 6,
			ReadReplicas: ydbpartition.Replicas{Count: 2}},
		KeyBloomFilter: true,
	}
	resolved, err := ydbpartition.ResolveTable(spec, other)
	c.Assert(err, qt.IsNil)
	c.Assert(resolved.Equal(settings), qt.IsTrue, qt.Commentf("%+v", resolved))
}

// TestResolveTable_FailurePath refuses a declaration YDB refuses whatever the
// table's columns are.
func TestResolveTable_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		spec    *ast.YDBTablePartitioningSpec
		wantErr string
	}{
		{
			name:    "both starting layouts",
			spec:    &ast.YDBTablePartitioningSpec{UniformPartitions: 3, PartitionAtKeys: [][]string{{"10"}}},
			wantErr: "uniform_partitions and partition_at_keys are both declared, which YDB refuses .*",
		},
		{
			name:    "a size without splitting by size",
			spec:    &ast.YDBTablePartitioningSpec{BySize: new(false), PartitionSizeMB: 100},
			wantErr: "auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*",
		},
		{
			name:    "replicas in an unknown mode",
			spec:    &ast.YDBTablePartitioningSpec{ReadReplicas: "ALL_AZ:1"},
			wantErr: `read replicas "ALL_AZ:1" are not one YDB takes: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ResolveTable(test.spec, ydbpartition.DefaultTableSettings())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbpartition.TableSettings{})
		})
	}
}

// TestTableSpec writes what differs from YDB's documented defaults, which is
// what the reader reports, and reading the report gives the settings back.
func TestTableSpec(t *testing.T) {
	tuned := ydbpartition.TableSettings{
		Settings: ydbpartition.Settings{
			BySize: true, PartitionSizeMB: 64, ByLoad: true, MinPartitions: 7, MaxPartitions: 9,
			ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 2},
		},
		KeyBloomFilter: true,
	}
	tests := []struct {
		name     string
		settings ydbpartition.TableSettings
		want     *ast.YDBTablePartitioningSpec
	}{
		{name: "the defaults are nil", settings: ydbpartition.DefaultTableSettings(), want: nil},
		{
			name:     "every setting off its default",
			settings: tuned,
			want: &ast.YDBTablePartitioningSpec{
				PartitionSizeMB: 64, ByLoad: new(true), MinPartitions: 7, MaxPartitions: 9, ReadReplicas: "PER_AZ:2",
				KeyBloomFilter: new(true),
			},
		},
		{
			name:     "not splitting by size",
			settings: ydbpartition.TableSettings{Settings: ydbpartition.Settings{MinPartitions: 1}},
			want:     &ast.YDBTablePartitioningSpec{BySize: new(false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ydbpartition.TableSpec(test.settings)
			c.Assert(spec, qt.DeepEquals, test.want)
			held, err := ydbpartition.HeldTable(spec)
			c.Assert(err, qt.IsNil)
			c.Assert(held.Equal(test.settings), qt.IsTrue)
		})
	}
}

// TestTableChangeRefusal refuses the change YDB cannot make on a table in
// place: a starting layout whose minimum the table does not hold. A layout
// whose minimum the table holds, and a layout beside a declared minimum, leave
// nothing to compare and are not refused, and neither is a maximum the
// declaration leaves out, which keeps the held one.
func TestTableChangeRefusal(t *testing.T) {
	defaults := ydbpartition.DefaultTableSettings()
	resolve := func(spec *ast.YDBTablePartitioningSpec) ydbpartition.TableSettings {
		return must.Must(ydbpartition.ResolveTable(spec, defaults))
	}
	capped := resolve(&ast.YDBTablePartitioningSpec{MaxPartitions: 9})
	uniform := &ast.YDBTablePartitioningSpec{UniformPartitions: 4}
	atKeys := &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10"}, {"20"}}}
	pinned := &ast.YDBTablePartitioningSpec{UniformPartitions: 4, MinPartitions: 2}

	tests := []struct {
		name     string
		desired  *ast.YDBTablePartitioningSpec
		resolved ydbpartition.TableSettings
		current  ydbpartition.TableSettings
		want     string
	}{
		{name: "nothing changes", desired: nil, resolved: defaults, current: defaults, want: ""},
		{name: "a maximum left out", desired: nil, resolved: capped, current: capped, want: ""},
		{
			name: "uniform partitions on a table created without them", desired: uniform, resolved: resolve(uniform), current: defaults,
			want: "it declares UNIFORM_PARTITIONS = 4, which YDB takes only when it creates a table (`UNIFORM_PARTITIONS alter " +
				"is not supported`) and which gives a new table a minimum of 4 partitions, where this table holds 1. " +
				"Declare auto_partitioning_min_partitions_count to change the minimum in place",
		},
		{
			name: "split points on a table created without them", desired: atKeys, resolved: resolve(atKeys), current: defaults,
			want: "it declares PARTITION_AT_KEYS with 2 split points, which YDB takes only when it creates a table " +
				"(`PARTITION_AT_KEYS alter is not supported`) and which gives a new table a minimum of 3 partitions, where " +
				"this table holds 1. Declare auto_partitioning_min_partitions_count to change the minimum in place",
		},
		{name: "a layout the table holds the minimum of", desired: uniform, resolved: resolve(uniform), current: resolve(uniform), want: ""},
		{name: "a layout beside a declared minimum", desired: pinned, resolved: resolve(pinned), current: defaults, want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpartition.TableChangeRefusal(test.desired, test.resolved, test.current), qt.Equals, test.want)
		})
	}
}

// TestTableClause names every setting of each group a change touches: the
// whole splitting group, because setting one of its settings resets others,
// and the read replicas and the key bloom filter alone, because setting them
// resets nothing.
func TestTableClause(t *testing.T) {
	tuned := ydbpartition.TableSettings{
		Settings: ydbpartition.Settings{
			BySize: true, PartitionSizeMB: 100, ByLoad: true, MinPartitions: 6, MaxPartitions: 20,
			ReadReplicas: ydbpartition.Replicas{PerAZ: true, Count: 1},
		},
		KeyBloomFilter: true,
	}
	withoutFilter := tuned
	withoutFilter.KeyBloomFilter = false
	withoutReplicas := tuned
	withoutReplicas.ReadReplicas = ydbpartition.Replicas{}
	loadOnly := tuned
	loadOnly.ByLoad = false

	tests := []struct {
		name             string
		desired, current ydbpartition.TableSettings
		want             []string
	}{
		{name: "equal settings write nothing", desired: tuned, current: tuned, want: nil},
		{name: "the filter alone", desired: withoutFilter, current: tuned, want: []string{"KEY_BLOOM_FILTER = DISABLED"}},
		{name: "replicas going away", desired: withoutReplicas, current: tuned, want: []string{`READ_REPLICAS_SETTINGS = "PER_AZ:0"`}},
		{name: "replicas coming", desired: tuned, current: withoutReplicas, want: []string{`READ_REPLICAS_SETTINGS = "PER_AZ:1"`}},
		{
			// The size and the minimum are the same on both sides and are named
			// anyway: setting AUTO_PARTITIONING_BY_LOAD alone would reset the
			// minimum to 1.
			name: "a splitting change names the whole group", desired: tuned, current: loadOnly,
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 100",
				"AUTO_PARTITIONING_BY_LOAD = ENABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20",
			},
		},
		{
			name: "every group", desired: tuned, current: ydbpartition.DefaultTableSettings(),
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 100",
				"AUTO_PARTITIONING_BY_LOAD = ENABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20",
				`READ_REPLICAS_SETTINGS = "PER_AZ:1"`,
				"KEY_BLOOM_FILTER = ENABLED",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpartition.TableClause(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

// TestCreateClause writes each setting a declaration names, as it names it,
// in a fixed order, and nothing for a declaration of none.
func TestCreateClause(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.YDBTablePartitioningSpec
		want []string
	}{
		{name: "none", spec: nil, want: nil},
		{
			name: "every setting",
			spec: &ast.YDBTablePartitioningSpec{
				KeyBloomFilter: new(false), ReadReplicas: "ANY_AZ:2", MaxPartitions: 9, MinPartitions: 3,
				ByLoad: new(true), PartitionSizeMB: 64, BySize: new(true), UniformPartitions: 4,
			},
			want: []string{
				"AUTO_PARTITIONING_BY_SIZE = ENABLED",
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 64",
				"AUTO_PARTITIONING_BY_LOAD = ENABLED",
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3",
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9",
				`READ_REPLICAS_SETTINGS = "ANY_AZ:2"`,
				"KEY_BLOOM_FILTER = DISABLED",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbpartition.CreateClause(test.spec), qt.DeepEquals, test.want)
		})
	}
}

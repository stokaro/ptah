package ydbpartition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbpartition"
)

// TestParseSplitPoints_HappyPath reads YQL's own list: bare values are split
// points on the first key column, parenthesized ones on the leading columns,
// and quoted strings keep what a bare value cannot hold.
func TestParseSplitPoints_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want [][]string
	}{
		{name: "values", text: "10, 20,30", want: [][]string{{"10"}, {"20"}, {"30"}}},
		{name: "tuples", text: " (10, 'a') , (20) ", want: [][]string{{"10", "a"}, {"20"}}},
		{name: "quotes of both kinds", text: `"it's", 'say "hi"'`, want: [][]string{{"it's"}, {`say "hi"`}}},
		{name: "an escaped quote", text: `'a\'b'`, want: [][]string{{"a'b"}}},
		{name: "a comma inside a string", text: `'a,b', c`, want: [][]string{{"a,b"}, {"c"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ParseSplitPoints(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			again, err := ydbpartition.ParseSplitPoints(ydbpartition.FormatSplitPoints(got))
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, test.want)
		})
	}
}

// TestParseSplitPoints_FailurePath refuses a list that is not the grammar,
// naming where it stopped.
func TestParseSplitPoints_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "nothing", text: "", wantErr: "name at least one split point, .*"},
		{name: "an empty tuple", text: "()", wantErr: "expected a value at offset 1"},
		{name: "an unclosed tuple", text: "(10, 20", wantErr: "expected a comma or a closing parenthesis at offset 7"},
		{name: "an unclosed string", text: "'abc", wantErr: "a string value has no closing '"},
		{name: "two values with no comma", text: "10 20", wantErr: "expected a comma after split point 1 at offset 3"},
		{name: "a trailing comma", text: "10,", wantErr: "a split point ends without a value"},
		{name: "a nested tuple", text: "((10))", wantErr: "expected a value at offset 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.ParseSplitPoints(test.text)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestFormatSplitPoints writes whole numbers bare and everything else quoted,
// and parenthesizes a split point of more than one value.
func TestFormatSplitPoints(t *testing.T) {
	c := qt.New(t)
	got := ydbpartition.FormatSplitPoints([][]string{{"10", "it's"}, {"20"}, {"-5"}})
	c.Assert(got, qt.Equals, `(10, 'it\'s'), 20, '-5'`)
}

// TestLayoutClause_HappyPath writes the starting layout in the key's own
// types: integers bare, strings as typed literals, every split point as a
// tuple.
func TestLayoutClause_HappyPath(t *testing.T) {
	caps := capability.YDB262()
	tests := []struct {
		name     string
		spec     *ast.YDBTablePartitioningSpec
		keyTypes []string
		want     string
	}{
		{name: "none", spec: nil, keyTypes: []string{"Uint64"}, want: ""},
		{name: "settings without a layout", spec: &ast.YDBTablePartitioningSpec{MinPartitions: 3}, keyTypes: []string{"Uint64"}, want: ""},
		{name: "uniform on Uint64", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, keyTypes: []string{"Uint64", "Utf8"},
			want: "UNIFORM_PARTITIONS = 4"},
		{name: "uniform on Uint32", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 2}, keyTypes: []string{"Uint32"},
			want: "UNIFORM_PARTITIONS = 2"},
		{
			name:     "split points on a composite key",
			spec:     &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10", "a'b"}, {"20"}}},
			keyTypes: []string{"Uint64", "Utf8"},
			want:     `PARTITION_AT_KEYS = ((10, 'a\'b'u), (20))`,
		},
		{
			name:     "the largest Uint64 and a String key",
			spec:     &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"18446744073709551615", "m"}}},
			keyTypes: []string{"Uint64", "String"},
			want:     `PARTITION_AT_KEYS = ((18446744073709551615, 'm'))`,
		},
		{
			name:     "a signed key and a Serial one",
			spec:     &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"007", "2147483647"}}},
			keyTypes: []string{"Int16", "Serial"},
			want:     `PARTITION_AT_KEYS = ((7, 2147483647))`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.LayoutClause(test.spec, test.keyTypes, caps)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestLayoutClause_FailurePath refuses a layout YDB refuses for the key it is
// declared on, with the server's own words where it has some.
func TestLayoutClause_FailurePath(t *testing.T) {
	caps := capability.YDB262()
	tests := []struct {
		name     string
		spec     *ast.YDBTablePartitioningSpec
		keyTypes []string
		wantErr  string
	}{
		{name: "uniform on a text key", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, keyTypes: []string{"Utf8"},
			wantErr: "uniform_partitions splits the range of the first key column, .* this one is Utf8 .*"},
		{name: "uniform on a Serial key", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, keyTypes: []string{"Serial"},
			wantErr: ".* this one is Serial \\(`Unsupported first key column type Int32, .*"},
		{name: "uniform with no key", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, keyTypes: nil,
			wantErr: "uniform_partitions needs the table's key, and the table declares none"},
		{name: "more values than key columns", spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"1", "a", "3"}}},
			keyTypes: []string{"Uint64", "Utf8"},
			wantErr:  "split point 1 of partition_at_keys: it holds 3 values, and the key has 2 columns .*"},
		{name: "a value out of range", spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10"}, {"300"}}},
			keyTypes: []string{"Uint8"},
			wantErr:  `split point 2 of partition_at_keys: "300" is not a whole number from 0 to the largest Uint8, .*`},
		{name: "a negative value", spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"-10"}}},
			keyTypes: []string{"Int64"},
			wantErr:  `split point 1 of partition_at_keys: "-10" is not a whole number .*`},
		{name: "past a signed range", spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"128"}}},
			keyTypes: []string{"Int8"},
			wantErr:  `split point 1 of partition_at_keys: "128" is not a whole number from 0 to the largest Int8, .*`},
		{name: "a key type with no literal here", spec: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"2026-01-01"}}},
			keyTypes: []string{"Timestamp"},
			wantErr:  "split point 1 of partition_at_keys: its key column is Timestamp, and YDB takes only literal numbers and strings here, .*"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbpartition.LayoutClause(test.spec, test.keyTypes, caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

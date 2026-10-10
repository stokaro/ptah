package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
)

var (
	diffBucket = ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	diffEvents = ydbexternal.Table{DataSource: "s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
)

// TestExternalChanges_Effect classifies each change: a creation adds, and a
// drop or a replacement changes what queries reading the object read. No
// external object holds data in YDB.
func TestExternalChanges_Effect(t *testing.T) {
	observed, desired := &ydbexternal.ObservedSource{Spec: diffBucket}, &ydbexternal.DesiredSource{Spec: diffBucket}
	tests := []struct {
		name   string
		change interface{ Effect() schemaext.Effect }
		want   schemaext.Effect
	}{
		{name: "a created source", change: &ydbdiff.ExternalDataSource{After: desired},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE EXTERNAL DATA SOURCE adds a data source"}},
		{name: "a dropped source", change: &ydbdiff.ExternalDataSource{Before: observed},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.DropSourceReason}},
		{name: "a changed source", change: &ydbdiff.ExternalDataSource{Before: observed, After: desired},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.ReplaceReason}},
		{name: "a created table", change: &ydbdiff.ExternalTable{After: &ydbexternal.DesiredTable{Spec: diffEvents}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE EXTERNAL TABLE adds an external table"}},
		{name: "a dropped table", change: &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: diffEvents}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.DropTableReason}},
		{name: "a change without operands", change: &ydbdiff.ExternalTable{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Effect(), qt.Equals, test.want)
		})
	}
}

// TestExternalDataSource_Displaces says which source changes the tables over
// the source cannot stay through: a target without CREATE OR REPLACE drops
// every changed source and creates it again, and with it only a new source
// type displaces the tables.
func TestExternalDataSource_Displaces(t *testing.T) {
	retyped := diffBucket.Clone()
	retyped.SourceType = "Ydb"
	moved := diffBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	replace := capability.YDB262().With(capability.ExternalObjectReplace, true)
	tests := []struct {
		name      string
		change    *ydbdiff.ExternalDataSource
		caps      capability.Capabilities
		displaces bool
	}{
		{name: "moved, with replace", change: change(&diffBucket, &moved), caps: replace},
		{name: "moved, without", change: change(&diffBucket, &moved), caps: capability.YDB262(), displaces: true},
		{name: "retyped, with replace", change: change(&diffBucket, &retyped), caps: replace, displaces: true},
		{name: "removed", change: change(&diffBucket, nil), caps: capability.YDB262()},
		{name: "created", change: change(nil, &diffBucket), caps: capability.YDB262()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Displaces(test.caps), qt.Equals, test.displaces)
		})
	}
}

// change is a source change from before to after; a nil side is absence.
func change(before, after *ydbexternal.DataSource) *ydbdiff.ExternalDataSource {
	result := &ydbdiff.ExternalDataSource{}
	if before != nil {
		result.Before = &ydbexternal.ObservedSource{Spec: *before}
	}
	if after != nil {
		result.After = &ydbexternal.DesiredSource{Spec: *after}
	}
	return result
}

// TestExternalChangeCodecs_RoundTrip writes each change and reads it back as
// written, both operands of a table recreated over a recreated source
// included.
func TestExternalChangeCodecs_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		codec  schemaext.Codec
		change schemaext.ChangeValue
	}{
		{name: "a source", codec: ydbdiff.ExternalDataSourceCodec(), change: change(&diffBucket, &diffBucket)},
		{name: "a table", codec: ydbdiff.ExternalTableCodec(), change: &ydbdiff.ExternalTable{
			Before: &ydbexternal.ObservedTable{Spec: diffEvents}, After: &ydbexternal.DesiredTable{Spec: diffEvents, StructName: "Event"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := test.codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			decoded, err := test.codec.Decode(data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.change)
		})
	}
}

// TestExternalChangeCodecs_FailurePath refuses a change with no operand, a
// missing side, and an operand of the other representation.
func TestExternalChangeCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "no operand", codec: ydbdiff.ExternalDataSourceCodec(), input: `{"before":null,"after":null}`},
		{name: "a missing side", codec: ydbdiff.ExternalTableCodec(), input: `{"after":null}`},
		{name: "a holder on the observed side", codec: ydbdiff.ExternalDataSourceCodec(),
			input: `{"before":{"spec":{"source_type":"ObjectStorage","auth_method":"NONE"},"struct_name":"S"},"after":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

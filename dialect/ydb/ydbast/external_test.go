package ydbast_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbexternal"
)

func externalRegistry(c *qt.C) schemaext.Registry {
	c.Helper()
	registry, err := schemaext.NewRegistry(
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.ExternalDataSourceCodec()},
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.ExternalTableCodec()})
	c.Assert(err, qt.IsNil)
	return registry
}

// TestExternalCodecs_HappyPath round-trips each statement on an external
// object with its separate path parts; a dot stays in the part it is written
// in, and a drop carries an empty spec.
func TestExternalCodecs_HappyPath(t *testing.T) {
	source := ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE",
		Options: map[string]string{"AWS_REGION": "us-east-1"}}
	table := ydbexternal.Table{DataSource: "ext/s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}}}
	tests := []struct {
		name  string
		value schemaext.Payload
	}{
		{name: "a created source", value: &ydbast.ExternalDataSource{Operation: ydbast.ExternalCreate, Schema: "ext.v1", Name: "s3.v1", Spec: source}},
		{name: "a recreated source", value: &ydbast.ExternalDataSource{Operation: ydbast.ExternalRecreate, Schema: "ext", Name: "s3", Spec: source}},
		{name: "a dropped source", value: &ydbast.ExternalDataSource{Operation: ydbast.ExternalDrop, Name: "s3"}},
		{name: "a replaced table", value: &ydbast.ExternalTable{Operation: ydbast.ExternalReplace, Schema: "ext", Name: "events", Spec: table}},
		{name: "a dropped table", value: &ydbast.ExternalTable{Operation: ydbast.ExternalDrop, Schema: "ext", Name: "events"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := externalRegistry(c).Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := externalRegistry(c).Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.value})
		})
	}
}

// TestExternalCodecs_FailurePath refuses an external statement wire that is
// incomplete, holds a null, names no object, carries a spec on a drop, has an
// unknown operation, or recreates an external table.
func TestExternalCodecs_FailurePath(t *testing.T) {
	sourceWire := `{"operation":"create","schema":"ext","name":"s3","spec":{"source_type":"ObjectStorage","auth_method":"NONE"}}`
	tableWire := `{"operation":"drop","schema":"ext","name":"events","spec":{"data_source":"","location":"","columns":null}}`
	tests := []struct {
		name    string
		codec   schemaext.Codec
		data    string
		wantErr string
	}{
		{name: "a missing spec", codec: ydbast.ExternalDataSourceCodec(), data: strings.Replace(sourceWire, `,"spec":{"source_type":"ObjectStorage","auth_method":"NONE"}`, "", 1),
			wantErr: ".*external data source operation requires spec"},
		{name: "a null name", codec: ydbast.ExternalDataSourceCodec(), data: strings.Replace(sourceWire, `"name":"s3"`, `"name":null`, 1),
			wantErr: ".*external data source operation requires name"},
		{name: "a path in the name", codec: ydbast.ExternalDataSourceCodec(), data: strings.Replace(sourceWire, `"name":"s3"`, `"name":"ext/s3"`, 1),
			wantErr: ".*an external object requires a schema-scoped YDB identity"},
		{name: "an unknown operation", codec: ydbast.ExternalDataSourceCodec(), data: strings.Replace(sourceWire, `"create"`, `"alter"`, 1),
			wantErr: `.*unknown external object operation "alter"`},
		{name: "a spec on a drop", codec: ydbast.ExternalDataSourceCodec(), data: strings.Replace(sourceWire, `"create"`, `"drop"`, 1),
			wantErr: ".*a drop of .* carries no spec"},
		{name: "a recreated table", codec: ydbast.ExternalTableCodec(), data: strings.Replace(tableWire, `"drop"`, `"recreate"`, 1),
			wantErr: ".*an external table is dropped and created in two operations, not recreated in one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.data))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

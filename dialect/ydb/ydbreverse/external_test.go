package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreverse"
)

func externalReverseRequest(caps capability.Capabilities, changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: caps, Changes: changes}
}

// TestExternalReversal_HappyPath swaps each change's operands: a creation is
// dropped, a drop is created again as it was, and a replacement is replaced
// back, with nothing the rollback cannot restore. A table recreated over a
// recreated source keeps its equal operands, so the rollback recreates it
// too.
func TestExternalReversal_HappyPath(t *testing.T) {
	bucket := ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	moved := bucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	events := ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	source := ydbexternal.SourceRef("ext", "bucket")
	table := ydbexternal.TableRef("ext", "events")
	tests := []struct {
		name     string
		record   schemaext.ChangeRecord
		want     schemaext.ChangeValue
		forward  schemaext.ProjectedValue
		strategy string
	}{
		{name: "a created source", record: schemaext.ChangeRecord{Subject: source, Value: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: bucket}}},
			want:     &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: bucket}},
			forward:  schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbexternal.SourceKind, Value: &ydbexternal.ObservedSource{Spec: bucket}},
			strategy: "drop the created data source"},
		{name: "a dropped source", record: schemaext.ChangeRecord{Subject: source, Value: &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: bucket}}},
			want:     &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: bucket}},
			forward:  schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbexternal.SourceKind},
			strategy: "create the dropped data source again with the definition it had"},
		{name: "a replaced source", record: schemaext.ChangeRecord{Subject: source, Value: &ydbdiff.ExternalDataSource{
			Before: &ydbexternal.ObservedSource{Spec: bucket}, After: &ydbexternal.DesiredSource{Spec: moved}}},
			want:     &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: moved}, After: &ydbexternal.DesiredSource{Spec: bucket}},
			forward:  schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbexternal.SourceKind, Value: &ydbexternal.ObservedSource{Spec: moved}},
			strategy: "replace the data source with the definition it had"},
		{name: "a table recreated over a recreated source", record: schemaext.ChangeRecord{Subject: table, Value: &ydbdiff.ExternalTable{
			Before: &ydbexternal.ObservedTable{Spec: events}, After: &ydbexternal.DesiredTable{Spec: events}}},
			want:     &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: events}, After: &ydbexternal.DesiredTable{Spec: events}},
			forward:  schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbexternal.TableKind, Value: &ydbexternal.ObservedTable{Spec: events}},
			strategy: "replace the external table with the definition it had"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(capability.ExternalDataSources, true)

			result, err := (ydbreverse.ExternalService{}).ReverseChanges(t.Context(), externalReverseRequest(caps, test.record))

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: test.record.Subject, Value: test.want})
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{test.forward})
			c.Assert(result[0].Strategy, qt.Equals, test.strategy)
			c.Assert(result[0].Limitations, qt.HasLen, 0)
		})
	}
}

// TestExternalReversal_FailurePath refuses a target without external data
// sources, a change whose kind disagrees with its subject, and one with no
// operand.
func TestExternalReversal_FailurePath(t *testing.T) {
	bucket := ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}
	created := &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: bucket}}
	enabled := capability.YDB262().With(capability.ExternalDataSources, true)
	tests := []struct {
		name    string
		caps    capability.Capabilities
		record  schemaext.ChangeRecord
		wantIs  error
		wantErr string
	}{
		{name: "no key", caps: capability.YDB262(), record: schemaext.ChangeRecord{Subject: ydbexternal.SourceRef("", "s3"), Value: created},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: ".*reversing external objects requires external_data_sources"},
		{name: "a source change on a table", caps: enabled, record: schemaext.ChangeRecord{Subject: ydbexternal.TableRef("", "s3"), Value: created},
			wantIs: schemaext.ErrInvalidValue, wantErr: ".*a data source change names ptah.run/ydb/external-table"},
		{name: "no operand", caps: enabled, record: schemaext.ChangeRecord{Subject: ydbexternal.SourceRef("", "s3"), Value: &ydbdiff.ExternalDataSource{}},
			wantIs: schemaext.ErrInvalidValue, wantErr: ".*an external data source change requires a before or after operand"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.ExternalService{}).ReverseChanges(t.Context(), externalReverseRequest(test.caps, test.record))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.IsNil)
		})
	}
}

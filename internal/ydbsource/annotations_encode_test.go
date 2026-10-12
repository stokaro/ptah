package ydbsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/ydbsource"
)

var events = annotation.Table{Name: "events", Struct: "Event"}

func encodeYDB(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	return ydbsource.Annotations().Encoder.EncodeAnnotations(ctx, request)
}

// limitKinds are the kinds of limits.
func limitKinds(limits []annotation.EncodedLimit) []string {
	var kinds []string
	for _, limit := range limits {
		kinds = append(kinds, limit.Kind)
	}
	return kinds
}

// TestEncodeAnnotations_WritesLimitsWhereYDBReadsThem pins that the limits of
// a schema that describes no YDB namespace are written for a YDB export and
// for one that names no target, and left out of an export for another target,
// which never holds these objects.
func TestEncodeAnnotations_WritesLimitsWhereYDBReadsThem(t *testing.T) {
	every := []string{"coordination_node", "streaming_query", "resource_pool", "resource_pool_classifier", "secret", "topic",
		"external_data_source", "external_table", "replication", "transfer"}
	for _, test := range []struct {
		target string
		want   []string
	}{
		{"ydb", every},
		{"", every},
		{"postgres", nil},
		{"mysql", nil},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Target: test.target})

			c.Assert(err, qt.IsNil)
			c.Assert(limitKinds(result.Limits), qt.DeepEquals, test.want)
		})
	}
}

// TestEncodeAnnotations_WritesEachGroupWhereTheSourceDoes pins the order and
// the placement of the owner's objects, whatever their identities' order: the
// topics before the secrets after the views, then the resource pools before
// the coordination nodes after the default privileges.
func TestEncodeAnnotations_WritesEachGroupWhereTheSourceDoes(t *testing.T) {
	c := qt.New(t)
	objects := []schemaext.Object{
		ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{}),
		ydbworkload.DesiredPoolObject("analytics", "", ydbworkload.PoolSpec{}),
		ydbsecret.DesiredObject("", "token", "", "PTAH_SECRET_TOKEN"),
		ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "reader"}}}),
	}

	result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Objects: objects, Target: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.DeepEquals, []annotation.EncodedDirective{
		{Directive: "ptah:schema:topic", Placement: annotation.AfterViews, Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "events"}}},
		{Directive: "ptah:schema:topic:consumer", Placement: annotation.AfterViews,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "reader"}, {Name: "topic", Value: "events"}}},
		{Directive: "ptah:schema:secret", Placement: annotation.AfterViews,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "token"}, {Name: "value_env", Value: "PTAH_SECRET_TOKEN"}}},
		{Directive: "ptah:schema:resourcepool", Placement: annotation.AfterPrivileges,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "analytics"}}},
		{Directive: "ptah:schema:coordinationnode", Placement: annotation.AfterPrivileges,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "locks"}}},
	})
	c.Assert(result.Limits, qt.IsNil)
}

// TestEncodeAnnotations_WritesAChangefeedBesideItsTable pins that a
// changefeed and its consumers stand beside the declared table they belong
// to, after the table's column families.
func TestEncodeAnnotations_WritesAChangefeedBesideItsTable(t *testing.T) {
	c := qt.New(t)
	feed := ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON",
		Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", Important: true}}})
	families := must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "default", Data: "ssd"}, {Name: "archive", Data: "hdd", Compression: "lz4", Columns: []string{"payload"}}}}))

	result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{feed}, Tables: annotation.Tables{events},
		Slots: []annotation.SlotFacets{{Slot: annotation.Slot{Directive: "ptah:schema:table", Table: events}, Facets: families}}, Target: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.DeepEquals, []annotation.EncodedDirective{
		{Directive: "ptah:schema:columnfamily", Placement: annotation.AfterTable, Table: &events,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "archive"}, {Name: "data", Value: "hdd"},
				{Name: "compression", Value: "lz4"}, {Name: "fields", Value: "payload"}}},
		{Directive: "ptah:schema:columnfamily", Placement: annotation.AfterTable, Table: &events,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "default"}, {Name: "data", Value: "ssd"}}},
		{Directive: "ptah:schema:changefeed", Placement: annotation.AfterTable, Table: &events,
			Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "updates"}, {Name: "mode", Value: "UPDATES"}, {Name: "format", Value: "JSON"}}},
		{Directive: "ptah:schema:changefeed:consumer", Placement: annotation.AfterTable, Table: &events,
			Attributes: []annotation.EncodedAttribute{{Name: "changefeed", Value: "updates"}, {Name: "name", Value: "audit"}, {Name: "important", Value: "true"}}},
	})
}

// directiveNames are the directives' names, each with its placement.
func directiveNames(directives []annotation.EncodedDirective) []string {
	names := make([]string, 0, len(directives))
	for _, directive := range directives {
		names = append(names, directive.Directive+" "+string(directive.Placement))
	}
	return names
}

// TestEncodeAnnotations_WritesExternalTablesBeforeStreamingQueries pins the
// order of the last groups after the views: the external data sources, the
// external tables, then the streaming queries.
func TestEncodeAnnotations_WritesExternalTablesBeforeStreamingQueries(t *testing.T) {
	c := qt.New(t)
	objects := []schemaext.Object{
		ydbstreaming.DesiredObject("", "count", "", ydbstreaming.Spec{Text: "SELECT 1"}, false),
		ydbexternal.DesiredTableObject("", "logs", "", ydbexternal.Table{DataSource: "s3", Location: "/logs/",
			Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}, Options: map[string]string{"FORMAT": "csv_with_names"}}),
		ydbexternal.DesiredSourceObject("", "s3", "", ydbexternal.DataSource{SourceType: "ObjectStorage",
			Location: "https://storage.example.com/bucket", AuthMethod: "NONE"}),
	}

	result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Objects: objects})

	c.Assert(err, qt.IsNil)
	c.Assert(directiveNames(result.Directives), qt.DeepEquals, []string{
		"ptah:schema:externaldatasource after-views", "ptah:schema:externaltable after-views", "ptah:schema:streamingquery after-views",
	})
}

// TestEncodeAnnotations_RefusesAnInvalidDeclaration pins that a value the
// owner's model refuses is refused at export rather than written, since
// reading the source back would refuse it.
func TestEncodeAnnotations_RefusesAnInvalidDeclaration(t *testing.T) {
	limit := int32(4)
	spec := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		wantErr string
	}{
		{name: "a topic without a name", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbtopic.DesiredObject("", "", "", ydbtopic.Spec{})}}, wantErr: `invalid feature value: a topic requires a schema-scoped YDB identity`},
		{name: "a topic naming a consumer twice", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "a"}}})}},
			wantErr: `two of its consumers are named "a".*`},
		{name: "a secret read from another variable", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbsecret.DesiredObject("", "token", "", "TOKEN")}}, wantErr: `.*does not start with PTAH_SECRET_.*`},
		{name: "a default pool that limits queries", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ConcurrentQueryLimit: &limit})}},
			wantErr: `.*the default pool cannot limit concurrent queries.*`},
		{name: "a streaming query without text", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbstreaming.DesiredObject("", "count", "", ydbstreaming.Spec{}, false)}}, wantErr: `.*streaming query text is required`},
		{name: "a coordination node with an unknown mode", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{ReadConsistencyMode: "eventual"})}},
			wantErr: `.*the mode is "strict" or "relaxed".*`},
		{name: "a changefeed without a mode", request: annotation.EncodeRequest{Tables: annotation.Tables{events}, Objects: []schemaext.Object{
			{Ref: ydbschema.ChangefeedRef("", "events", "updates"), Value: &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Format: "JSON"}}}}},
			wantErr: `.*a changefeed requires mode and format`},
		{name: "a changefeed that keeps a replication's binding", request: annotation.EncodeRequest{Tables: annotation.Tables{events},
			Objects: []schemaext.Object{{Ref: ydbschema.ChangefeedRef("", "events", "updates"),
				Value: &ydbschema.DesiredChangefeed{Spec: spec, RetainedReplication: &ydbschema.ReplicationBinding{DestinationPath: "copy"}}}}},
			wantErr: `.*Go annotations cannot preserve the retained replication binding of changefeed .*`},
		{name: "a changefeed named apart from its reference", request: annotation.EncodeRequest{Tables: annotation.Tables{events},
			Objects: []schemaext.Object{{Ref: ydbschema.ChangefeedRef("", "events", "other"), Value: &ydbschema.DesiredChangefeed{Spec: spec}}}},
			wantErr: `.*changefeed name disagrees with its reference`},
		{name: "column families naming one family twice", request: annotation.EncodeRequest{Tables: annotation.Tables{events},
			Slots: []annotation.SlotFacets{{Slot: annotation.Slot{Directive: "ptah:schema:table", Table: events},
				Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
					{Name: "archive", Data: "hdd"}, {Name: "archive", Data: "ssd"}}}))}}},
			wantErr: `.*column family "archive" is listed twice`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), test.request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	families := must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{}))
	disabled := ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true})
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		wantIs  error
		wantErr string
	}{
		{name: "an observation", request: annotation.EncodeRequest{Objects: []schemaext.Object{ydbtopic.ObservedObject("", "events", ydbtopic.Spec{})}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature object .* with value \*ydbtopic.Observed`},
		{name: "column families of an index", request: annotation.EncodeRequest{Tables: annotation.Tables{events}, Slots: []annotation.SlotFacets{
			{Slot: annotation.Slot{Directive: "ptah:schema:index", Table: events, Name: "by_time"}, Facets: families}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/ydb/column-families"`},
		{name: "column families of a primary key", request: annotation.EncodeRequest{Tables: annotation.Tables{events}, Slots: []annotation.SlotFacets{
			{Slot: annotation.Slot{Directive: "ptah:schema:table", Table: events, PrimaryKey: true}, Facets: families}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/ydb/column-families"`},
		{name: "a changefeed of an undeclared table", request: annotation.EncodeRequest{Objects: []schemaext.Object{
			ydbschema.DesiredObject("", "orders", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"})},
			Tables: annotation.Tables{events}},
			wantIs: ptaherr.ErrInvalidSchemaDiff, wantErr: `.*has no declared parent table`},
		{name: "a disabled changefeed", request: annotation.EncodeRequest{Objects: []schemaext.Object{disabled}, Tables: annotation.Tables{events}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot preserve disabled changefeed .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

// TestEncodeAnnotations_RefusesACanceledRequest pins that a canceled context
// stops the owner before it writes an object.
func TestEncodeAnnotations_RefusesACanceledRequest(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()

	result, err := encodeYDB(ctx, annotation.EncodeRequest{Objects: []schemaext.Object{ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{})}})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}

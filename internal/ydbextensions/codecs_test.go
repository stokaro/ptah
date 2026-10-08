package ydbextensions_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/ydbextensions"
)

func TestCodecs_RoundTripAllChangefeedState(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	feed := ydbschema.ChangefeedSpec{
		Name: "updates", Mode: "NEW_IMAGE", Format: "JSON", VirtualTimestamps: true,
		ResolvedTimestamps: "PT5S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
		TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT2H", Disabled: true,
		Consumers: []ast.TopicConsumerSpec{{Name: "worker", Important: true, ReadFrom: "2026-01-01T00:00:00Z",
			SupportedCodecs: []string{"zstd", "raw"}, AvailabilityPeriod: "PT3H"}},
	}
	payloads := []schemaext.Payload{
		&ydbast.AddChangefeed{Changefeed: feed}, &ydbast.DropChangefeed{Name: feed.Name},
		&ydbast.AlterChangefeedTopic{Changefeed: feed, Previous: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}},
	}
	data, err := runtime.Codecs().Marshal(context.Background(), schemaext.Operation, payloads)
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().Unmarshal(context.Background(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, payloads)
	c.Assert(string(data), qt.Contains, `"owner":"ptah.run/ydb"`)
	decoded[0].(*ydbast.AddChangefeed).Changefeed.Consumers[0].SupportedCodecs[0] = "gzip"
	c.Assert(feed.Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"zstd", "raw"})
}

func TestCodecs_DefinitionsCoverConcreteFields(t *testing.T) {
	c := qt.New(t)
	codecs := ydbextensions.Codecs()
	type shape struct{ Properties map[string]json.RawMessage }
	var definition struct {
		Defs       map[string]shape                              `json:"$defs"`
		Changes    map[string]shape                              `json:"changes"`
		Operations map[string]shape                              `json:"operations"`
		Values     map[schemaext.Representation]map[string]shape `json:"values"`
	}
	c.Assert(json.Unmarshal(codecs[0].Definition, &definition), qt.IsNil)
	for name, model := range map[string]any{"changefeed": ydbschema.ChangefeedSpec{}, "consumer": ast.TopicConsumerSpec{}, "replication_binding": ydbschema.ReplicationBinding{}} {
		var fields []string
		modelType := reflect.TypeOf(model)
		for field := range modelType.Fields() {
			fields = append(fields, strings.Split(field.Tag.Get("json"), ",")[0])
		}
		slices.Sort(fields)
		var described []string
		for field := range definition.Defs[name].Properties {
			described = append(described, field)
		}
		slices.Sort(described)
		c.Assert(described, qt.DeepEquals, fields)
	}
	definition.Values[schemaext.Operation] = definition.Operations
	definition.Values[schemaext.Change] = definition.Changes
	c.Assert(codecs, qt.HasLen, 6)
	for _, codec := range codecs {
		var fields, described []string
		modelType := reflect.TypeOf(codec.Prototype).Elem()
		for field := range modelType.Fields() {
			fields = append(fields, strings.Split(field.Tag.Get("json"), ",")[0])
		}
		for field := range definition.Values[codec.Representation][string(codec.Prototype.Kind())].Properties {
			described = append(described, field)
		}
		slices.Sort(fields)
		slices.Sort(described)
		c.Assert(described, qt.DeepEquals, fields)
	}
}

func TestCodecs_RefuseMalformedAndLossyValues(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	for _, feed := range []ydbschema.ChangefeedSpec{
		{Name: "", Mode: "UPDATES", Format: "JSON"},
		{Name: "nested/name", Mode: "UPDATES", Format: "JSON"},
		{Name: "updates", Format: "JSON"},
		{Name: "updates", Mode: "UPDATES"},
		{Name: "\xff", Mode: "UPDATES", Format: "JSON"},
		{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "\xff"},
		{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "worker"}, {Name: "worker"}}},
		{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "worker", ReadFrom: "\xff"}}},
		{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "worker", SupportedCodecs: []string{"\xff"}}}},
	} {
		encoded, err := runtime.Codecs().Encode(context.Background(), schemaext.Operation, []schemaext.Payload{&ydbast.AddChangefeed{Changefeed: feed}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(encoded, qt.IsNil)
	}
}

func TestCodecs_CanonicalConsumerOrderRetainsDeclarations(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{
		{Name: "worker-b", SupportedCodecs: []string{"zstd", "raw"}}, {Name: "worker-a"},
	}}
	fingerprint, err := runtime.Codecs().Fingerprint(context.Background(), schemaext.Operation, []schemaext.Payload{&ydbast.AddChangefeed{Changefeed: feed}})
	c.Assert(err, qt.IsNil)
	reorderedFeed := feed.Clone()
	slices.Reverse(reorderedFeed.Consumers)
	slices.Reverse(reorderedFeed.Consumers[1].SupportedCodecs)
	reordered, err := runtime.Codecs().Fingerprint(context.Background(), schemaext.Operation, []schemaext.Payload{&ydbast.AddChangefeed{Changefeed: reorderedFeed}})
	c.Assert(err, qt.IsNil)
	c.Assert(reordered, qt.Equals, fingerprint)
	c.Assert(feed.Consumers[0].Name, qt.Equals, "worker-b")
	c.Assert(feed.Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"zstd", "raw"})
	reorderedFeed.RetentionPeriod = "P1D"
	explicitDefault, err := runtime.Codecs().Fingerprint(context.Background(), schemaext.Operation, []schemaext.Payload{&ydbast.AddChangefeed{Changefeed: reorderedFeed}})
	c.Assert(err, qt.IsNil)
	c.Assert(explicitDefault, qt.Not(qt.Equals), fingerprint)
}

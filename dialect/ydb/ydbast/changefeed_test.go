package ydbast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
)

func TestChangefeedPayloads_CloneRetainsIndependentConsumers(t *testing.T) {
	c := qt.New(t)
	feed := ydbschema.ChangefeedSpec{Name: "updates", Consumers: []ydbtopic.ConsumerSpec{
		{Name: "worker", SupportedCodecs: []string{"raw"}},
	}}
	added := &ydbast.AddChangefeed{Changefeed: feed}
	changed := &ydbast.AlterChangefeedTopic{Changefeed: feed, Previous: feed}
	addCopy := added.CloneExtension().(*ydbast.AddChangefeed)
	changeCopy := changed.CloneExtension().(*ydbast.AlterChangefeedTopic)
	addCopy.Changefeed.Consumers[0].Name = "other"
	addCopy.Changefeed.Consumers[0].SupportedCodecs[0] = "gzip"
	changeCopy.Changefeed.Consumers[0].SupportedCodecs[0] = "zstd"
	changeCopy.Previous.Consumers[0].SupportedCodecs[0] = "custom"
	c.Assert(added.Changefeed.Consumers, qt.DeepEquals, []ydbtopic.ConsumerSpec{{Name: "worker", SupportedCodecs: []string{"raw"}}})
	c.Assert(changed.Changefeed.Consumers, qt.DeepEquals, added.Changefeed.Consumers)
	c.Assert(changed.Previous.Consumers, qt.DeepEquals, added.Changefeed.Consumers)
	c.Assert(changeCopy.Changefeed.Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"zstd"})
}

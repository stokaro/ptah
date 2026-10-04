package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Topic_RoundTrip writes a YDB topic and its consumers as
// annotations that parse back to the same declaration, so `ptah introspect`
// of a YDB database keeps its topics.
func TestRender_Topic_RoundTrip(t *testing.T) {
	c := qt.New(t)
	topic := schemamodel.Topic{Name: "events", Schema: "app", Spec: ast.TopicSpec{
		MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
		AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "P1DT12H",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
		SupportedCodecs: []string{"raw", "gzip"},
		Consumers: []ast.TopicConsumerSpec{
			{Name: "billing", Important: true},
			{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"zstd"}, AvailabilityPeriod: "PT2H"},
		},
	}}
	db := &schemamodel.Database{Topics: []schemamodel.Topic{topic, {Name: "plain"}}}

	files, err := goschematogo.Render(db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Topics, qt.HasLen, 2)
	c.Assert(parsed.Topics[0].Spec, qt.DeepEquals, topic.Spec)
	c.Assert([]string{parsed.Topics[0].Name, parsed.Topics[0].Schema, parsed.Topics[1].Name, parsed.Topics[1].Schema},
		qt.DeepEquals, []string{"events", "app", "plain", ""})
}

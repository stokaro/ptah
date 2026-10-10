package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Topic_RoundTrip writes a YDB topic and its consumers as
// annotations that parse back to the same declaration, so `ptah introspect`
// of a YDB database keeps its topics.
func TestRender_Topic_RoundTrip(t *testing.T) {
	c := qt.New(t)
	spec := ydbtopic.Spec{
		MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
		AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "P1DT12H",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
		SupportedCodecs: []string{"raw", "gzip"},
		Consumers: []ydbtopic.ConsumerSpec{
			{Name: "billing", Important: true},
			{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"zstd"}, AvailabilityPeriod: "PT2H"},
		},
	}
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbtopic.DesiredObject("app", "events", "", spec), ydbtopic.DesiredObject("", "plain.v1", "", ydbtopic.Spec{})))}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	objects, err := parsed.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbtopic.DesiredObject("", "plain.v1", "PtahSchemaObjects", ydbtopic.Spec{}),
		ydbtopic.DesiredObject("app", "events", "PtahSchemaObjects", spec),
	})
}

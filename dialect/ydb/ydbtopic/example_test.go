package ydbtopic_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ExampleDeclare declares the topic app/events with a retention period and
// one important consumer, and plans it against a database that holds no
// topic. The source describes every topic, so the read's empty answer is the
// absence the comparison needs before it plans CREATE TOPIC.
func ExampleDeclare() {
	ctx := context.Background()
	runtime := must.Must(builtin.New())
	caps := capability.YDB262()
	declared, err := ydbtopic.Declare(schemaext.Objects{}, "app", "events", "", ydbtopic.Spec{
		RetentionPeriod: "PT2H", Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	desired := &schemamodel.Database{FeatureObjects: declared,
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	current := &catalog.Database{
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}

	diff, err := schemadiff.CompareWithDatabaseInfo(ctx, desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, diff, "ydb", planner.Options{Capabilities: caps})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range statements {
		fmt.Println(statement)
	}
	// Output:
	// CREATE TOPIC `app/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (retention_period = Interval('PT2H'))
}

// ExampleEqual compares a declaration that names YDB's defaults with a read
// that reports them, and one that leaves them out: each setting resolves to
// the value YDB gives a new topic, so all three are the same topic.
func ExampleEqual() {
	read := ydbtopic.Spec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "PT24H",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}
	named := ydbtopic.Spec{RetentionPeriod: "PT1440M", MinActivePartitions: 1}
	fmt.Println(ydbtopic.Equal(named, read))
	fmt.Println(ydbtopic.Equal(ydbtopic.Spec{}, read))
	fmt.Println(ydbtopic.Equal(ydbtopic.Spec{RetentionPeriod: "PT2H"}, read))
	// Output:
	// true
	// true
	// false
}

// ExampleParsePath reads a topic's path relative to the database root: a
// slash separates directories and a dot is part of a name. A path written
// from the server root is refused; ResolvePath reads one where the root is
// known.
func ExampleParsePath() {
	for _, written := range []string{"app/events.v1", "events", "/local/app/events"} {
		ref, err := ydbtopic.ParsePath(written)
		if err != nil {
			fmt.Println(written, "refused")
			continue
		}
		fmt.Printf("%s: directory %q, topic %q\n", written, ref.Schema.Source, ref.Name.Source)
	}
	ref, err := ydbtopic.ResolvePath("/local", "/local/app/events")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(ydbtopic.Display(ref.Schema.Source, ref.Name.Source))
	// Output:
	// app/events.v1: directory "app", topic "events.v1"
	// events: directory "", topic "events"
	// /local/app/events refused
	// app/events
}

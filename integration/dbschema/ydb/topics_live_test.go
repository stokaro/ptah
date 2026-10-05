//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
)

// topicSchema is the directory the topic tests write into.
const topicSchema = "ptah_ydb_topics"

var topicSchemas = []string{topicSchema}

// topicDeclaration declares the topics in the topic directory, with one table
// beside them so a read of the directory is not empty for the topics alone.
func topicDeclaration(topics ...schemamodel.Topic) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Note", Name: "notes", Schema: topicSchema}},
		Fields: []schemamodel.Field{{StructName: "Note", Name: "id", Type: "BIGINT", Primary: true}},
	}
	for _, topic := range topics {
		topic.Schema = topicSchema
		db.Topics = append(db.Topics, topic)
	}
	schemamodel.Finalize(db)
	return db
}

// dropTopics drops every topic and table in the topic directory.
func dropTopics(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, topicSchemas)
	c.Assert(err, qt.IsNil)
	for _, topic := range live.Topics {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TOPIC `"+topicSchema+"/"+topic.Name+"`"), qt.IsNil)
	}
	dropTables(c, conn, topicSchemas)
}

// topicNamed returns the topic name of the read, or fails.
func topicNamed(c *qt.C, live *catalog.Database, name string) ast.TopicSpec {
	c.Helper()
	for _, topic := range live.Topics {
		if topic.Schema == topicSchema && topic.Name == name {
			return topic.Spec
		}
	}
	c.Fatalf("topic %s/%s is not in the read %+v", topicSchema, name, live.Topics)
	return ast.TopicSpec{}
}

// fullTopic declares a topic naming every setting and two consumers, one
// with an availability period on a line that takes one.
func fullTopic(caps capability.Capabilities) schemamodel.Topic {
	audit := ast.TopicConsumerSpec{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}}
	if caps.Has(capability.TopicConsumerAvailabilityPeriod) {
		audit.AvailabilityPeriod = "PT2H"
	}
	return schemamodel.Topic{Name: "orders", Spec: ast.TopicSpec{
		MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
		AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "PT36H",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
		SupportedCodecs: []string{"raw", "gzip"},
		Consumers:       []ast.TopicConsumerSpec{{Name: "billing", Important: true}, audit},
	}}
}

// TestYDBTopics_RoundTrip creates a topic naming every setting and a topic
// naming none, reads both back, and plans nothing after; the same declaration
// applied again plans nothing too.
func TestYDBTopics_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTopics(c, conn)
			c.Cleanup(func() { dropTopics(c, conn) })
			full := fullTopic(line.preset())
			declared := topicDeclaration(full, schemamodel.Topic{Name: "plain"})

			first := planAgainst(c, conn, declared, topicSchemas)
			c.Assert(first[1:], qt.DeepEquals, []string{
				"CREATE TOPIC `ptah_ydb_topics/orders` (" + consumerClauses(full) + ") WITH (" +
					"min_active_partitions = 2, max_active_partitions = 6, auto_partitioning_strategy = 'scale_up', " +
					"auto_partitioning_up_utilization_percent = 70, auto_partitioning_down_utilization_percent = 10, " +
					"auto_partitioning_stabilization_window = Interval('PT2M'), retention_period = Interval('P1DT12H'), " +
					"partition_write_speed_bytes_per_second = 2097152, partition_write_burst_bytes = 3145728, " +
					"supported_codecs = 'raw,gzip')",
				"CREATE TOPIC `ptah_ydb_topics/plain`",
			})
			apply(c, conn, first)
			c.Assert(planAgainst(c, conn, declared, topicSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, topicSchemas))
			c.Assert(planAgainst(c, conn, declared, topicSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, topicSchemas)
			c.Assert(topicNamed(c, live, "orders"), qt.DeepEquals, ast.TopicSpec{
				MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
				AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "P1DT12H",
				PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
				SupportedCodecs: []string{"raw", "gzip"},
				Consumers:       full.Spec.Consumers,
			})
			c.Assert(topicNamed(c, live, "plain"), qt.DeepEquals, ast.TopicSpec{
				MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
				PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576,
			})
		})
	}
}

// consumerClauses is the consumer list CREATE TOPIC writes for the full topic.
func consumerClauses(topic schemamodel.Topic) string {
	audit := "CONSUMER `audit` WITH (read_from = Timestamp('2026-01-01T00:00:00Z'), supported_codecs = 'raw,gzip')"
	if topic.Spec.Consumers[1].AvailabilityPeriod != "" {
		audit = "CONSUMER `audit` WITH (read_from = Timestamp('2026-01-01T00:00:00Z'), supported_codecs = 'raw,gzip', " +
			"availability_period = Interval('PT2H'))"
	}
	return "CONSUMER `billing` WITH (important = TRUE), " + audit
}

// TestYDBTopics_ChangesConverge changes a topic's settings and consumers in
// one plan and asks for nothing left to plan after one apply, without reading
// the statements. The changes are the ones whose naive statement would leave
// the topic short of its declaration: a write speed changed alone, which
// leaves the burst where the topic was created with it; auto-partitioning
// enabled on a topic created without it, which takes thresholds of 80% and
// 20% rather than the 90% and 30% a topic created with it gets; a consumer
// whose codec list is emptied, which YDB changes only by dropping the consumer
// and adding it again; and consumers added, changed and dropped together.
func TestYDBTopics_ChangesConverge(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTopics(c, conn)
			c.Cleanup(func() { dropTopics(c, conn) })
			before := schemamodel.Topic{Name: "events", Spec: ast.TopicSpec{
				PartitionWriteSpeedBytesPerSecond: 2097152,
				Consumers: []ast.TopicConsumerSpec{
					{Name: "gone"},
					{Name: "kept", Important: true},
					{Name: "narrowed", SupportedCodecs: []string{"raw"}},
				},
			}}
			apply(c, conn, planAgainst(c, conn, topicDeclaration(before), topicSchemas))

			after := topicDeclaration(schemamodel.Topic{Name: "events", Spec: ast.TopicSpec{
				MinActivePartitions: 2, MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up",
				RetentionPeriod: "PT2H", PartitionWriteSpeedBytesPerSecond: 4194304,
				Consumers: []ast.TopicConsumerSpec{
					{Name: "kept", ReadFrom: "2026-01-01T00:00:00Z"},
					{Name: "narrowed"},
					{Name: "fresh", Important: true},
				},
			}})
			apply(c, conn, planAgainst(c, conn, after, topicSchemas))

			c.Assert(planAgainst(c, conn, after, topicSchemas), qt.HasLen, 0)
			live := topicNamed(c, readScoped(c, conn, topicSchemas), "events")
			c.Assert(live, qt.DeepEquals, ast.TopicSpec{
				MinActivePartitions: 2, MaxActivePartitions: 4, AutoPartitioningStrategy: "scale_up",
				AutoPartitioningUpUtilizationPercent: 90, AutoPartitioningDownUtilizationPercent: 30,
				AutoPartitioningStabilizationWindow: "PT5M", RetentionPeriod: "PT2H",
				PartitionWriteSpeedBytesPerSecond: 4194304, PartitionWriteBurstBytes: 4194304,
				Consumers: []ast.TopicConsumerSpec{
					{Name: "kept", ReadFrom: "2026-01-01T00:00:00Z"},
					{Name: "fresh", Important: true},
					{Name: "narrowed"},
				},
			})
		})
	}
}

// TestYDBTopics_DropRemovesTheTopic plans a DROP TOPIC for a topic the
// declaration no longer names, and the read finds it gone.
func TestYDBTopics_DropRemovesTheTopic(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTopics(c, conn)
			c.Cleanup(func() { dropTopics(c, conn) })
			apply(c, conn, planAgainst(c, conn, topicDeclaration(schemamodel.Topic{Name: "events"}), topicSchemas))

			drop := planAgainst(c, conn, topicDeclaration(), topicSchemas)
			apply(c, conn, drop)

			c.Assert(drop, qt.DeepEquals, []string{"DROP TOPIC `ptah_ydb_topics/events`"})
			c.Assert(readScoped(c, conn, topicSchemas).Topics, qt.HasLen, 0)
		})
	}
}

// TestYDBTopics_ATopicAndATableTradeAPath plans a topic into the path a
// dropped table held, and a table into the path a dropped topic held. YDB
// keeps one object at a path (`unexpected path type`), so a plan that
// created before it dropped would fail at the create.
func TestYDBTopics_ATopicAndATableTradeAPath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTopics(c, conn)
			c.Cleanup(func() { dropTopics(c, conn) })
			asTopic := topicDeclaration(schemamodel.Topic{Name: "swap"})
			asTable := topicDeclaration()
			asTable.Tables = append(asTable.Tables, schemamodel.Table{StructName: "Swap", Name: "swap", Schema: topicSchema})
			asTable.Fields = append(asTable.Fields, schemamodel.Field{StructName: "Swap", Name: "id", Type: "BIGINT", Primary: true})
			schemamodel.Finalize(asTable)
			apply(c, conn, planAgainst(c, conn, asTopic, topicSchemas))

			toTable := planAgainst(c, conn, asTable, topicSchemas)
			apply(c, conn, toTable)
			tableSettled := planAgainst(c, conn, asTable, topicSchemas)
			toTopic := planAgainst(c, conn, asTopic, topicSchemas)
			apply(c, conn, toTopic)

			c.Assert(toTable, qt.DeepEquals, []string{
				"DROP TOPIC `ptah_ydb_topics/swap`",
				"CREATE TABLE `ptah_ydb_topics/swap` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n)",
			})
			c.Assert(tableSettled, qt.HasLen, 0)
			c.Assert(toTopic, qt.DeepEquals, []string{
				"DROP TABLE `ptah_ydb_topics/swap`",
				"CREATE TOPIC `ptah_ydb_topics/swap`",
			})
			c.Assert(planAgainst(c, conn, asTopic, topicSchemas), qt.HasLen, 0)
		})
	}
}

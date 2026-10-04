package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

// withTopicKeys adds the question the YDB renderer, reader and planner decide
// a standalone topic with: whether the target has topics at all. Whether a
// topic consumer takes an availability period is asked once, with the
// changefeed keys.
//
// On YDB each is asked in YDB's spelling and then read back through Ptah's
// reader, since no SQL describes a topic: the topic service's DescribeTopic
// carries its settings and consumers. A topic is YDB's, so every other engine
// is asked the same statement and its refusal is the measurement, as with an
// index's partitioning.
func withTopicKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		p.experiments = append(p.experiments, ydbTopicExperiments()...)
		return p
	}
	if _, ok := typeKeySpellingFor(dialect); !ok {
		return p
	}
	p.experiments = append(p.experiments,
		proven(capability.Topics, schemaChange{
			change: []string{"CREATE TOPIC tpk (CONSUMER c WITH (important = TRUE)) WITH (retention_period = Interval('PT12H'))"},
		}),
	)
	return p
}

// ydbTopicExperiments creates a topic, changes it, and reads it back.
func ydbTopicExperiments() []experiment {
	return []experiment{
		proven(capability.Topics, schemaChange{
			change: []string{
				"CREATE TOPIC tpk (CONSUMER c WITH (important = TRUE)) WITH (retention_period = Interval('PT12H'))",
				"ALTER TOPIC tpk ADD CONSUMER d WITH (read_from = Timestamp('2026-01-01T00:00:00Z')), " +
					"SET (min_active_partitions = 2)",
			},
			after: []check{ydbDescribedTopic("tpk",
				"the topic with two partitions, keeping messages for 12 hours, read by important consumer c "+
					"and by consumer d from 2026-01-01",
				func(spec ast.TopicSpec) bool {
					return spec.MinActivePartitions == 2 && spec.RetentionPeriod == "PT12H" &&
						len(spec.Consumers) == 2 && spec.Consumers[0].Name == "c" && spec.Consumers[0].Important &&
						spec.Consumers[1].Name == "d" && spec.Consumers[1].ReadFrom == "2026-01-01T00:00:00Z"
				})},
		}),
	}
}

// ydbDescribedTopic reads the namespace through Ptah's YDB reader and holds
// when its topic name reads back as want says.
func ydbDescribedTopic(name, expectation string, want func(ast.TopicSpec) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read topic %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, name))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Topics {
				if found.Name == name {
					return attempt, want(found.Spec), fmt.Sprintf("read %+v", found.Spec)
				}
			}
			return attempt, false, "found no such topic"
		},
	}
}

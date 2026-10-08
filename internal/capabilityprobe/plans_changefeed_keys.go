package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// withChangefeedKeys adds the questions the YDB renderer, reader and planner
// decide a changefeed with: whether a table carries one at all, and whether
// the line takes each option that came in a later release.
//
// On YDB each is asked in YDB's spelling and then read back through Ptah's
// reader, since no SQL describes a changefeed: DescribeTable carries its
// options, and DescribeTopic on its path the retention and the consumers. A
// changefeed is YDB's, so every other engine is asked the same statement and
// its refusal is the measurement, as with an index's partitioning.
func withChangefeedKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == platform.YDB {
		p.experiments = append(p.experiments, ydbChangefeedExperiments()...)
		return p
	}
	spelling, ok := typeKeySpellingFor(normalized)
	if !ok {
		return p
	}
	refused := func(key capability.Capability, table, option string) experiment {
		return proven(key, schemaChange{
			setup:  []string{spelling.table(table, "n "+spelling.integer)},
			change: []string{changefeedStatement(table, option)},
		})
	}
	p.experiments = append(p.experiments,
		refused(capability.Changefeeds, "cfk", ""),
		refused(capability.ChangefeedUserSIDs, "cfu", ", USER_SIDS = TRUE"),
		refused(capability.ChangefeedSchemaChanges, "cfs", ", SCHEMA_CHANGES = TRUE"),
		refused(capability.ChangefeedTopicAutoPartitioning, "cfa", ", TOPIC_AUTO_PARTITIONING = 'ENABLED'"),
		proven(capability.TopicConsumerAvailabilityPeriod, schemaChange{
			setup: []string{spelling.table("cfc", "n "+spelling.integer)},
			change: []string{changefeedStatement("cfc", ""),
				"ALTER TOPIC `cfc/feed` ADD CONSUMER c WITH (availability_period = Interval('PT1H'))"},
		}),
	)
	return p
}

// changefeedStatement adds changefeed feed to table with the options YDB
// requires and the ones options adds.
func changefeedStatement(table, options string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'JSON'%s)", table, options)
}

// ydbChangefeedExperiments asks YDB to add a changefeed with each option a
// key names, and reads each back.
func ydbChangefeedExperiments() []experiment {
	t := ydbSpelling
	option := func(key capability.Capability, table, clause, expectation string, want func(ydbschema.ChangefeedSpec) bool) experiment {
		return proven(key, schemaChange{
			setup:  []string{t.table(table, "id Uint64 NOT NULL, n Int64", "id")},
			change: []string{changefeedStatement(table, clause)},
			after:  []check{ydbDescribedChangefeed(table, "feed", expectation, want)},
		})
	}
	return []experiment{
		proven(capability.Changefeeds, schemaChange{
			setup: []string{t.table("cfk", "id Uint64 NOT NULL, n Int64", "id")},
			change: []string{
				changefeedStatement("cfk", ", RETENTION_PERIOD = Interval('PT12H')"),
				"ALTER TOPIC `cfk/feed` ADD CONSUMER c WITH (important = TRUE)",
			},
			after: []check{ydbDescribedChangefeed("cfk", "feed",
				"the changefeed in UPDATES mode, its topic keeping records for 12 hours for important consumer c",
				func(spec ydbschema.ChangefeedSpec) bool {
					return spec.Mode == "UPDATES" && spec.RetentionPeriod == "PT12H" &&
						len(spec.Consumers) == 1 && spec.Consumers[0].Name == "c" && spec.Consumers[0].Important
				})},
		}),
		option(capability.ChangefeedUserSIDs, "cfu", ", USER_SIDS = TRUE", "the changefeed naming the user of each change",
			func(spec ydbschema.ChangefeedSpec) bool { return spec.UserSIDs }),
		option(capability.ChangefeedSchemaChanges, "cfs", ", SCHEMA_CHANGES = TRUE",
			"the changefeed writing a record for each schema change",
			func(spec ydbschema.ChangefeedSpec) bool { return spec.SchemaChanges }),
		option(capability.ChangefeedTopicAutoPartitioning, "cfa", ", TOPIC_AUTO_PARTITIONING = 'ENABLED'",
			"the changefeed's topic gaining partitions as writes grow",
			func(spec ydbschema.ChangefeedSpec) bool { return spec.TopicAutoPartitioning }),
		proven(capability.TopicConsumerAvailabilityPeriod, schemaChange{
			setup: []string{t.table("cfc", "id Uint64 NOT NULL, n Int64", "id"), changefeedStatement("cfc", "")},
			change: []string{
				"ALTER TOPIC `cfc/feed` ADD CONSUMER c WITH (availability_period = Interval('PT1H'))",
			},
			after: []check{ydbDescribedChangefeed("cfc", "feed", "consumer c keeping unread records for an hour",
				func(spec ydbschema.ChangefeedSpec) bool {
					return len(spec.Consumers) == 1 && spec.Consumers[0].Name == "c" &&
						spec.Consumers[0].AvailabilityPeriod == "PT1H"
				})},
		}),
	}
}

// ydbDescribedChangefeed reads table through Ptah's YDB reader and holds when
// its changefeed name reads back as want says.
func ydbDescribedChangefeed(table, name, expectation string, want func(ydbschema.ChangefeedSpec) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read changefeed %s of table %s through Ptah's YDB reader",
				name, path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Tables {
				if found.Name != table {
					continue
				}
				feeds, err := ydbschema.ObservedChangefeeds(db.FeatureObjects, found.Schema, found.Name)
				if err != nil {
					attempt.Accepted = false
					attempt.ServerErr = err.Error()
					return attempt, false, "the read returned invalid changefeed state"
				}
				for _, changefeed := range feeds {
					if changefeed.Name == name {
						return attempt, want(changefeed), fmt.Sprintf("read %+v", changefeed)
					}
				}
			}
			return attempt, false, "found no such changefeed"
		},
	}
}

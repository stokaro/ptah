package capabilityprobe

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withChangefeedKeys adds the questions whether a table carries a changefeed
// at all, and whether the line takes each option that came in a later
// release. A changefeed is YDB's, which the YDB provider measures on its own
// lines, so every engine here is asked the statement and its refusal is the
// measurement, as with an index's partitioning.
func withChangefeedKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(platform.NormalizeDialect(dialect))
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

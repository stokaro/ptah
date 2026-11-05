package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withTopicKeys adds the question whether the target has standalone topics.
// Whether a topic consumer takes an availability period is asked once, with
// the changefeed keys. A topic is YDB's, which the YDB provider measures on
// its own lines, so every engine here is asked the statement and its refusal
// is the measurement, as with an index's partitioning.
func withTopicKeys(p plan, dialect string) plan {
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

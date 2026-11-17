package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withPublicationKeys adds the questions a publication plan is refused on:
// whether a logical replication publication can publish every table of a
// schema, and whether it takes publish_generated_columns.
//
// Every dialect is asked, so a refusal is a measured answer: an engine with no
// publication answers a syntax error, and CockroachDB refuses CREATE
// PUBLICATION. The schema clause names public, which every PostgreSQL
// database holds, so the experiment needs no setup that another engine could
// fail and leave the key undecidable.
func withPublicationKeys(p plan) plan {
	p.experiments = append(p.experiments,
		acceptance(capability.PublicationSchemas, nil, "CREATE PUBLICATION pbs_pub FOR TABLES IN SCHEMA public"),
		acceptance(capability.PublicationGeneratedColumns, nil, "CREATE PUBLICATION pgc_pub WITH (publish_generated_columns = stored)"),
	)
	return p
}

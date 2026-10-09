// Package schemaexportloss recognizes schema properties that compact exports omit.
package schemaexportloss

import (
	"fmt"
	"sort"

	"ptah.run/core/schemamodel"
)

// familyCount is one object family DBML has no spelling for.
type familyCount struct {
	name  string
	count int
}

// CommonFamilies names object families absent from both DBML and compact
// schema-inspection JSON. Keep their recognition shared: a newly modeled
// family must not become a silent omission in one of those exports.
//
// Listed rather than counted in one number, because "3 objects were dropped"
// tells a reader nothing about whether the export is usable and "views (2),
// triggers (1)" tells them exactly.
func CommonFamilies(db *schemamodel.Database) []string {
	families := []familyCount{
		{"async replications", len(db.AsyncReplications)},
		{"composite types", len(db.CompositeTypes)},
		{"continuous aggregates", len(db.ContinuousAggregates)},
		{"domains", len(db.Domains)},
		{"external data sources", len(db.ExternalDataSources)},
		{"external tables", len(db.ExternalTables)},
		{"extended properties", len(db.ExtendedProperties)},
		{"extensions", len(db.Extensions)},
		{"functions", len(db.Functions)},
		{"grants", len(db.Grants)},
		{"hypertables", len(db.Hypertables)},
		{"managed data", len(db.ManagedData)},
		{"materialized views", len(db.MaterializedViews)},
		{"ranges", len(db.Ranges)},
		{"resource pools", len(db.ResourcePools)},
		{"resource pool classifiers", len(db.ResourcePoolClassifiers)},
		{"revoked grants", len(db.RevokedGrants)},
		{"roles", len(db.Roles)},
		{"row-level security policies", len(db.RLSPolicies)},
		{"secrets", len(db.Secrets)},
		{"sequences", len(db.Sequences)},
		{"synonyms", len(db.Synonyms)},
		{"topics", len(db.Topics)},
		{"transfers", len(db.Transfers)},
		{"triggers", len(db.Triggers)},
		{"views", len(db.Views)},
	}
	omitted := make([]string, 0, len(families))
	for _, family := range families {
		if family.count == 0 {
			continue
		}
		omitted = append(omitted, fmt.Sprintf("%s (%d)", family.name, family.count))
	}
	sort.Strings(omitted)
	return omitted
}

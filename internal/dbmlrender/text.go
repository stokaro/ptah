package dbmlrender

import (
	"fmt"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
)

// qualified renders an object's identity, schema-qualified when it has one.
func qualified(schema, name string) string {
	if schema == "" {
		return quote(name)
	}
	return quote(schema) + "." + quote(name)
}

// quote writes an identifier in double quotes, always.
//
// Unconditionally, because the alternative is a rule about which names are safe
// bare, and a name that is safe today stops being safe when the format grows a
// keyword. An embedded quote is doubled, which is what the format reads back.
func quote(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// quoteNote writes a note or a literal default in single quotes.
//
// A value with a newline takes the triple-quoted form, which is the only one
// that can hold one. A backslash and a single quote are escaped so the value
// reads back as itself.
func quoteNote(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	if strings.ContainsAny(value, "\n\r") {
		return "'''" + strings.ReplaceAll(escaped, "'''", `\'\'\'`) + "'''"
	}
	return "'" + strings.ReplaceAll(escaped, `'`, `\'`) + "'"
}

// omittedFamily is one object family DBML has no spelling for.
type omittedFamily struct {
	name  string
	count int
}

// omittedFamilies names every family that had members and no representation.
//
// Listed rather than counted in one number, because "3 objects were dropped"
// tells a reader nothing about whether the export is usable and "views (2),
// triggers (1)" tells them exactly.
func omittedFamilies(db *schemamodel.Database) []string {
	families := []omittedFamily{
		{"async replications", len(db.AsyncReplications)},
		{"composite types", len(db.CompositeTypes)},
		{"continuous aggregates", len(db.ContinuousAggregates)},
		{"coordination nodes", len(db.CoordinationNodes)},
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
		{"streaming queries", len(db.StreamingQueries)},
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

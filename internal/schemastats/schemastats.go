// Package schemastats counts the objects in a schema and renders the counts as
// OpenMetrics.
//
// It exists below the CLI because the counting means something without Atlas: a
// team watching its own schema grow wants the numbers whichever binary it runs.
// Both surfaces call [Collect] and [WriteOpenMetrics]; neither owns the
// semantics (stokaro/ptah#1711).
//
// # What is counted, and what a count means
//
// Every metric is a COUNT of objects Ptah's own reader returns, and nothing
// else. There is no row count, no table size, no index bloat: those are
// properties of the data, and reading them means scanning or trusting the
// planner's statistics, which is a different and much more expensive promise
// than "describe the schema". A caller who wants those has the database's own
// statistics views.
//
// The counts are therefore exactly as complete as the reader that produced the
// schema. On a dialect where Ptah reads no triggers, the trigger count is zero
// rather than absent, and that zero says "Ptah sees none here" rather than "the
// server has none" -- the same thing every other Ptah output means by an empty
// collection.
package schemastats

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// Metric is one counted object kind.
type Metric struct {
	// Name is the OpenMetrics metric name, without the family suffix.
	Name string
	// Help is the one-line description emitted as HELP.
	Help string
	// Value is the count.
	Value int
}

// Stats is the full set of counts for one schema.
type Stats struct {
	// Metrics are the counted object kinds, in a stable order.
	Metrics []Metric
}

// Collect counts every object kind in db.
//
// A nil database yields zero for every kind rather than an error: "no schema"
// and "an empty schema" are the same shape to a metrics pipeline, and a scrape
// that fails is worse than one reporting zeroes it can chart.
func Collect(db *goschema.Database) Stats {
	if db == nil {
		db = &goschema.Database{}
	}
	metrics := []Metric{
		{Name: "schemas", Help: "Schemas declared or read", Value: len(db.Schemas)},
		{Name: "tables", Help: "Tables", Value: len(db.Tables)},
		{Name: "columns", Help: "Columns across all tables", Value: len(db.Fields)},
		{Name: "indexes", Help: "Indexes", Value: len(db.Indexes)},
		{Name: "constraints", Help: "Table-level constraints", Value: len(db.Constraints)},
		{Name: "enums", Help: "Enum types", Value: len(db.Enums)},
		{Name: "extensions", Help: "Extensions", Value: len(db.Extensions)},
		{Name: "functions", Help: "Functions and procedures", Value: len(db.Functions)},
		{Name: "sequences", Help: "Standalone sequences", Value: len(db.Sequences)},
		{Name: "domains", Help: "Domain types", Value: len(db.Domains)},
		{Name: "composite_types", Help: "Composite types", Value: len(db.CompositeTypes)},
		{Name: "range_types", Help: "Range types", Value: len(db.Ranges)},
		{Name: "views", Help: "Views", Value: len(db.Views)},
		{Name: "materialized_views", Help: "Materialized views", Value: len(db.MaterializedViews)},
		{Name: "triggers", Help: "Triggers", Value: len(db.Triggers)},
		{Name: "rls_policies", Help: "Row-level security policies", Value: len(db.RLSPolicies)},
		{Name: "roles", Help: "Roles", Value: len(db.Roles)},
		{Name: "grants", Help: "Privilege grants", Value: len(db.Grants)},
	}
	return Stats{Metrics: metrics}
}

// metricPrefix namespaces every metric, so a pipeline scraping several tools
// can tell whose numbers these are.
const metricPrefix = "ptah_schema_"

// WriteOpenMetrics renders stats in the OpenMetrics text format.
//
// The format is small and strict, and three of its rules are easy to miss:
// every metric carries TYPE and HELP before its sample, a gauge is the right
// type for a count that can fall as well as rise, and the body ends with a
// literal `# EOF` line -- a reader that does not see it treats the scrape as
// truncated rather than empty.
//
// Labels carry the dialect and the schema the numbers came from, so a pipeline
// scraping several databases can tell them apart without a separate job per
// database.
func WriteOpenMetrics(w io.Writer, stats Stats, labels map[string]string) error {
	labelText := formatLabels(labels)
	for _, metric := range stats.Metrics {
		name := metricPrefix + metric.Name
		if _, err := fmt.Fprintf(w, "# HELP %s %s.\n", name, metric.Help); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s gauge\n", name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s%s %d\n", name, labelText, metric.Value); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(w, "# EOF\n")
	return err
}

// formatLabels renders a label set in a stable order, escaping the value.
//
// The order is sorted rather than map order: a scrape that reorders its labels
// between runs is a different series to some collectors, and map iteration in
// Go is deliberately unordered.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		if strings.TrimSpace(labels[name]) == "" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("{")
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[name]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

// escapeLabelValue escapes the three characters OpenMetrics reserves inside a
// label value. A database name carrying a quote would otherwise end the value
// early and produce a line no collector can parse.
func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}

// Package safety classifies schema changes by operational risk.
package safety

import (
	"sort"

	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// Severity is the operational risk level for a schema change.
type Severity string

const (
	// Safe changes should not remove data or tighten existing constraints.
	Safe Severity = "safe"
	// Warning changes are data-dependent or may affect runtime behavior.
	Warning Severity = "warning"
	// Destructive changes remove data, database objects, or protections.
	Destructive Severity = "destructive"
)

// Finding summarizes one non-empty schema-diff category.
type Finding struct {
	Category string   `json:"category"`
	Count    int      `json:"count"`
	Severity Severity `json:"severity"`
}

// ClassifySchemaDiff returns severity findings for every non-empty diff
// category.
func ClassifySchemaDiff(diff *types.SchemaDiff) []Finding {
	if diff == nil {
		return nil
	}

	var findings []Finding
	add(&findings, "tables_added", len(diff.TablesAdded), Safe)
	add(&findings, "tables_removed", len(diff.TablesRemoved), Destructive)
	add(&findings, "enums_added", len(diff.EnumsAdded), Safe)
	add(&findings, "enums_removed", len(diff.EnumsRemoved), Destructive)
	add(&findings, "indexes_added", len(diff.IndexesAdded), Warning)
	add(&findings, "indexes_removed", len(diff.IndexesRemoved), Warning)
	add(&findings, "extensions_added", len(diff.ExtensionsAdded), Safe)
	add(&findings, "extensions_removed", len(diff.ExtensionsRemoved), Destructive)
	add(&findings, "functions_added", len(diff.FunctionsAdded), Safe)
	add(&findings, "functions_removed", len(diff.FunctionsRemoved), Destructive)
	add(&findings, "functions_modified", len(diff.FunctionsModified), Warning)
	add(&findings, "rls_policies_added", len(diff.RLSPoliciesAdded), Safe)
	add(&findings, "rls_policies_removed", len(diff.RLSPoliciesRemoved), Warning)
	add(&findings, "rls_policies_modified", len(diff.RLSPoliciesModified), Warning)
	add(&findings, "rls_enabled_tables_added", len(diff.RLSEnabledTablesAdded), Safe)
	add(&findings, "rls_enabled_tables_removed", len(diff.RLSEnabledTablesRemoved), Destructive)
	add(&findings, "roles_added", len(diff.RolesAdded), Safe)
	add(&findings, "roles_removed", len(diff.RolesRemoved), Destructive)
	add(&findings, "roles_modified", len(diff.RolesModified), Warning)
	add(&findings, "constraints_added", len(diff.ConstraintsAdded), Warning)
	add(&findings, "constraints_removed", len(diff.ConstraintsRemoved), Destructive)

	for _, table := range diff.TablesModified {
		add(&findings, "columns_added", len(table.ColumnsAdded), Warning)
		add(&findings, "columns_removed", len(table.ColumnsRemoved), Destructive)
		add(&findings, "columns_modified", len(table.ColumnsModified), Warning)
		add(&findings, "table_constraints_added", len(table.ConstraintsAdded), Warning)
		add(&findings, "table_constraints_removed", len(table.ConstraintsRemoved), Destructive)
	}
	for _, enum := range diff.EnumsModified {
		add(&findings, "enum_values_added", len(enum.ValuesAdded), Warning)
		add(&findings, "enum_values_removed", len(enum.ValuesRemoved), Destructive)
	}

	findings = aggregate(findings)
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Severity != findings[j].Severity {
			return severityRank(findings[i].Severity) > severityRank(findings[j].Severity)
		}
		return findings[i].Category < findings[j].Category
	})
	return findings
}

// Highest returns the highest severity from findings.
func Highest(findings []Finding) Severity {
	highest := Safe
	for _, finding := range findings {
		if severityRank(finding.Severity) > severityRank(highest) {
			highest = finding.Severity
		}
	}
	return highest
}

// HasDestructive returns true when any finding is destructive.
func HasDestructive(findings []Finding) bool {
	return Highest(findings) == Destructive
}

func add(findings *[]Finding, category string, count int, severity Severity) {
	if count == 0 {
		return
	}
	*findings = append(*findings, Finding{
		Category: category,
		Count:    count,
		Severity: severity,
	})
}

func aggregate(findings []Finding) []Finding {
	byCategory := make(map[string]Finding, len(findings))
	for _, finding := range findings {
		existing, ok := byCategory[finding.Category]
		if !ok {
			byCategory[finding.Category] = finding
			continue
		}
		existing.Count += finding.Count
		if severityRank(finding.Severity) > severityRank(existing.Severity) {
			existing.Severity = finding.Severity
		}
		byCategory[finding.Category] = existing
	}

	out := make([]Finding, 0, len(byCategory))
	for _, finding := range byCategory {
		out = append(out, finding)
	}
	return out
}

func severityRank(severity Severity) int {
	switch severity {
	case Destructive:
		return 3
	case Warning:
		return 2
	default:
		return 1
	}
}

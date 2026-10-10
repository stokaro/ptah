package pgpolicysource

import (
	"cmp"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/feature/pgpolicy"
)

// The YAML frontend's keys whose PostgreSQL-scoped entries the owner reads.
// EnabledTablesKey routes rls_enabled_tables, the rls_enabled spelling of it,
// and a table's own rls_enabled.
const (
	PoliciesKey      = "rls_policies"
	EnabledTablesKey = "rls_enabled_tables"
)

// YAML is the row-security owner's contribution to the YAML frontend. It
// reads the row-level security entries that name no target, or only
// PostgreSQL-family targets, as policies and a table's switches, through the
// same readers as [Annotations], and makes the same claim.
func YAML() yamlext.Extension {
	scope := func(key string) yamlext.TargetScope {
		return yamlext.TargetScope{Key: key, Targets: Family(), Unscoped: true, Label: "PostgreSQL-family targets"}
	}
	return yamlext.Extension{
		Owner:        pgpolicy.Owner,
		Kinds:        []schemaext.Kind{pgpolicy.PolicyKind, pgpolicy.TableStateKind},
		TargetScopes: []yamlext.TargetScope{scope(PoliciesKey), scope(EnabledTablesKey)},
		Entries:      readEntries,
		Cover:        Claim,
		Coverage:     func() (schemaext.Coverage, error) { return pgpolicy.CompleteCoverage(schemaext.Desired) },
	}
}

// readEntries reads a document's row-level security entries in the order the
// frontend hands them, and refuses two that declare one policy, or one
// table's switches, naming both.
func readEntries(entries []yamlext.Entry, tables yamlext.Tables) ([]yamlext.Contribution, error) {
	var collector Collector
	contributions := make([]yamlext.Contribution, 0, len(entries))
	for _, entry := range entries {
		read := readPolicyEntry
		if entry.Key == EnabledTablesKey {
			read = readSwitchesEntry
		}
		contribution, err := read(&collector, tables, entry)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, contribution)
	}
	return contributions, nil
}

// readSwitchesEntry reads an enablement as the switches facet of the table it
// names, which the document must declare.
func readSwitchesEntry(collector *Collector, tables yamlext.Tables, entry yamlext.Entry) (yamlext.Contribution, error) {
	structName, tableName := entry.Attributes["struct_name"], entry.Attributes["table"]
	index, err := tables.Find(structName, tableName)
	if err != nil {
		return yamlext.Contribution{}, fmt.Errorf("%s: %w", entry.Origin, err)
	}
	if index < 0 {
		return yamlext.Contribution{}, fmt.Errorf("%s: %w: row-level security names table %q, which the document does not declare",
			entry.Origin, ptaherr.ErrInvalidAttributeValue, cmp.Or(tableName, structName))
	}
	table := tables[index]
	state := switchesOf(entry.Attributes, table.Struct)
	if _, err := collector.AddSwitches(entry.Origin, TableRef(table.Schema, table.Name), schemaext.Facets{}, state, entry.Targets); err != nil {
		return yamlext.Contribution{}, err
	}
	return yamlext.Contribution{Facet: &state, Table: index, Targets: entry.Targets, Label: "row-level security switches"}, nil
}

// readPolicyEntry reads a policy on the table it names: one the document
// declares, by name or by struct, or one it names as written.
func readPolicyEntry(collector *Collector, tables yamlext.Tables, entry yamlext.Entry) (yamlext.Contribution, error) {
	structName, written := entry.Attributes["struct_name"], entry.Attributes["table"]
	index, err := tables.Find(structName, written)
	if err != nil {
		return yamlext.Contribution{}, fmt.Errorf("%s: %w", entry.Origin, err)
	}
	var schemaName, tableName string
	switch {
	case index >= 0:
		schemaName, tableName = tables[index].Schema, tables[index].Name
	case written == "":
		return yamlext.Contribution{}, fmt.Errorf("%s: %w: a row-level security policy names its table", entry.Origin, ptaherr.ErrInvalidAttributeValue)
	default:
		if schemaName, tableName, err = TableParts(written); err != nil {
			return yamlext.Contribution{}, fmt.Errorf("%s: %w", entry.Origin, err)
		}
	}
	ref, policy, err := policyOf(entry.Attributes, schemaName, tableName, structName)
	if err != nil {
		return yamlext.Contribution{}, fmt.Errorf("%s: %w", entry.Origin, err)
	}
	if err := collector.AddPolicy(entry.Origin, ref, policy, entry.Targets); err != nil {
		return yamlext.Contribution{}, err
	}
	object, _, err := collector.Objects().Get(ref)
	if err != nil {
		return yamlext.Contribution{}, err
	}
	return yamlext.Contribution{Object: &object, Label: fmt.Sprintf("policy %q on table %s", ref.Name.Source, tableText(pgpolicy.Table(ref)))}, nil
}

package chsource

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
)

// RowPolicyDirective declares a ClickHouse row policy: a SELECT filter on one
// table for the users and roles it names.
const RowPolicyDirective = "ptah:schema:rowpolicy"

// RowPoliciesKey is the YAML document key the owner reads row policies from,
// one entry per policy keyed by its name.
const RowPoliciesKey = "row_policies"

// YAML is the owner's contribution to the YAML frontend: the row_policies
// key, which declares row policies as the //ptah:schema:rowpolicy directive
// does, and the claim that a document describes every row policy it holds,
// so one the document leaves out is absent.
func YAML() yamlext.Extension {
	return yamlext.Extension{
		Owner:    chschema.Owner,
		Kinds:    []schemaext.Kind{chschema.RowPolicyKind},
		Sections: []yamlext.Section{{Key: RowPoliciesKey, Decode: decodeYAMLRowPolicies}},
		Coverage: RowPolicyCoverage,
	}
}

// yamlRowPolicy is one entry of the row_policies key. It takes the
// directive's attributes, and a struct_name that names the table by the
// struct it maps to, as the document's other table references do.
type yamlRowPolicy struct {
	Name       string `yaml:"name"`
	Table      string `yaml:"table"`
	StructName string `yaml:"struct_name"`
	Using      string `yaml:"using"`
	To         string `yaml:"to"`
	As         string `yaml:"as"`
}

// decodeYAMLRowPolicies reads the row_policies key. An entry names its table,
// by name or by struct, and the table the document declares under that name
// gives the policy its database. A table the document leaves to the database
// is named as written. Two entries that declare one policy are refused,
// naming both.
func decodeYAMLRowPolicies(decode func(target any) error, tables yamlext.Tables) ([]yamlext.Contribution, error) {
	var entries map[string]yamlRowPolicy
	if err := decode(&entries); err != nil {
		return nil, err
	}
	var collector chpolicysource.Collector
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		if err := addYAMLRowPolicy(&collector, tables, key, entries[key]); err != nil {
			return nil, fmt.Errorf("%s.%s: %w", RowPoliciesKey, key, err)
		}
	}
	objects, err := collector.Objects().All()
	if err != nil {
		return nil, err
	}
	contributions := make([]yamlext.Contribution, 0, len(objects))
	for _, object := range objects {
		contributions = append(contributions, yamlext.Contribution{Object: &object,
			Label: fmt.Sprintf("row policy %q on table %q", object.Ref.Name.Source, object.Ref.Parent.Source)})
	}
	return contributions, nil
}

func addYAMLRowPolicy(collector *chpolicysource.Collector, tables yamlext.Tables, key string, entry yamlRowPolicy) error {
	index, err := tables.Find(entry.StructName, entry.Table)
	if err != nil {
		return err
	}
	var database, table string
	switch {
	case index >= 0:
		database, table = tables[index].Schema, tables[index].Name
	case strings.TrimSpace(entry.Table) == "":
		return fmt.Errorf("%w: a row policy names its table", ptaherr.ErrInvalidAttributeValue)
	default:
		database, table = annotation.QualifiedName("", entry.Table)
	}
	written, err := composition(entry.As)
	if err != nil {
		return err
	}
	policy, err := chpolicysource.Attributes{To: entry.To, Using: entry.Using, StructName: entry.StructName}.Policy()
	if err != nil {
		return err
	}
	policy.Composition = written
	ref := chpolicysource.Ref(database, table, cmp.Or(strings.TrimSpace(entry.Name), key))
	return collector.AddPolicy(RowPoliciesKey+"."+key, ref, policy, []string{platform.ClickHouse})
}

// RowPolicyCoverage is what a desired source that can declare row policies
// claims about them: it describes every one, so a comparison drops a policy
// the source leaves out.
func RowPolicyCoverage() (schemaext.Coverage, error) {
	return chpolicysource.Coverage()
}

func decodeRowPolicy(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	written, err := composition(kv["as"])
	if err != nil {
		return nil, err
	}
	policy, err := chpolicysource.Attributes{To: kv["to"], Using: kv["using"], StructName: declaration.Struct}.Policy()
	if err != nil {
		return nil, err
	}
	policy.Composition = written
	database, table := annotation.QualifiedName("", kv["table"])
	ref := chpolicysource.Ref(database, table, kv["name"])
	object, err := chschema.DesiredRowPolicyObject(ref, policy)
	if err != nil {
		return nil, err
	}
	object.Targets = []string{platform.ClickHouse}
	return []annotation.Contribution{{Object: &object,
		Label: fmt.Sprintf("row policy %q on table %q", ref.Name.Source, kv["table"])}}, nil
}

// composition reads the `as` attribute, PERMISSIVE or RESTRICTIVE in any
// letter case, as the composition it names. Left out, it is ClickHouse's
// default, permissive, and the declaration states none.
func composition(written string) (chschema.Composition, error) {
	switch strings.ToUpper(strings.TrimSpace(written)) {
	case "":
		return "", nil
	case "PERMISSIVE":
		return chschema.Permissive, nil
	case "RESTRICTIVE":
		return chschema.Restrictive, nil
	default:
		return "", fmt.Errorf("%w: a row policy is PERMISSIVE or RESTRICTIVE, not %q", ptaherr.ErrInvalidAttributeValue, written)
	}
}

func rowPolicyDirective() annotation.Directive {
	return annotation.Directive{
		Name: RowPolicyDirective,
		Description: "Declares a ClickHouse row policy: a filter on the rows of one table that the " +
			"users and roles it names may read.",
		Scopes: []annotation.Scope{annotation.ScopeStruct},
		Attributes: []annotation.Attribute{
			{Name: "name", Description: "Policy name, unique on its table.", Value: "string", Required: true},
			{Name: "table", Description: "Table the policy filters, optionally database-qualified.", Value: "string", Required: true},
			{Name: "using", Description: "Filter condition. Omit it to admit every row to the users the policy names.",
				Value: "string"},
			{Name: "to", Description: "Users and roles the policy applies to: names, ALL, ALL EXCEPT names, or NONE. " +
				"Omit it to apply the policy to nobody.", Value: "string"},
			{Name: "as", Description: "PERMISSIVE, combined with the other permissive policies by OR, or RESTRICTIVE, " +
				"combined with the rest by AND. Omit it for PERMISSIVE.", Value: "string"},
		},
	}
}

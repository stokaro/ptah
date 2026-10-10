package chsource

import (
	"fmt"
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

// YAML is the owner's claim on a YAML document: it describes every row policy
// it holds, as an rls_policies entry scoped to ClickHouse, so one the document
// leaves out is absent.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: chschema.Owner, Kinds: []schemaext.Kind{chschema.RowPolicyKind}, Coverage: RowPolicyCoverage}
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

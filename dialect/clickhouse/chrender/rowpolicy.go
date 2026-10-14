package chrender

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/sqlident"
)

func validateRowPolicy(ctx renderer.ExtensionContext, op *chast.RowPolicy) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse row policy operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	if !ctx.Capabilities.Has(capability.RowLevelSecurity) {
		return fmt.Errorf("%w: the target does not host ClickHouse row policies", ptaherr.ErrUnsupportedFeature)
	}
	return op.Validate()
}

// renderRowPolicy writes one statement for the transition.
//
// Measured on 24.10 and 26.9: CREATE OR REPLACE ROW POLICY is a syntax
// error, and CREATE ROW POLICY IF NOT EXISTS against a policy that exists
// succeeds and changes nothing, so a creation is a plain CREATE that fails if
// the policy is already there. ALTER ROW POLICY sets every part a change can
// touch in place: USING NONE removes a filter and TO NONE applies the policy
// to nobody. A drop is a plain DROP ROW POLICY, which fails if the policy is
// gone, rather than one that reports success for a policy it did not find.
//
// Every part is stated, so the statement means the same thing whatever the
// server defaults to: the composition is always written, and so is the role
// clause of an ALTER. The filter is the declaration's spelling; the server's
// own spelling is a fact for comparing, not for writing.
func renderRowPolicy(ctx renderer.ExtensionContext, op *chast.RowPolicy) ([]string, error) {
	command := &chast.RowPolicyCommand{Action: "DROP", Database: op.Database, Table: op.Table, Name: op.Name}
	if op.Change.After != nil {
		command.Values = op.Change.After.Copy()
		if command.Values.Composition == "" {
			command.Values.Composition = chschema.Permissive
		}
		command.Action = "ALTER"
		command.FilterSet, command.RolesSet = true, true
		if op.Change.Before == nil {
			command.Action = "CREATE"
			command.FilterSet = command.Values.Filter != nil
			command.RolesSet = !command.Values.Roles.IsZero()
		}
	}
	return renderRowPolicyCommand(ctx, command)
}

func validateRowPolicyCommand(ctx renderer.ExtensionContext, op *chast.RowPolicyCommand) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse row policy command on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	if !ctx.Capabilities.Has(capability.RowLevelSecurity) {
		return fmt.Errorf("%w: the target does not host ClickHouse row policies", ptaherr.ErrUnsupportedFeature)
	}
	return op.Validate()
}

func renderRowPolicyCommand(_ renderer.ExtensionContext, op *chast.RowPolicyCommand) ([]string, error) {
	statement := op.Action + " ROW POLICY " + quoteName(op.Name) + " ON " + quotePolicyTable(op.Database, op.Table)
	if op.Values == nil {
		return []string{statement + ";"}, nil
	}
	if op.FilterSet {
		using := "NONE"
		if op.Values.Filter != nil {
			using = "(" + *op.Values.Filter + ")"
		}
		statement += " USING " + using
	}
	if op.Values.Composition != "" {
		statement += " AS " + compositionKeyword(op.Values.Composition)
	}
	if op.RolesSet {
		statement += " TO " + chdiff.RoleClause(op.Values.Roles)
	}
	return []string{statement + ";"}, nil
}

// compositionKeyword states the composition, ClickHouse's default included.
func compositionKeyword(composition chschema.Composition) string {
	if composition == chschema.Restrictive {
		return "RESTRICTIVE"
	}
	return "PERMISSIVE"
}

func quoteName(name string) string { return sqlident.Quote(platform.ClickHouse, name) }

func quotePolicyTable(database, table string) string {
	if database == "" {
		return quoteName(table)
	}
	return quoteName(database) + "." + quoteName(table)
}

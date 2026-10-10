package goschematogo

import (
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/mssqlpolicysource"
)

// blockCommands is the FOR clause a Go annotation writes for each block
// operation it can say. BEFORE UPDATE has none: the annotation's UPDATE is
// AFTER UPDATE.
var blockCommands = map[mssqlschema.BlockOperation]string{
	"": "", mssqlschema.AfterInsert: "INSERT", mssqlschema.AfterUpdate: "UPDATE", mssqlschema.BeforeDelete: "DELETE",
}

// captureSecurityPolicy writes a SQL Server security policy as the row-level
// security annotations the Go source reads back into it (see
// [mssqlpolicysource.Attributes.Policy]): one per table and block operation,
// the first on a table carrying its filter predicate, each scoped to the
// policy's targets, or to SQL Server where the policy names none. A policy
// the annotations cannot say is refused: one turned off, not schema bound, or
// not for replication, one with a BEFORE UPDATE block predicate, and one
// binding a table the export does not declare.
func (ctx *renderContext) captureSecurityPolicy(object schemaext.Object, policy *mssqlschema.DesiredSecurityPolicy) error {
	if err := mssqlschema.ValidateSecurityPolicyRef(object.Ref); err != nil {
		return err
	}
	if err := mssqlschema.ValidateDesiredSecurityPolicy(policy); err != nil {
		return err
	}
	if enabled, schemaBinding, notForReplication := policy.Resolved(); !enabled || !schemaBinding || notForReplication {
		return fmt.Errorf("%w: Go annotations cannot declare security policy %s turned off, without schema binding, or not for replication",
			ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	targets := object.Targets
	if len(targets) == 0 {
		targets = []string{platform.SQLServer}
	}
	for _, table := range policyTables(policy.Predicates) {
		declared, found := ctx.securityPolicyTable(table)
		if !found {
			return fmt.Errorf("%w: security policy %s binds table %s, which the export does not declare",
				ptaherr.ErrUnsupportedFeature, object.Ref, table)
		}
		name := object.Ref.Name.Source
		if object.Ref.Schema.Source != table.Schema {
			name = object.Ref.Schema.Source + "." + name
		}
		var filter string
		for _, predicate := range policy.Predicates {
			if predicate.Table == table && predicate.Type == mssqlschema.Filter {
				filter = predicate.Invocation()
			}
		}
		annotations := 0
		for _, predicate := range mssqlschema.SortedPredicates(policy.Predicates) {
			if predicate.Table != table || predicate.Type != mssqlschema.Block {
				continue
			}
			command, sayable := blockCommands[predicate.Operation]
			if !sayable {
				return fmt.Errorf("%w: Go annotations cannot declare the %s block predicate of security policy %s",
					ptaherr.ErrUnsupportedFeature, predicate.Operation, object.Ref)
			}
			ctx.policyAnnotations[declared] = append(ctx.policyAnnotations[declared],
				securityPolicyAnnotation(name, declared, command, filter, predicate.Invocation(), targets))
			filter = ""
			annotations++
		}
		if annotations == 0 {
			ctx.policyAnnotations[declared] = append(ctx.policyAnnotations[declared],
				securityPolicyAnnotation(name, declared, "", filter, "", targets))
		}
	}
	return nil
}

// securityPolicyAnnotation is one row-level security annotation of a security
// policy.
func securityPolicyAnnotation(name, table, command, using, withCheck string, targets []string) string {
	return annotation("ptah:schema:rls:policy",
		attr{name: "name", value: name, set: true},
		attr{name: "table", value: table, set: true},
		attr{name: "for", value: command, set: command != ""},
		attr{name: "using", value: using, set: using != ""},
		attr{name: "with_check", value: withCheck, set: withCheck != ""},
		dialectsAttr(targets),
	)
}

// securityPolicyTable finds the declared table a predicate binds, the
// unqualified one standing for SQL Server's default schema, and returns the
// name its annotations are written beside.
func (ctx *renderContext) securityPolicyTable(table mssqlschema.ObjectName) (string, bool) {
	for _, declared := range ctx.db.Tables {
		schema := declared.Schema
		if schema == "" {
			schema = mssqlpolicysource.DefaultSchema
		}
		if declared.Name == table.Name && schema == table.Schema {
			return declared.QualifiedName(), true
		}
	}
	return "", false
}

// policyTables lists the tables predicates bind, in the order of their first
// predicate in canonical order.
func policyTables(predicates []mssqlschema.Predicate) []mssqlschema.ObjectName {
	var tables []mssqlschema.ObjectName
	for _, predicate := range mssqlschema.SortedPredicates(predicates) {
		if !slices.Contains(tables, predicate.Table) {
			tables = append(tables, predicate.Table)
		}
	}
	return tables
}

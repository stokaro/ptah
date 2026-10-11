package mssqlpolicysource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// blockCommands is the FOR clause a Go annotation writes for each block
// operation it can say. BEFORE UPDATE has none: the annotation's UPDATE is
// AFTER UPDATE.
var blockCommands = map[mssqlschema.BlockOperation]string{
	"": "", mssqlschema.AfterInsert: "INSERT", mssqlschema.AfterUpdate: "UPDATE", mssqlschema.BeforeDelete: "DELETE",
}

// annotationEncoder writes the security policies of one schema as Go
// annotations.
type annotationEncoder struct{}

// EncodeAnnotations writes each security policy as the row-level security
// annotations the Go source reads back into it (see [Attributes.Policy]): one
// per table and block operation, the first on a table carrying its filter
// predicate, each beside its table after the table's own policies, and each
// scoped to the policy's targets, or to SQL Server where the policy names
// none.
//
// A policy the annotations cannot say is refused: one turned off, not schema
// bound, or not for replication, one with a BEFORE UPDATE block predicate,
// one binding a table the schema does not declare, and one binding none.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var result annotation.EncodeResult
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directives, err := policyDirectives(object, request.Tables)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directives...)
	}
	return result, nil
}

// policyDirectives writes one security policy.
func policyDirectives(object schemaext.Object, declared annotation.Tables) ([]annotation.EncodedDirective, error) {
	policy, ok := object.Value.(*mssqlschema.DesiredSecurityPolicy)
	if !ok {
		return nil, fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T",
			ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
	if err := mssqlschema.ValidateSecurityPolicyRef(object.Ref); err != nil {
		return nil, err
	}
	if err := mssqlschema.ValidateDesiredSecurityPolicy(policy); err != nil {
		return nil, err
	}
	if enabled, schemaBinding, notForReplication := policy.Resolved(); !enabled || !schemaBinding || notForReplication {
		return nil, fmt.Errorf("%w: Go annotations cannot declare security policy %s turned off, without schema binding, or not for replication",
			ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	if len(policy.Predicates) == 0 {
		// Every annotation of a policy stands beside a table one of its
		// predicates binds, so one that binds none would be left out.
		return nil, fmt.Errorf("%w: Go annotations cannot declare security policy %s, which binds no table",
			ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	targets := object.Targets
	if len(targets) == 0 {
		targets = []string{platform.SQLServer}
	}
	var directives []annotation.EncodedDirective
	for _, table := range policyTables(policy.Predicates) {
		at, found := declaredTable(declared, table)
		if !found {
			return nil, fmt.Errorf("%w: security policy %s binds table %s, which the export does not declare",
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
		written := 0
		for _, predicate := range mssqlschema.SortedPredicates(policy.Predicates) {
			if predicate.Table != table || predicate.Type != mssqlschema.Block {
				continue
			}
			command, sayable := blockCommands[predicate.Operation]
			if !sayable {
				return nil, fmt.Errorf("%w: Go annotations cannot declare the %s block predicate of security policy %s",
					ptaherr.ErrUnsupportedFeature, predicate.Operation, object.Ref)
			}
			directives = append(directives, policyAnnotation(name, at, command, filter, predicate.Invocation(), targets))
			filter = ""
			written++
		}
		if written == 0 {
			directives = append(directives, policyAnnotation(name, at, "", filter, "", targets))
		}
	}
	return directives, nil
}

// policyAnnotation is one row-level security annotation of a security policy,
// beside table.
func policyAnnotation(name string, table annotation.Table, command, using, withCheck string, targets []string) annotation.EncodedDirective {
	attributes := []annotation.EncodedAttribute{{Name: "name", Value: name}, {Name: "table", Value: table.QualifiedName()}}
	for _, attribute := range []annotation.EncodedAttribute{{Name: "for", Value: command}, {Name: "using", Value: using}, {Name: "with_check", Value: withCheck}} {
		if attribute.Value != "" {
			attributes = append(attributes, attribute)
		}
	}
	return annotation.EncodedDirective{Directive: policyDirective, Placement: annotation.AfterPolicies, Table: &table,
		Attributes: attributes, Targets: slices.Clone(targets)}
}

// declaredTable finds the declared table a predicate binds, the unqualified
// one standing for SQL Server's default schema.
func declaredTable(declared annotation.Tables, table mssqlschema.ObjectName) (annotation.Table, bool) {
	for _, candidate := range declared {
		schema := candidate.Schema
		if schema == "" {
			schema = DefaultSchema
		}
		if candidate.Name == table.Name && schema == table.Schema {
			return candidate, true
		}
	}
	return annotation.Table{}, false
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

package pgpolicysource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// tableDirective is the frontend's directive that declares a table, the one
// declaration a table's switches belong to.
const tableDirective = "ptah:schema:table"

// annotationEncoder writes the row-level security policies and switches of
// one schema as Go annotations.
type annotationEncoder struct{}

// EncodeAnnotations writes each policy as [PolicyDirective] beside the table
// it names, after the table's own policies, and the switch of a table whose
// row-level security is on as [EnableDirective], after the table's own
// switches. Each is scoped to the targets of its declaration.
//
// It refuses what the annotations cannot say: a coverage record that limits
// what is known about a policy or a switch, a value that is not a valid
// declaration, a switch on anything but a table, one that forces row-level
// security without enabling it, and a policy whose table the schema does not
// declare.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	objects, err := schemaext.NewObjects(request.Objects...)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	if err := RequireRepresentable(request.Coverage, objects); err != nil {
		return annotation.EncodeResult{}, err
	}
	var result annotation.EncodeResult
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, written, err := switchDirective(slot)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		if written {
			result.Directives = append(result.Directives, directive)
		}
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres))
	tables := make(map[objectidentity.Key]annotation.Table, len(request.Tables))
	for _, table := range request.Tables {
		tables[builder.TableParts(table.Schema, table.Name).Key()] = table
	}
	for _, object := range request.Objects {
		directive, err := policyDirective(object, tables)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	return result, nil
}

// switchDirective writes the switch of the table slot declares, and reports
// whether its row-level security is on: a switch that is off is what a table
// with no switch declares.
func switchDirective(slot annotation.SlotFacets) (annotation.EncodedDirective, bool, error) {
	if slot.Slot.Directive != tableDirective || slot.Slot.PrimaryKey {
		return annotation.EncodedDirective{}, false, fmt.Errorf("%w: Go annotations cannot represent feature facet %q",
			ptaherr.ErrUnsupportedFeature, pgpolicy.TableStateKind)
	}
	state, _, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](slot.Facets, pgpolicy.TableStateKind)
	if err != nil {
		return annotation.EncodedDirective{}, false, err
	}
	if err := pgpolicy.ValidateDesiredTableState(state); err != nil {
		return annotation.EncodedDirective{}, false, err
	}
	table := slot.Slot.Table
	if !state.Enabled && state.Forced {
		return annotation.EncodedDirective{}, false, fmt.Errorf("%w: Go annotations cannot force row-level security on table %s without enabling it",
			ptaherr.ErrUnsupportedFeature, table.QualifiedName())
	}
	if !state.Enabled {
		return annotation.EncodedDirective{}, false, nil
	}
	attributes := []annotation.EncodedAttribute{{Name: "table", Value: table.QualifiedName()}}
	if state.Forced {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "force", Value: "true"})
	}
	if state.Comment != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "comment", Value: state.Comment})
	}
	return annotation.EncodedDirective{Directive: EnableDirective, Placement: annotation.AfterSwitches, Table: &table,
		Attributes: attributes, Targets: slot.Facets.TargetScope(pgpolicy.TableStateKind)}, true, nil
}

// policyDirective writes one policy beside the declared table it names,
// which tables holds by the identity a policy names its table by.
func policyDirective(object schemaext.Object, tables map[objectidentity.Key]annotation.Table) (annotation.EncodedDirective, error) {
	policy, ok := object.Value.(*pgpolicy.DesiredPolicy)
	if !ok {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T",
			ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
	if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
		return annotation.EncodedDirective{}, err
	}
	if err := pgpolicy.ValidateDesiredPolicy(policy); err != nil {
		return annotation.EncodedDirective{}, err
	}
	table, found := tables[pgpolicy.Table(object.Ref).Key()]
	if !found {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: policy %s names a table the export does not declare",
			ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	attributes := []annotation.EncodedAttribute{{Name: "name", Value: object.Ref.Name.Source}, {Name: "table", Value: table.QualifiedName()}}
	if policy.Command != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "for", Value: string(policy.Command)})
	}
	if policy.Roles != nil {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "to", Value: FormatRoleList(policy.Roles)})
	}
	if policy.Using != nil {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "using", Value: *policy.Using})
	}
	if policy.WithCheck != nil {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "with_check", Value: *policy.WithCheck})
	}
	if policy.Composition != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "as", Value: strings.ToUpper(string(policy.Composition))})
	}
	if policy.Comment != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "comment", Value: policy.Comment})
	}
	return annotation.EncodedDirective{Directive: PolicyDirective, Placement: annotation.AfterPolicies, Table: &table,
		Attributes: attributes, Targets: object.Targets}, nil
}

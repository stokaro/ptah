package chsource

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
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
)

// annotationEncoder writes the refresh schedules and the row policies of one
// schema as Go annotations.
type annotationEncoder struct{}

// EncodeAnnotations writes a materialized view's refresh schedule as the
// `refresh` attribute of its directive, and each row policy as a
// [RowPolicyDirective] beside the table it filters, after that table's own
// row-level security policies, with its composition, so a restrictive policy
// reads back restrictive (stokaro/ptah#4343).
//
// It refuses a value that is not a valid declaration, a schedule on anything
// but a materialized view, a policy on a table the schema does not declare,
// and a coverage record a source cannot carry: a schedule the read could not
// capture, which a Go source would state as no schedule (stokaro/ptah#4278),
// and row policies the read did not describe.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var result annotation.EncodeResult
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		attributes, err := refreshAttributes(slot)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Attributes = append(result.Attributes, attributes)
	}
	if err := requireCapturedSchedules(request.Coverage, "a Go annotation"); err != nil {
		return annotation.EncodeResult{}, err
	}
	if err := chpolicysource.RequireDescribed(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	tables := make(map[objectidentity.Key]annotation.Table, len(request.Tables))
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse))
	for _, table := range request.Tables {
		tables[builder.TablePartsVerbatim(table.Schema, table.Name).Key()] = table
	}
	for _, object := range request.Objects {
		directive, err := rowPolicyDirectiveOf(object, tables)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	return result, nil
}

// refreshAttributes writes the refresh schedule a materialized view holds.
func refreshAttributes(slot annotation.SlotFacets) (annotation.SlotAttributes, error) {
	if slot.Slot.Directive != MaterializedViewDirective {
		return annotation.SlotAttributes{}, fmt.Errorf("%w: Go annotations cannot represent feature facet %q",
			ptaherr.ErrUnsupportedFeature, chschema.RefreshKind)
	}
	schedule, _, err := schemaext.FacetAs[*chschema.DesiredRefresh](slot.Facets, chschema.RefreshKind)
	if err == nil {
		err = chschema.ValidateDesiredRefresh(schedule)
	}
	if err != nil {
		return annotation.SlotAttributes{}, fmt.Errorf("materialized view %q: %w", slot.Slot.Name, err)
	}
	// A valid schedule has a mode and an interval, so its clause is never
	// empty.
	return annotation.SlotAttributes{Slot: slot.Slot,
		Attributes: []annotation.EncodedAttribute{{Name: "refresh", Value: schedule.Clause()}}}, nil
}

// requireCapturedSchedules refuses a schedule the read could not capture,
// such as a stored clause a reader could not parse: each source states every
// view's schedule, so the view would be written as a plain one, and applying
// it would remove the schedule (stokaro/ptah#4278).
func requireCapturedSchedules(coverage schemaext.Coverage, format string) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == chschema.RefreshKind && (record.Knowledge.Unscoped().State == schemaext.Unrepresentable || record.Knowledge.Unscoped().State == schemaext.Uninspected) {
			return fmt.Errorf("%w: ClickHouse refresh schedules were not captured (%s), and %s states every schedule", ptaherr.ErrUnsupportedFeature, record.Knowledge.Unscoped().Reason, format)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind != chschema.RefreshKind || (record.Knowledge.State != schemaext.Unrepresentable && record.Knowledge.State != schemaext.Uninspected) {
			continue
		}
		return fmt.Errorf("%w: materialized view %s: its refresh schedule was not captured (%s), and %s states every schedule, "+
			"so the view would be written as a plain one", ptaherr.ErrUnsupportedFeature, qualifiedSubject(record.Subject), record.Knowledge.Reason, format)
	}
	return nil
}

// qualifiedSubject spells a subject as schema.name.
func qualifiedSubject(subject objectidentity.ID) string {
	if subject.Schema.Source == "" {
		return subject.Name.Source
	}
	return subject.Schema.Source + "." + subject.Name.Source
}

// rowPolicyDirectiveOf writes one row policy beside the declared table it
// filters, which tables holds by the identity a ClickHouse row policy names
// its table by.
func rowPolicyDirectiveOf(object schemaext.Object, tables map[objectidentity.Key]annotation.Table) (annotation.EncodedDirective, error) {
	policy, ok := object.Value.(*chschema.DesiredRowPolicy)
	if !ok {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T",
			ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
	if err := chschema.ValidateRowPolicyRef(object.Ref); err != nil {
		return annotation.EncodedDirective{}, err
	}
	if err := chschema.ValidateDesiredRowPolicy(policy); err != nil {
		return annotation.EncodedDirective{}, err
	}
	table, found := tables[chschema.RowPolicyTable(object.Ref).Key()]
	if !found {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: row policy %s names a table the export does not declare",
			ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	attributes := []annotation.EncodedAttribute{{Name: "name", Value: object.Ref.Name.Source}, {Name: "table", Value: table.QualifiedName()}}
	if policy.Filter != nil {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "using", Value: *policy.Filter})
	}
	if !policy.Roles.IsZero() {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "to", Value: chpolicysource.FormatRoles(policy.Roles)})
	}
	if policy.Composition != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "as", Value: strings.ToUpper(string(policy.Composition))})
	}
	return annotation.EncodedDirective{Directive: RowPolicyDirective, Placement: annotation.AfterPolicies, Table: &table,
		Attributes: attributes}, nil
}

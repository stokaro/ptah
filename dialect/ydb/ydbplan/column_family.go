package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// ColumnFamiliesService plans column family changes in place and accounts for
// the families through table removal and rebuild. Its zero value supports
// concurrent use without database access.
//
// One ALTER TABLE per table adds the families the declaration names and the
// table lacks, sets each setting the declaration states and the table holds
// otherwise, and moves each column whose family differs (see
// [ydbfamily.AlterActions]). The host places it after the table's added
// columns, so a column added into a family exists when it moves there. A
// column the plan drops is left out of both sides: it goes with its DROP
// COLUMN, and moving it first would only rewrite its data.
//
// A setting or a family the declaration leaves out is the table's to keep,
// since a cluster's table profile gives every new table its own (see package
// ydbfamily), so no family change needs a rebuild. A rebuild made for another
// reason writes the families the comparison made effective, which keep what
// the table holds.
type ColumnFamiliesService struct{}

var familyPlanning = tableFacetPlanning{
	name: "YDB column family", facet: ydbschema.ColumnFamiliesKind, change: ydbdiff.ColumnFamiliesKind,
	plan: planFamilyChange, assess: assessFamilyParent,
}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
func (ColumnFamiliesService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return familyPlanning.planFeatures(ctx, request)
}

func planFamilyChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var result plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: ydbdiff.ColumnFamiliesKind,
		Strategy: "nothing to change once the columns the plan drops leave their families"}
	change, ok := record.Value.(*ydbdiff.ColumnFamilies)
	if !ok {
		return result, plan, fmt.Errorf("%w: expected a YDB column family change", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateColumnFamilies(change); err != nil {
		return result, plan, err
	}
	table, err := surviving(request, record, "column family")
	if err != nil {
		return result, plan, err
	}
	if err := refuseFamilyChange(request.Capabilities, table, change); err != nil {
		return result, plan, err
	}
	dropped := droppedColumns(table)
	payload := &ydbast.AlterColumnFamilies{Change: ydbdiff.ColumnFamilies{
		After: &ydbschema.DesiredColumnFamilies{Families: ydbfamily.WithoutColumns(change.After.Families, dropped)},
	}}
	if change.Before != nil {
		payload.Change.Before = &ydbschema.ObservedColumnFamilies{Families: ydbfamily.WithoutColumns(change.Before.Families, dropped)}
	}
	if len(payload.Actions()) == 0 {
		return result, plan, nil
	}
	result.Owner = ydbschema.Owner
	families := table.Subject
	families.Kind = objectidentity.Kind(ydbschema.ColumnFamiliesKind)
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		// Zero-padded, because a scheduler orders independent steps by name and
		// the statements should keep the order of the changes.
		ID:          plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("column-families/%06d", index)},
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload},
		Transaction: plangraph.TransactionForbidden,
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: families, Action: plangraph.Alter}},
		Impact:      payload.Effect(),
	}}
	plan.Strategy = "add families, set their settings and move columns in one ALTER TABLE after column additions"
	plan.Steps = []plangraph.StepID{result.Steps[0].ID}
	return result, plan, nil
}

// refuseFamilyChange refuses, before anything is emitted, a change of a
// table's column families this target cannot make: an action the target has
// no key for, a declaration YDB refuses, read against the table's declared
// columns and key (see [ydbfamily.Refusal]), and a keep_in_memory no
// statement writes (see [ydbfamily.ChangeRefusal]).
func refuseFamilyChange(caps capability.Capabilities, table featureplan.Table, change *ydbdiff.ColumnFamilies) error {
	subject := "table " + quoted(familyDisplayName(table.Subject))
	var held []ydbschema.ColumnFamily
	if change.Before != nil {
		held = change.Before.Families
	}
	for _, requirement := range ydbfamily.ChangeRequirements(change.After.Families, held) {
		if !caps.Has(requirement.Key) {
			return ttlKey(requirement.Key, "changing the "+requirement.Settings+" of "+subject)
		}
	}
	if table.Desired.HasTable() {
		columns, key := declaredColumns(table.Desired)
		if reason := ydbfamily.Refusal(change.After.Families, columns, key); reason != "" {
			return ttlFact(subject, reason)
		}
	}
	if reason := ydbfamily.ChangeRefusal(change.After.Families, held); reason != "" {
		return ttlFact(subject, reason)
	}
	return nil
}

// droppedColumns are the columns the table holds and the declaration does
// not name, which the plan drops.
func droppedColumns(table featureplan.Table) []string {
	if !table.Desired.HasTable() {
		return nil
	}
	declared, _ := declaredColumns(table.Desired)
	var dropped []string
	for _, column := range table.Current.Table.Columns {
		if !slices.Contains(declared, column.Name) {
			dropped = append(dropped, column.Name)
		}
	}
	return dropped
}

// declaredColumns are the columns a declared table names and its key: the key
// the table names, or its key fields.
func declaredColumns(declaration schemacapture.TableDeclaration) (columns, key []string) {
	for _, field := range declaration.Fields {
		if field.StructName != declaration.Table.StructName {
			continue
		}
		columns = append(columns, field.Name)
		if field.Primary {
			key = append(key, field.Name)
		}
	}
	if len(declaration.Table.PrimaryKey) > 0 {
		key = declaration.Table.PrimaryKey
	}
	return columns, key
}

// assessFamilyParent accounts for the families through a table operation.
// Dropping a table removes its families with it, and a created table's CREATE
// TABLE writes them. A rebuilt table's CREATE TABLE writes [RebuiltFamilies],
// which are held to the target here so the plan refuses before any statement
// rather than when the rebuild is rendered. A table that survives
// keeps its families unless a change in the same plan changes them.
func assessFamilyParent(caps capability.Capabilities, table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove the column families with the table", nil
	case featureplan.CreateTable:
		return "write the declared column families into the CREATE TABLE", nil
	case featureplan.RebuildTable:
		families, err := RebuiltFamilies(table.Desired, table.Current)
		if err != nil {
			return "", err
		}
		columns, key := declaredColumns(table.Desired)
		if err := refuseCreatedFamilies(caps, "rebuilding table "+quoted(familyDisplayName(table.Subject)), families, columns, key); err != nil {
			return "", err
		}
		return "write the families the table holds once the declaration is applied into the rebuilt table", nil
	case featureplan.AlterTable:
		return "retain the column families unless a planned change in this plan changes them", nil
	default:
		return "", fmt.Errorf("%w: YDB column families have no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

// RebuiltFamilies are the families the CREATE TABLE of a rebuild writes for a
// table declared as declaration whose read is observation, so the new table
// keeps what the old one holds: what the table holds once the declaration is
// applied ([ydbfamily.Applied]), as an in-place change would leave it. Where
// the declaration's source could not describe families, an HCL or a DBML
// document, the families the table holds stay as they are, columns included,
// except a column the document leaves out: the plan drops that column, and
// the new table cannot name it. An invalid facet is an error.
func RebuiltFamilies(declaration schemacapture.TableDeclaration, observation schemacapture.TableObservation) ([]ydbschema.ColumnFamily, error) {
	declared, hasDeclared, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](declaration.Table.Facets, ydbschema.ColumnFamiliesKind)
	if err != nil {
		return nil, err
	}
	observed, hasObserved, err := schemaext.FacetAs[*ydbschema.ObservedColumnFamilies](observation.Table.Facets, ydbschema.ColumnFamiliesKind)
	if err != nil {
		return nil, err
	}
	var stated, held []ydbschema.ColumnFamily
	if hasDeclared {
		stated = declared.Families
	}
	if hasObserved {
		held = observed.Families
	}
	known := FamiliesKnown(declaration.FeatureCoverage)
	families := ydbschema.CloneColumnFamilies(held)
	if hasDeclared || known {
		families = ydbfamily.Applied(stated, held)
	}
	if known {
		return families, nil
	}
	columns, _ := declaredColumns(declaration)
	return ydbfamily.OnlyColumns(families, func(column string) bool { return slices.Contains(columns, column) }), nil
}

// FamiliesKnown reports whether coverage captured for one table, as a table
// declaration or observation carries it, knows the table's column families:
// the table's own claim when it has one, and the kind's otherwise.
func FamiliesKnown(coverage schemaext.Coverage) bool {
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == ydbschema.ColumnFamiliesKind && record.Subject.Kind == objectidentity.KindTable {
			return record.Knowledge.State == schemaext.Complete || record.Knowledge.State == schemaext.Absent
		}
	}
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == ydbschema.ColumnFamiliesKind {
			return record.Knowledge.State == schemaext.Complete || record.Knowledge.State == schemaext.Absent
		}
	}
	return false
}

// refuseCreatedFamilies refuses families a CREATE TABLE of subject cannot
// write: a family the target has no key for, a declaration YDB refuses, read
// against the table's columns and key, and a family that keeps its columns in
// memory (see [ydbfamily.CreateRefusal]).
func refuseCreatedFamilies(caps capability.Capabilities, subject string, families []ydbschema.ColumnFamily, columns, key []string) error {
	for _, requirement := range ydbfamily.Requirements(families) {
		if !caps.Has(requirement.Key) {
			return ttlKey(requirement.Key, subject+" with "+requirement.Settings)
		}
	}
	if reason := ydbfamily.Refusal(families, columns, key); reason != "" {
		return ttlFact(subject, reason)
	}
	if reason := ydbfamily.CreateRefusal(families); reason != "" {
		return ttlFact(subject, reason)
	}
	return nil
}

func familyDisplayName(subject objectidentity.ID) string { return ttlDisplayName(subject) }

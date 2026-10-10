package schemadiff

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/internal/tableidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

func comparisonIdentifiers(desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions) (identifier.Semantics, error) {
	semantics := identifier.ForDialect(opts.Dialect)
	if opts.IdentifierSemantics == nil {
		return semantics, nil
	}
	candidate := opts.IdentifierSemantics.Normalize(opts.Dialect)
	if !opts.IdentifierSemantics.IsZero() && !opts.IdentifierSemantics.Equal(candidate) {
		return identifier.Semantics{}, fmt.Errorf("%w: invalid identifier semantics snapshot", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := identifiervalidation.ValidateCoverage(candidate, collectIdentifierNames(desired, current, candidate.DefaultSchema)); err != nil {
		return identifier.Semantics{}, err
	}
	if err := identifiervalidation.ValidateTarget(desired, opts.Dialect, candidate); err != nil {
		return identifier.Semantics{}, err
	}
	return candidate, nil
}

func compareFeatures(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, target string,
	semantics identifier.Semantics, caps capability.Capabilities, parents []schemaext.ParentState, requests []schemaext.ChangeRequest,
	runtime schemaext.ComparisonRuntime,
) (schemaext.ComparisonResult, error) {
	declared, observed, err := captureFeatureStates(desired, current, target, semantics)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	declared.Coverage, err = namedFeatureSchemaLimits(declared, observed, desired.NotDescribed)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	observed.Coverage, err = namedFeatureSchemaLimits(observed, declared, current.NotDescribed)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if target == "" {
		// Without a target no owner can compare anything, so any owner value,
		// any limit a source recorded about one subject, and any change the
		// caller asks for is refused. A kind-level knowledge claim with no
		// value on either side is not: a read records what it knows about a
		// model even when it found none, and with nothing declared and nothing
		// found there is nothing to compare. A schema that uses no owner model
		// compares as it did before the model had an owner.
		if declared.Objects.Len() != 0 || observed.Objects.Len() != 0 || len(declared.Facets) != 0 || len(observed.Facets) != 0 ||
			len(declared.Coverage.SubjectRecords()) != 0 || len(observed.Coverage.SubjectRecords()) != 0 || len(requests) != 0 {
			return schemaext.ComparisonResult{}, fmt.Errorf("%w: feature comparison requires an explicit target", ptaherr.ErrUnsupportedDialect)
		}
		return schemaext.ComparisonResult{Complete: true, Desired: declared}, nil
	}
	result, err := runtime.CompareFeatures(ctx, schemaext.ComparisonRequest{Target: target, Identifiers: semantics, Capabilities: caps,
		Desired: declared, Current: observed, Owners: parents, Requests: requests, DeclaredRelations: declaredRelations(desired, semantics)})
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if !result.Complete {
		return schemaext.ComparisonResult{}, fmt.Errorf("%w: feature comparison did not complete", schemaext.ErrInvalidValue)
	}
	return result, ctx.Err()
}

// declaredRelations names the views and materialized views the desired
// schema declares, for owners whose objects share their namespace.
func declaredRelations(desired *schemamodel.Database, semantics identifier.Semantics) []objectidentity.ID {
	builder := objectidentity.NewBuilder(semantics)
	relations := make([]objectidentity.ID, 0, len(desired.Views)+len(desired.MaterializedViews))
	relation := func(kind objectidentity.Kind, name string) {
		ref := builder.Table(name)
		if ref.Name.Source == "" || ref.Name.Normalized == "" {
			return
		}
		ref.Kind = kind
		relations = append(relations, ref)
	}
	for _, view := range desired.Views {
		relation(objectidentity.KindView, view.Name)
	}
	for _, view := range desired.MaterializedViews {
		relation(objectidentity.KindMatView, view.Name)
	}
	return relations
}

func featureParents(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) ([]schemaext.ParentState, error) {
	declared, err := declaredFacetSlots(desired, target, semantics)
	if err != nil {
		return nil, err
	}
	parents := make(map[objectidentity.Key]schemaext.ParentState)
	for _, slot := range declared {
		ref := slot.subject
		if ref.Name.Source == "" || ref.Name.Normalized == "" {
			return nil, &RefusalError{cause: fmt.Errorf("%w: desired %s requires a name", ptaherr.ErrInvalidSchemaDiff, ref.Kind)}
		}
		if parents[ref.Key()].Desired {
			return nil, fmt.Errorf("%w: duplicate desired owner identity %s", ptaherr.ErrInvalidSchemaDiff, ref)
		}
		parents[ref.Key()] = schemaext.ParentState{Subject: ref, Desired: true}
	}
	for _, slot := range observedFacetSlots(current, target, semantics) {
		ref := slot.subject
		parent, found := parents[ref.Key()]
		if parent.Current {
			return nil, fmt.Errorf("%w: duplicate current owner identity %s", ptaherr.ErrInvalidSchemaDiff, ref)
		}
		if !found {
			parent.Subject = ref
		}
		parent.Current = true
		parents[ref.Key()] = parent
	}
	result := slices.Collect(maps.Values(parents))
	slices.SortFunc(result, func(a, b schemaext.ParentState) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}

func comparisonChanges(result schemaext.ComparisonResult) []schemaext.ChangeRecord {
	changes := slices.Clone(result.Changes)
	for _, facet := range result.FacetChanges {
		changes = append(changes, facet.Change)
	}
	return changes
}

func attachFeatureChanges(diff *difftypes.SchemaDiff, desired *schemamodel.Database, current *catalog.Database, changes []schemaext.ChangeRecord, target string, semantics identifier.Semantics) error {
	positions := make(map[objectidentity.Key]int, len(diff.TablesModified))
	for i, table := range diff.TablesModified {
		if table.Desired.HasTable() {
			ref := tableidentity.Subject(table.Desired.Table.Schema, table.Desired.Table.Name, target, semantics)
			positions[ref.Key()] = i
		}
	}
	declarations := make(map[objectidentity.Key]schemamodel.Table, len(desired.Tables))
	for _, table := range desired.Tables {
		ref := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		declarations[ref.Key()] = table
	}
	for _, change := range changes {
		if change.Subject.Parent.Empty() && change.Subject.Kind != objectidentity.KindTable {
			diff.FeatureChanges = append(diff.FeatureChanges, change)
			continue
		}
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: change.Subject.Catalog, Schema: change.Subject.Schema, Name: change.Subject.Parent}
		if change.Subject.Kind == objectidentity.KindTable {
			parent = change.Subject
		}
		position, found := positions[parent.Key()]
		if !found {
			table, exists := declarations[parent.Key()]
			if !exists {
				return fmt.Errorf("%w: feature change has no desired table %s", ptaherr.ErrInvalidSchemaDiff, parent)
			}
			position = len(diff.TablesModified)
			positions[parent.Key()] = position
			diff.TablesModified = append(diff.TablesModified, difftypes.TableDiff{TableName: catalog.QualifyTableName(table.Schema, table.Name), Desired: schemacapture.DeclareTable(desired, table, semantics)})
		}
		diff.TablesModified[position].FeatureChanges = append(diff.TablesModified[position].FeatureChanges, change)
	}
	observedTables := make(map[objectidentity.Key]catalog.Table, len(current.Tables))
	for _, observed := range current.Tables {
		ref := tableidentity.Subject(observed.Schema, observed.Name, target, semantics)
		observedTables[ref.Key()] = observed
	}
	for i := range diff.TablesModified {
		table := &diff.TablesModified[i]
		ref := tableidentity.Subject(table.Desired.Table.Schema, table.Desired.Table.Name, target, semantics)
		if observed, found := observedTables[ref.Key()]; found {
			table.Current = difftypes.TableObservationFor(current, observed, target, semantics)
		}
	}
	slices.SortFunc(diff.TablesModified, func(a, b difftypes.TableDiff) int {
		return schemaext.CompareRefs(tableidentity.Subject(a.Desired.Table.Schema, a.Desired.Table.Name, target, semantics), tableidentity.Subject(b.Desired.Table.Schema, b.Desired.Table.Name, target, semantics))
	})
	return nil
}

// constraintHostObservations uses the same structured identity as feature
// comparison. Constraint-only changes have no TablesModified entry to hold it.
func constraintHostObservations(current *catalog.Database, declarations []schemacapture.TableDeclaration, target string, semantics identifier.Semantics) []schemacapture.TableObservation {
	wanted := make(map[objectidentity.Key]bool, len(declarations))
	for _, declaration := range declarations {
		wanted[tableidentity.Subject(declaration.Table.Schema, declaration.Table.Name, target, semantics).Key()] = true
	}
	var result []schemacapture.TableObservation
	for _, table := range current.Tables {
		if wanted[tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()] {
			result = append(result, difftypes.TableObservationFor(current, table, target, semantics))
		}
	}
	return result
}

// Attribute-only changes need captures too: the reverse plan starts from the
// new comment and validation state, even when no constraint is created or removed.
func constraintHostDeclarations(desired *schemamodel.Database, diff *difftypes.SchemaDiff, semantics identifier.Semantics) []schemacapture.TableDeclaration {
	hosts := slices.Clone(diff.ConstraintsRemoved)
	for _, change := range diff.ConstraintCommentsChanged {
		hosts = append(hosts, difftypes.ConstraintRemovalInfo{TableName: change.TableName})
	}
	for _, change := range diff.ConstraintsValidated {
		hosts = append(hosts, difftypes.ConstraintRemovalInfo{TableName: change.TableName})
	}
	return difftypes.ConstraintHostDeclarationsOf(desired, diff.ConstraintsAdded, hosts, semantics)
}

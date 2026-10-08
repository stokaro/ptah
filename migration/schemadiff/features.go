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
	semantics identifier.Semantics, caps capability.Capabilities, parents []schemaext.ParentState, runtime schemaext.ComparisonRuntime) (schemaext.ComparisonResult, error) {
	declared, observed, err := captureFeatureStates(desired, current, target, semantics)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if target == "" {
		if declared.Objects.Len() != 0 || observed.Objects.Len() != 0 || len(declared.Facets) != 0 || len(observed.Facets) != 0 || !declared.Coverage.IsZero() || !observed.Coverage.IsZero() {
			return schemaext.ComparisonResult{}, fmt.Errorf("%w: feature comparison requires an explicit target", ptaherr.ErrUnsupportedDialect)
		}
		return schemaext.ComparisonResult{Complete: true, Desired: declared}, nil
	}
	result, err := runtime.CompareFeatures(ctx, schemaext.ComparisonRequest{Target: target, Identifiers: semantics, Capabilities: caps, Desired: declared, Current: observed, Owners: parents})
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if !result.Complete {
		return schemaext.ComparisonResult{}, fmt.Errorf("%w: feature comparison did not complete", schemaext.ErrInvalidValue)
	}
	return result, ctx.Err()
}

func featureParents(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) ([]schemaext.ParentState, error) {
	parents := make(map[objectidentity.Key]schemaext.ParentState)
	for _, table := range desired.Tables {
		ref := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		if ref.Name.Source == "" || ref.Name.Normalized == "" {
			return nil, &RefusalError{cause: fmt.Errorf("%w: desired table requires a name", ptaherr.ErrInvalidSchemaDiff)}
		}
		if parents[ref.Key()].Desired {
			return nil, fmt.Errorf("%w: duplicate desired table identity %s", ptaherr.ErrInvalidSchemaDiff, ref)
		}
		parents[ref.Key()] = schemaext.ParentState{Subject: ref, Desired: true}
	}
	for _, table := range current.Tables {
		ref := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		parent, found := parents[ref.Key()]
		if parent.Current {
			return nil, fmt.Errorf("%w: duplicate current table identity %s", ptaherr.ErrInvalidSchemaDiff, ref)
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

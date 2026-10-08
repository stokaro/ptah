package generator

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/migration/internal/tableidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

// restoreTableSource builds the rollback input from captured removal operands.
// The surrounding catalog still supplies unrelated objects and type vocabulary.
// A table and its children cannot be recovered from a later copy of that catalog.
func restoreTableSource(diff *difftypes.SchemaDiff, source *catalog.Database, dialect string) (*catalog.Database, error) {
	if len(diff.TablesRemoved) == 0 {
		return source, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	parents, err := capturedRemovalParents(diff.TablesRemoved, dialect, semantics)
	if err != nil {
		return nil, err
	}
	result := catalog.Database{}
	if source != nil {
		result = *source
	}
	removed := func(schema, name string) bool {
		_, found := parents[tableidentity.Subject(schema, name, dialect, semantics).Key()]
		return found
	}
	result.Tables = slices.DeleteFunc(slices.Clone(result.Tables), func(t catalog.Table) bool { return removed(t.Schema, t.Name) })
	result.Indexes = slices.DeleteFunc(slices.Clone(result.Indexes), func(i catalog.Index) bool { return removed(i.Schema, i.TableName) })
	result.Constraints = slices.DeleteFunc(slices.Clone(result.Constraints), func(c catalog.Constraint) bool { return removed(c.Schema, c.TableName) })
	result.Triggers = slices.DeleteFunc(slices.Clone(result.Triggers), func(t catalog.Trigger) bool { return removed(t.Schema, t.Table) })
	for _, removal := range diff.TablesRemoved {
		captured := removal.Current.Clone()
		result.Tables = append(result.Tables, captured.Table)
		result.Indexes = append(result.Indexes, captured.Indexes...)
		result.Constraints = append(result.Constraints, captured.Constraints...)
		result.Triggers = append(result.Triggers, captured.Triggers...)
	}
	result.FeatureObjects, result.FeatureCoverage, err = restoreTableFeatures(result.FeatureObjects, result.FeatureCoverage, diff.TablesRemoved, parents, dialect, semantics)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func capturedRemovalParents(removals difftypes.TableRemovals, dialect string, semantics identifier.Semantics) (map[objectidentity.Key]objectidentity.ID, error) {
	parents := make(map[objectidentity.Key]objectidentity.ID, len(removals))
	builder := objectidentity.NewBuilder(semantics)
	for _, removal := range removals {
		if !removal.Current.HasTable() {
			return nil, fmt.Errorf("%w: table removal %q has no captured observation", ptaherr.ErrInvalidSchemaDiff, removal.Name)
		}
		parent := tableidentity.Subject(removal.Current.Table.Schema, removal.Current.Table.Name, dialect, semantics)
		if builder.Table(removal.Name).Key() != parent.Key() {
			return nil, fmt.Errorf("%w: table removal %q disagrees with its captured identity", ptaherr.ErrInvalidSchemaDiff, removal.Name)
		}
		if _, duplicate := parents[parent.Key()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate table removal %q", ptaherr.ErrInvalidSchemaDiff, removal.Name)
		}
		if err := validateRemovedChildren(removal.Current, parent, dialect, semantics); err != nil {
			return nil, err
		}
		parents[parent.Key()] = parent
	}
	return parents, nil
}

func validateRemovedChildren(current schemacapture.TableObservation, parent objectidentity.ID, dialect string, semantics identifier.Semantics) error {
	var owners []objectidentity.ID
	for _, index := range current.Indexes {
		owners = append(owners, tableidentity.Subject(index.Schema, index.TableName, dialect, semantics))
	}
	for _, constraint := range current.Constraints {
		owners = append(owners, tableidentity.Subject(constraint.Schema, constraint.TableName, dialect, semantics))
	}
	for _, trigger := range current.Triggers {
		owners = append(owners, tableidentity.Subject(trigger.Schema, trigger.Table, dialect, semantics))
	}
	if slices.ContainsFunc(owners, func(owner objectidentity.ID) bool { return owner.Key() != parent.Key() }) ||
		current.OwnedObjects.ForParent(parent).Len() != current.OwnedObjects.Len() ||
		len(current.FeatureCoverage.ForParent(parent).SubjectRecords()) != len(current.FeatureCoverage.SubjectRecords()) {
		return fmt.Errorf("%w: table %s contains a captured child owned by another table", ptaherr.ErrInvalidSchemaDiff, parent)
	}
	if direction := current.FeatureCoverage.Representation(); direction != "" && direction != schemaext.Observed {
		return fmt.Errorf("%w: table %s requires observed feature coverage", ptaherr.ErrInvalidSchemaDiff, parent)
	}
	return nil
}

func restoredSubject(ref objectidentity.ID, parents map[objectidentity.Key]objectidentity.ID) bool {
	if _, found := parents[ref.Key()]; found {
		return true
	}
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
	_, found := parents[parent.Key()]
	return !ref.Parent.Empty() && found
}

func restoreTableFeatures(
	objects schemaext.Objects, coverage schemaext.Coverage, removals difftypes.TableRemovals,
	parents map[objectidentity.Key]objectidentity.ID, dialect string, semantics identifier.Semantics,
) (schemaext.Objects, schemaext.Coverage, error) {
	keep := func(ref objectidentity.ID) bool { return !restoredSubject(ref, parents) }
	objects = objects.Select(keep)
	for _, removal := range removals {
		var err error
		objects, err = objects.Merge(removal.Current.OwnedObjects)
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
	}
	coverage, err := restoreTableCoverage(coverage, removals, parents, dialect, semantics)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	return objects, coverage, nil
}

// Captured kind-wide knowledge becomes local to the restored table. Importing
// it as a global claim would describe unrelated tables the capture never read.
func restoreTableCoverage(source schemaext.Coverage, removals difftypes.TableRemovals, parents map[objectidentity.Key]objectidentity.ID, dialect string, semantics identifier.Semantics) (schemaext.Coverage, error) {
	if direction := source.Representation(); direction != "" && direction != schemaext.Observed {
		return schemaext.Coverage{}, fmt.Errorf("%w: rollback source requires observed feature coverage", ptaherr.ErrInvalidSchemaDiff)
	}
	kinds := make(map[schemaext.Kind]schemaext.KindCoverage)
	for _, record := range source.KindRecords() {
		kinds[record.Model.Kind] = record
	}
	for _, removal := range removals {
		for _, record := range removal.Current.FeatureCoverage.KindRecords() {
			kind := record.Model.Kind
			if existing, found := kinds[kind]; found {
				if existing.Model != record.Model {
					return schemaext.Coverage{}, fmt.Errorf("%w: captured definition of %q differs from the rollback source", schemaext.ErrInvalidValue, kind)
				}
				continue
			}
			record.Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "this model was captured only for removed tables"}
			kinds[kind] = record
		}
	}
	subjects := source.SelectSubjects(func(ref objectidentity.ID) bool { return !restoredSubject(ref, parents) }).SubjectRecords()
	for _, removal := range removals {
		parent := tableidentity.Subject(removal.Current.Table.Schema, removal.Current.Table.Name, dialect, semantics)
		for kind := range kinds {
			subjects = append(subjects, schemaext.SubjectCoverage{Kind: kind, Subject: parent, Knowledge: removal.Current.FeatureCoverage.Lookup(kind, parent)})
		}
		for _, record := range removal.Current.FeatureCoverage.SubjectRecords() {
			if record.Subject.Key() != parent.Key() {
				subjects = append(subjects, record)
			}
		}
	}
	return schemaext.NewCoverage(schemaext.Observed, slices.Collect(maps.Values(kinds)), subjects)
}

package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/tableidentity"
)

// projectCreatedTableRemovals captures the state a reverse DROP would destroy.
// Only accepted creations and their accepted additions supply this projection.
// Removals for a table replaced under the same name belong to its former state.
// The declarations of the created tables are converted together, so a provider
// is asked twice for all of them rather than twice for each.
func projectCreatedTableRemovals(ctx context.Context, forward, reverse *difftypes.SchemaDiff, dialect string, caps capability.Capabilities, runtime Runtime) error {
	semantics := forward.EffectiveIdentifierSemantics(dialect)
	creations := make(map[string]difftypes.TableCreation, len(forward.TablesAdded))
	for _, creation := range forward.TablesAdded {
		key := semantics.QualifiedTableIdentityKey(creation.Name)
		if _, duplicate := creations[key]; duplicate {
			return fmt.Errorf("cannot project duplicate table creation %q", creation.Name)
		}
		creations[key] = creation
	}
	var (
		projected    []int
		declarations []*schemamodel.Database
		uncreated    error
	)
	for i := range reverse.TablesRemoved {
		removal := &reverse.TablesRemoved[i]
		creation, found := creations[semantics.QualifiedTableIdentityKey(removal.Name)]
		if !found {
			// The removals ahead of this one are still projected first, so
			// their refusals come before this one.
			uncreated = fmt.Errorf("cannot project removal of uncreated table %q", removal.Name)
			break
		}
		if creation.Table.Name == "" {
			// A name-only intent is not evidence of an empty table. The planner
			// reports the missing creation operand before it can render a CREATE.
			removal.Current = schemacapture.TableObservation{}
			continue
		}
		projected = append(projected, i)
		declarations = append(declarations, createdTableDeclaration(forward, creation, semantics))
	}
	if len(declarations) != 0 {
		converted, err := goschematodb.ToDBSchemas(ctx, declarations, dialect, runtime)
		if err != nil {
			return err
		}
		for j, i := range projected {
			removal := &reverse.TablesRemoved[i]
			creation := creations[semantics.QualifiedTableIdentityKey(removal.Name)]
			current, err := projectCreatedTableState(ctx, forward, creation, converted[j], dialect, caps, runtime)
			if err != nil {
				return err
			}
			removal.Current = current
		}
	}
	return uncreated
}

// createdTableDeclaration is the document that declares one created table:
// the table with its fields, enums, constraints, owned objects and their
// coverage, and the indexes and triggers the diff adds to it.
func createdTableDeclaration(diff *difftypes.SchemaDiff, creation difftypes.TableCreation, semantics identifier.Semantics) *schemamodel.Database {
	declaration := &schemamodel.Database{
		Tables: []schemamodel.Table{creation.Table.Clone()}, Fields: slices.Clone(creation.Fields), Enums: slices.Clone(creation.Enums),
		Constraints: slices.Clone(creation.Constraints), FeatureObjects: creation.OwnedObjects, FeatureCoverage: creation.FeatureCoverage,
	}
	for _, added := range diff.IndexesAdded {
		if sameProjectedTable(semantics, added.TableName, creation.Name) {
			index := added.Index.Clone()
			index.TableName = creation.Table.QualifiedName()
			declaration.Indexes = append(declaration.Indexes, index)
		}
	}
	for _, added := range diff.TriggersAdded {
		if sameProjectedTable(semantics, added.TableName, creation.Name) {
			trigger := added.Desired.Clone()
			trigger.Table = creation.Table.QualifiedName()
			declaration.Triggers = append(declaration.Triggers, trigger)
		}
	}
	return declaration
}

// projectCreatedTableState is the state a created table holds once its
// declaration, converted as the target would read it, takes the constraints
// the diff adds to it.
func projectCreatedTableState(
	ctx context.Context, diff *difftypes.SchemaDiff, creation difftypes.TableCreation, converted *catalog.Database, dialect string, caps capability.Capabilities, runtime Runtime,
) (schemacapture.TableObservation, error) {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	if len(converted.Tables) != 1 {
		return schemacapture.TableObservation{}, fmt.Errorf("cannot project incomplete creation of table %q", creation.Name)
	}
	selected, err := runtime.ResolveTarget(dialect)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	converted.FeatureCoverage, err = tableidentity.BindCoverage(converted.FeatureCoverage, selected, semantics)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	before := difftypes.TableObservationFor(converted, converted.Tables[0], dialect, semantics)
	after := before.Clone()
	accepted := &difftypes.SchemaDiff{IdentifierSemantics: diff.IdentifierSemantics}
	for _, added := range diff.ConstraintsAdded {
		if !sameProjectedTable(semantics, added.TableName, creation.Name) {
			continue
		}
		constraint, err := projectedConstraintDefinition(added, after.Table)
		if err != nil {
			return schemacapture.TableObservation{}, err
		}
		position := slices.IndexFunc(after.Constraints, func(existing catalog.Constraint) bool {
			return semantics.IndexIdentityKey(existing.Name) == semantics.IndexIdentityKey(added.Name)
		})
		if position < 0 {
			after.Constraints = append(after.Constraints, constraint)
		} else {
			after.Constraints[position] = constraint
		}
		accepted.ConstraintsAdded = append(accepted.ConstraintsAdded, added)
	}
	after, unavailable, err := projectConstraintEffects(ctx, accepted, before, after, dialect, caps, runtime)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	if unavailable != "" {
		return schemacapture.TableObservation{}, nil
	}
	return after, nil
}

func sameProjectedTable(semantics identifier.Semantics, left, right string) bool {
	return semantics.QualifiedTableIdentityKey(left) == semantics.QualifiedTableIdentityKey(right)
}

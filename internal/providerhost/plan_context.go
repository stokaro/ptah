package providerhost

import (
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// planningContext retains unchanged prepared tables beside the captures each
// change carries. Changed operands are authoritative; preparation supplies the
// other parents an owner may need to interpret references.
func (p commonPlanner) planningContext(diff *difftypes.SchemaDiff) (featureplan.Request, map[objectidentity.Key]string, error) {
	request := featureplan.Request{Target: p.target, Identifiers: diff.EffectiveIdentifierSemantics(p.target),
		Capabilities: p.options.Capabilities, Changes: slices.Clone(diff.FeatureChanges), DatabasePath: diff.CurrentDatabasePath,
		Types: featureplan.TypeContext{Desired: schemaext.DefinedTypes(diff.Features.DesiredObjects), Current: schemaext.DefinedTypes(diff.Features.CurrentObjects)}}
	builder := objectidentity.NewBuilder(request.Identifiers)
	parents := newPlanningParents(builder, p.assessParents)
	if diff.TablePreparation != nil {
		for _, table := range diff.TablePreparation.Prepared {
			name := tableref.CanonicalExact(table.Desired.Table.Schema, table.Desired.Table.Name)
			if err := parents.add(featureplan.Table{Subject: table.Subject, Desired: table.Desired, Current: table.Current}, name); err != nil {
				return featureplan.Request{}, nil, err
			}
		}
	}
	for _, creation := range diff.TablesAdded {
		declared := schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields, Enums: creation.Enums, Constraints: creation.Constraints,
			OwnedObjects: creation.OwnedObjects, FeatureCoverage: creation.FeatureCoverage}
		table := featureplan.Table{Subject: builder.TableParts(creation.Table.Schema, creation.Table.Name), Action: featureplan.CreateTable, Desired: declared}
		if err := parents.add(table, creation.Name); err != nil {
			return featureplan.Request{}, nil, err
		}
	}
	for _, modification := range diff.TablesModified {
		table := featureplan.Table{Subject: builder.TableParts(modification.Desired.Table.Schema, modification.Desired.Table.Name), Action: featureplan.AlterTable,
			Desired: modification.Desired, Current: modification.Current}
		if err := parents.add(table, modification.TableName); err != nil {
			return featureplan.Request{}, nil, err
		}
		request.Changes = append(request.Changes, modification.FeatureChanges...)
	}
	for _, removal := range diff.TablesRemoved {
		observed := removal.Current.Table
		table := featureplan.Table{Subject: builder.TableParts(observed.Schema, observed.Name), Action: featureplan.DropTable, Current: removal.Current}
		if err := parents.add(table, removal.Name); err != nil {
			return featureplan.Request{}, nil, err
		}
	}
	request.Tables = parents.tables
	return request, parents.names, nil
}

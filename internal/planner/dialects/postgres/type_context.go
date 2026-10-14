package postgres

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) typeContext(diff *difftypes.SchemaDiff) featureplan.TypeContext {
	context := featureplan.TypeContext{Desired: schemaext.DefinedTypes(diff.Features.DesiredObjects), Current: schemaext.DefinedTypes(diff.Features.CurrentObjects)}
	// Change operands also describe types when the caller constructs a diff
	// without whole-schema snapshots. The owner validates each change before
	// scheduling; this projection does not decide whether the change is valid.
	for _, change := range diff.FeatureChanges {
		if creator, ok := change.Value.(schemaext.TypeCreator); ok {
			if typ, found := creator.CreatedType(change.Subject); found {
				context.Desired = appendType(context.Desired, typ)
			}
		}
		if observer, ok := change.Value.(schemaext.TypeObserver); ok {
			if typ, found := observer.ObservedType(change.Subject); found {
				context.Current = appendType(context.Current, typ)
			}
		}
	}
	return context
}

func appendType(types []schemaext.DefinedType, typ schemaext.DefinedType) []schemaext.DefinedType {
	for i, existing := range types {
		if existing.ID.Key() == typ.ID.Key() {
			types[i] = typ.Clone()
			return types
		}
	}
	return append(types, typ.Clone())
}

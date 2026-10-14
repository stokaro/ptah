package postgres

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
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
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.targetDialect()))
	context.Desired = append(context.Desired, schemaprep.TypeDefinitions(builder, diff.DomainsAdded, diff.CompositeTypesAdded)...)
	for _, change := range diff.DomainsModified {
		if change.Desired.Name != "" {
			definitions := schemaprep.TypeDefinitions(builder, []schemamodel.Domain{change.Desired}, nil)
			context.Desired = append(context.Desired, definitions...)
			current := definitions[0].Clone()
			current.References = currentDomainReferences(change)
			context.Current = append(context.Current, current)
		}
	}
	for _, change := range diff.CompositeTypesModified {
		if change.Desired.Name != "" {
			definitions := schemaprep.TypeDefinitions(builder, nil, []schemamodel.CompositeType{change.Desired})
			context.Desired = append(context.Desired, definitions...)
			current := definitions[0].Clone()
			current.References = change.CurrentFieldTypes
			context.Current = append(context.Current, current)
		}
	}
	for _, domain := range diff.DomainsRemoved {
		id := builder.TablePartsVerbatim(domain.Schema, domain.Name)
		id.Kind = objectidentity.KindDomain
		context.Current = append(context.Current, schemaext.DefinedType{ID: id, References: []string{domain.BaseType}})
	}
	for _, composite := range diff.CompositeTypesRemoved {
		id := builder.TablePartsVerbatim(composite.Schema, composite.Name)
		id.Kind = objectidentity.KindComposite
		var refs []string
		for _, field := range composite.Fields {
			refs = append(refs, field.Type)
		}
		context.Current = append(context.Current, schemaext.DefinedType{ID: id, References: refs})
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

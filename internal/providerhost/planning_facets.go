package providerhost

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

func (c *planningCapture) declaredFacets(parent objectidentity.ID, source schemacapture.TableDeclaration) ([]schemaext.FacetRecord, error) {
	builder := objectidentity.NewBuilder(c.identifiers)
	var records []schemaext.FacetRecord
	if err := addPlanningFacet(&records, parent, source.Table.Facets); err != nil {
		return nil, err
	}
	key, err := declaredPlanningKeyFacets(parent, &source, c.identifiers)
	if err != nil {
		return nil, err
	}
	if err := addPlanningFacet(&records, objectidentity.PrimaryKeyOf(parent), *key); err != nil {
		return nil, err
	}
	for _, field := range source.Fields {
		subject := planningChild(parent, builder.ColumnParts(parent.Schema.Source, parent.Name.Source, field.Name))
		if err := addPlanningFacet(&records, subject, field.Facets); err != nil {
			return nil, err
		}
	}
	for _, constraint := range source.Constraints {
		subject, _ := schemacapture.ConstraintSubject(parent, constraint.Type, constraint.Name, c.identifiers)
		if subject.Kind == objectidentity.KindPrimaryKey {
			continue
		}
		if err := addPlanningFacet(&records, subject, constraint.Facets); err != nil {
			return nil, err
		}
	}
	for _, index := range source.Indexes {
		subject := planningChild(parent, builder.IndexParts(parent.Schema.Source, parent.Name.Source, index.Name))
		if err := addPlanningFacet(&records, subject, index.Facets); err != nil {
			return nil, err
		}
	}
	for _, enum := range source.Enums {
		subject := builder.SchemaScopedParts(objectidentity.KindEnum, enum.Schema, enum.Name)
		subject.Database, subject.Catalog = parent.Database, parent.Catalog
		if err := addPlanningFacet(&records, subject, enum.Facets); err != nil {
			return nil, err
		}
	}
	for _, trigger := range source.Triggers {
		subject := planningChild(parent, builder.SchemaScopedParts(objectidentity.KindTrigger, parent.Schema.Source, trigger.Name))
		subject.Parent = parent.Name
		if err := addPlanningFacet(&records, subject, trigger.Facets); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (c *planningCapture) observedFacets(parent objectidentity.ID, source schemacapture.TableObservation) ([]schemaext.FacetRecord, error) {
	builder := objectidentity.NewBuilder(c.identifiers)
	var records []schemaext.FacetRecord
	if err := addPlanningFacet(&records, parent, source.Table.Facets); err != nil {
		return nil, err
	}
	for _, column := range source.Table.Columns {
		subject := planningChild(parent, builder.ColumnParts(parent.Schema.Source, parent.Name.Source, column.Name))
		if err := addPlanningFacet(&records, subject, column.Facets); err != nil {
			return nil, err
		}
	}
	keySeen := false
	for _, constraint := range source.Constraints {
		subject, _ := schemacapture.ConstraintSubject(parent, constraint.Type, constraint.Name, c.identifiers)
		if subject.Kind == objectidentity.KindPrimaryKey {
			if keySeen && len(constraint.Facets.DeclaredKinds()) != 0 {
				return nil, fmt.Errorf("%w: secondary primary-key slot has no owner", schemaext.ErrInvalidValue)
			}
			keySeen = true
		}
		if err := addPlanningFacet(&records, subject, constraint.Facets); err != nil {
			return nil, err
		}
	}
	for _, index := range source.Indexes {
		subject := planningChild(parent, builder.IndexParts(parent.Schema.Source, parent.Name.Source, index.Name))
		if index.IsPrimary {
			subject.Parent = parent.Name
		}
		if err := addPlanningFacet(&records, subject, index.Facets); err != nil {
			return nil, err
		}
	}
	for _, trigger := range source.Triggers {
		subject := planningChild(parent, builder.SchemaScopedParts(objectidentity.KindTrigger, parent.Schema.Source, trigger.Name))
		subject.Parent = parent.Name
		if err := addPlanningFacet(&records, subject, trigger.Facets); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// A primary key has one authoritative slot even when that slot has no value.
// Use the capture authority so comparison and planning never select different
// declarations. Other populated slots must not become a second owner.
func declaredPlanningKeyFacets(parent objectidentity.ID, source *schemacapture.TableDeclaration, semantics identifier.Semantics) (*schemaext.Facets, error) {
	owned := make([]*schemamodel.Constraint, len(source.Constraints))
	for i := range source.Constraints {
		owned[i] = &source.Constraints[i]
	}
	key := schemacapture.PrimaryKeyFacets(&source.Table, owned)
	if key != &source.Table.PrimaryKeyFacets && len(source.Table.PrimaryKeyFacets.DeclaredKinds()) != 0 {
		return nil, fmt.Errorf("%w: table primary-key slot has no owner beside a PRIMARY KEY constraint", schemaext.ErrInvalidValue)
	}
	for _, constraint := range owned {
		subject, _ := schemacapture.ConstraintSubject(parent, constraint.Type, constraint.Name, semantics)
		if subject.Kind == objectidentity.KindPrimaryKey && key != &constraint.Facets && len(constraint.Facets.DeclaredKinds()) != 0 {
			return nil, fmt.Errorf("%w: secondary primary-key slot has no owner", schemaext.ErrInvalidValue)
		}
	}
	return key, nil
}

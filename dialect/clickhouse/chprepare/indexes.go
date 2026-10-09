package chprepare

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

func prepareIndexes(table *schemapreparation.Table, semantics identifier.Semantics) error {
	builder := objectidentity.NewBuilder(semantics)
	current := make(map[objectidentity.Key]catalog.Index, len(table.Current.Indexes))
	for _, index := range table.Current.Indexes {
		subject := builder.IndexParts(index.Schema, index.TableName, index.Name)
		if _, duplicate := current[subject.Key()]; duplicate {
			return fmt.Errorf("%w: duplicate observed index %s", schemapreparation.ErrInvalid, subject)
		}
		current[subject.Key()] = index
	}
	for _, index := range table.Desired.Indexes {
		value, err := declaredIndexSettings(index)
		if err != nil {
			return err
		}
		if value == nil {
			continue
		}
		subject := builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name)
		request, err := indexResolution(table, subject, current)
		if err != nil {
			return err
		}
		request.Desired = value
		resolved, err := chresolve.Index(request)
		if err != nil {
			return err
		}
		facets, err := schemaext.NewFacets(&resolved.Prepared)
		if err != nil {
			return err
		}
		table.ResolvedFacets = append(table.ResolvedFacets, schemaext.FacetRecord{Subject: subject, Values: facets})
	}
	return nil
}

// Preparation and CREATE prediction consume the same decoded representation.
// Accepting common settings beside a native value would give each stage a
// different possible authority for the index's type and granularity.
func declaredIndexSettings(index schemamodel.Index) (*chschema.DesiredIndex, error) {
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](index.Facets, chschema.IndexKind)
	if err != nil || !found {
		return nil, err
	}
	if index.Type != "" {
		return nil, fmt.Errorf("%w: ClickHouse index %q settings must be decoded before preparation", schemaext.ErrInvalidValue, index.Name)
	}
	return value, nil
}

func indexResolution(table *schemapreparation.Table, subject objectidentity.ID, current map[objectidentity.Key]catalog.Index) (chresolve.IndexRequest, error) {
	before, exists := current[subject.Key()]
	knowledge := table.Current.FeatureCoverage.Lookup(chschema.IndexKind, subject)
	if table.CurrentKnowledge.State == schemaext.Absent {
		if exists {
			return chresolve.IndexRequest{}, fmt.Errorf("%w: an absent table cannot have current index %s", schemaext.ErrInvalidValue, subject)
		}
		return chresolve.IndexRequest{Creating: true}, nil
	}
	if table.CurrentKnowledge.State != schemaext.Complete {
		return chresolve.IndexRequest{}, nil
	}
	if !exists {
		return chresolve.IndexRequest{Creating: knowledge.State == schemaext.Complete || knowledge.State == schemaext.Absent}, nil
	}
	observed, present, err := schemaext.FacetAs[*chschema.ObservedIndex](before.Facets, chschema.IndexKind)
	if err != nil {
		return chresolve.IndexRequest{}, err
	}
	if knowledge.State == schemaext.Absent && present {
		return chresolve.IndexRequest{}, fmt.Errorf("%w: observed index settings are marked absent on %s", schemaext.ErrInvalidValue, subject)
	}
	// A captured value is evidence even when sibling indexes were not enumerated.
	// A limitation on this particular value takes precedence over that evidence.
	if limit, explicit := table.Current.FeatureCoverage.SubjectKnowledge(chschema.IndexKind, subject); explicit && limit.State != schemaext.Complete {
		observed = nil
	}
	return chresolve.IndexRequest{Current: observed}, nil
}

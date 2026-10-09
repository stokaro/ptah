package schemadiff

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/internal/tableidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

type comparisonTables struct {
	desired   *schemamodel.Database
	capture   *schemapreparation.Capture
	bySubject map[objectidentity.Key]schemapreparation.Table
	parents   []schemaext.ParentState
}

func prepareComparisonTables(ctx context.Context, desired *schemamodel.Database, current *catalog.Database,
	target string, semantics identifier.Semantics, caps capability.Capabilities, runtime schemapreparation.Runtime,
) (comparisonTables, error) {
	if target == "" {
		return comparisonTables{desired: desired}, nil
	}
	parents, err := featureParents(desired, current, target, semantics)
	if err != nil {
		return comparisonTables{}, err
	}
	observed := make(map[objectidentity.Key]catalog.Table, len(current.Tables))
	for _, table := range current.Tables {
		observed[tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()] = table
	}
	request := schemapreparation.Request{Target: target, Identifiers: semantics, Capabilities: caps}
	for _, table := range desired.Tables {
		subject := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		captured := schemapreparation.Table{
			Subject: subject, Desired: schemacapture.DeclareTable(desired, table, semantics),
		}
		if found, exists := observed[subject.Key()]; exists {
			captured.Current = difftypes.TableObservationFor(current, found, target, semantics)
			captured.CurrentKnowledge.State = schemaext.Complete
		} else if current.NotDescribed.DescribesSchema(table.Schema) {
			captured.CurrentKnowledge.State = schemaext.Absent
		}
		request.Tables = append(request.Tables, captured)
	}
	source := request.Clone()
	result, err := runtime.PrepareTables(ctx, request)
	if err != nil {
		return comparisonTables{}, err
	}
	if err := ctx.Err(); err != nil {
		return comparisonTables{}, err
	}
	capture, err := schemapreparation.Accept(source, result)
	if err != nil {
		return comparisonTables{}, err
	}
	prepared := make(map[objectidentity.Key]schemapreparation.Table, len(capture.Prepared))
	for _, table := range capture.Prepared {
		prepared[table.Subject.Key()] = table.Clone()
	}
	resolved, err := applyResolvedFacets(desired, prepared, target, semantics)
	if err != nil {
		return comparisonTables{}, err
	}
	return comparisonTables{desired: resolved, capture: &capture, bySubject: prepared, parents: parents}, nil
}

func applyResolvedFacets(desired *schemamodel.Database, prepared map[objectidentity.Key]schemapreparation.Table,
	target string, semantics identifier.Semantics,
) (*schemamodel.Database, error) {
	resolved := *desired
	resolved.Tables = slices.Clone(desired.Tables)
	resolved.Indexes = slices.Clone(desired.Indexes)
	slots := make(map[objectidentity.Key]*schemaext.Facets)
	for _, slot := range declaredFacetSlots(&resolved, target, semantics) {
		slots[slot.subject.Key()] = slot.values
	}
	for _, table := range prepared {
		for _, record := range table.ResolvedFacets {
			facets, found := slots[record.Subject.Key()]
			if !found {
				return nil, fmt.Errorf("%w: resolved facets have no declared owner %s", ptaherr.ErrInvalidSchemaDiff, record.Subject)
			}
			values, err := record.Values.Values()
			if err != nil {
				return nil, err
			}
			for _, value := range values {
				*facets, err = facets.Replace(value)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return &resolved, nil
}

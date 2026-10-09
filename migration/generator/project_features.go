package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// Keep object and facet identity spaces separate while composing their shared
// knowledge. Only accepted owner projections may change the captured state.
func projectTableFeatures(ctx context.Context, current schemacapture.TableObservation, recovery []schemaext.Reversal, semantics identifier.Semantics, codecs schemaext.Registry) (schemacapture.TableObservation, error) {
	builder := objectidentity.NewBuilder(semantics)
	parent := builder.TableParts(current.Table.Schema, current.Table.Name)
	// The table and each captured index own a facet record. An index facet is
	// projected onto the index it names, never onto the table or a new record.
	records := []schemaext.FacetRecord{{Subject: parent, Values: current.Table.Facets}}
	owners := map[objectidentity.Key]bool{parent.Key(): true}
	indexes := make([]objectidentity.ID, len(current.Indexes))
	for i, index := range current.Indexes {
		indexes[i] = builder.IndexParts(index.Schema, index.TableName, index.Name)
		records = append(records, schemaext.FacetRecord{Subject: indexes[i], Values: index.Facets})
		owners[indexes[i].Key()] = true
	}
	var objects []schemaext.ObjectProjection
	var facets []schemaext.FacetProjection
	for _, result := range recovery {
		if len(result.ForwardState) == 0 {
			return schemacapture.TableObservation{}, fmt.Errorf("%w: table reversal requires captured state projections", schemaext.ErrInvalidValue)
		}
		for _, projected := range result.ForwardState {
			switch projected.Placement {
			case schemaext.ObjectPlacement:
				if result.Change.Subject.Kind != objectidentity.Kind(projected.Kind) || !projectedChildOf(parent, result.Change.Subject) {
					return schemacapture.TableObservation{}, fmt.Errorf("%w: projected object belongs to another table", schemaext.ErrInvalidValue)
				}
				objects = append(objects, schemaext.ObjectProjection{Subject: result.Change.Subject, Value: projected.Value})
			case schemaext.FacetPlacement:
				subject := result.Change.Subject
				if !owners[subject.Key()] || subject.Key() != parent.Key() && !projectedChildOf(parent, subject) {
					return schemacapture.TableObservation{}, fmt.Errorf("%w: projected facet belongs to another table", schemaext.ErrInvalidValue)
				}
				facets = append(facets, schemaext.FacetProjection{Subject: subject, Kind: projected.Kind, Value: projected.Value})
			default:
				return schemacapture.TableObservation{}, fmt.Errorf("%w: unknown table reversal projection placement", schemaext.ErrInvalidValue)
			}
		}
	}
	state, err := codecs.ProjectObjects(ctx, schemaext.ObjectState{Objects: current.OwnedObjects, Coverage: current.FeatureCoverage}, objects)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	attached, err := codecs.ProjectFacets(ctx, schemaext.FacetState{Records: records, Coverage: state.Coverage}, facets)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	projected := make(map[objectidentity.Key]schemaext.Facets, len(attached.Records))
	for _, record := range attached.Records {
		projected[record.Subject.Key()] = record.Values
	}
	current.OwnedObjects, current.FeatureCoverage = state.Objects, attached.Coverage
	current.Table.Facets = projected[parent.Key()]
	current.Indexes = slices.Clone(current.Indexes)
	for i, subject := range indexes {
		current.Indexes[i].Facets = projected[subject.Key()]
	}
	return current, nil
}

func projectedChildOf(parent, child objectidentity.ID) bool {
	return child.Catalog.Normalized == parent.Catalog.Normalized && child.Schema.Normalized == parent.Schema.Normalized &&
		child.Parent.Normalized != "" && child.Parent.Normalized == parent.Name.Normalized
}

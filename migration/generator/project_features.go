package generator

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// Keep object and facet identity spaces separate while composing their shared
// knowledge. Only accepted owner projections may change the captured state.
func projectTableFeatures(ctx context.Context, current schemacapture.TableObservation, recovery []schemaext.Reversal, semantics identifier.Semantics, codecs schemaext.Registry) (schemacapture.TableObservation, error) {
	parent := objectidentity.NewBuilder(semantics).TableParts(current.Table.Schema, current.Table.Name)
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
				if result.Change.Subject.Key() != parent.Key() {
					return schemacapture.TableObservation{}, fmt.Errorf("%w: projected facet belongs to another table", schemaext.ErrInvalidValue)
				}
				facets = append(facets, schemaext.FacetProjection{Subject: parent, Kind: projected.Kind, Value: projected.Value})
			default:
				return schemacapture.TableObservation{}, fmt.Errorf("%w: unknown table reversal projection placement", schemaext.ErrInvalidValue)
			}
		}
	}
	state, err := codecs.ProjectObjects(ctx, schemaext.ObjectState{Objects: current.OwnedObjects, Coverage: current.FeatureCoverage}, objects)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	attached, err := codecs.ProjectFacets(ctx, schemaext.FacetState{
		Records: []schemaext.FacetRecord{{Subject: parent, Values: current.Table.Facets}}, Coverage: state.Coverage,
	}, facets)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	current.OwnedObjects, current.FeatureCoverage = state.Objects, attached.Coverage
	current.Table.Facets = attached.Records[0].Values
	return current, nil
}

func projectedChildOf(parent, child objectidentity.ID) bool {
	return child.Catalog.Normalized == parent.Catalog.Normalized && child.Schema.Normalized == parent.Schema.Normalized &&
		child.Parent.Normalized != "" && child.Parent.Normalized == parent.Name.Normalized
}

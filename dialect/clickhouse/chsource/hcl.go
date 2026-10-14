package chsource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// HCL contributes ClickHouse refresh schedules to materialized view blocks.
// Omitted and empty refresh values declare an insert-maintained view. The
// schedule grammar and target scope match Go annotations and YAML. Export
// refuses an uncaptured schedule rather than declaring it absent.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: chschema.Owner, Kinds: []schemaext.Kind{chschema.RefreshKind},
		Blocks: []hclext.Block{{Site: hclext.MaterializedViewAttributes, Type: "materialized",
			Attributes: []hclext.Attribute{{Name: "refresh", Kind: hclext.String}}}},
		Decoder: refreshHCL{}, Encoder: refreshHCL{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return RefreshCoverage() },
	}
}

type refreshHCL struct{}

// DecodeHCL attaches a normalized schedule to each materialized view claim.
func (refreshHCL) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		facets, err := RefreshFacets(claim.Attributes["refresh"].Text)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations[i].Facets = facets
	}
	return result, nil
}

// EncodeHCL writes the schedule through the shared HCL writer's view slot.
func (refreshHCL) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if err := requireCapturedSchedules(request.Coverage, "an HCL schema"); err != nil {
		return hclext.EncodeResult{}, err
	}
	if len(request.Objects)+len(request.Tables)+len(request.Columns)+len(request.Indexes)+len(request.Keys)+len(request.Routines) > 0 {
		return hclext.EncodeResult{}, fmt.Errorf("%w: an HCL refresh schedule belongs to a materialized view", ptaherr.ErrUnsupportedFeature)
	}
	var result hclext.EncodeResult
	for _, slot := range request.MaterializedViews {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		schedule, _, err := schemaext.FacetAs[*chschema.DesiredRefresh](slot.Facets, chschema.RefreshKind)
		if err == nil {
			err = chschema.ValidateDesiredRefresh(schedule)
		}
		if err != nil {
			return hclext.EncodeResult{}, fmt.Errorf("materialized view %q: %w", slot.Slot.Name, err)
		}
		result.Attributes = append(result.Attributes, hclext.SlotAttributes{Slot: slot.Slot,
			Attributes: []hclext.EncodedAttribute{{Name: "refresh", Value: hclext.Value{Kind: hclext.String, Text: schedule.Clause()}}}})
	}
	return result, nil
}

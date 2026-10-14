package chsource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
)

// HCL contributes ClickHouse row_policy blocks and materialized view refresh
// schedules. Row policies use the same attributes and target scope as YAML.
// Omitted and empty refresh values declare an insert-maintained view. The
// schedule grammar and target scope match Go annotations and YAML. Export
// refuses an uncaptured schedule rather than declaring it absent.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: chschema.Owner, Kinds: []schemaext.Kind{chschema.RefreshKind, chschema.RowPolicyKind},
		Blocks: []hclext.Block{{Site: hclext.MaterializedViewAttributes, Type: "materialized",
			Attributes: []hclext.Attribute{{Name: "refresh", Kind: hclext.String}}}, rowPolicyHCLBlock()},
		Decoder: hclSource{}, Encoder: hclSource{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return ownedCoverage() },
	}
}

type hclSource struct{}

// DecodeHCL collects policies and attaches schedules to materialized views.
func (hclSource) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	var collector chpolicysource.Collector
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		if claim.Block == "row_policy" {
			declaration, err := decodeHCLRowPolicy(claim, &collector)
			if err != nil {
				return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
			}
			result.Declarations[i] = declaration
			continue
		}
		facets, err := RefreshFacets(claim.Attributes["refresh"].Text)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations[i].Facets = facets
	}
	return result, nil
}

// EncodeHCL writes policies and schedules through the shared HCL writer.
func (hclSource) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if err := requireCapturedSchedules(request.Coverage, "an HCL schema"); err != nil {
		return hclext.EncodeResult{}, err
	}
	if err := chpolicysource.RequireDescribed(request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	if len(request.Tables)+len(request.Columns)+len(request.Indexes)+len(request.Keys)+len(request.Routines) > 0 {
		return hclext.EncodeResult{}, fmt.Errorf("%w: an HCL refresh schedule belongs to a materialized view", ptaherr.ErrUnsupportedFeature)
	}
	var result hclext.EncodeResult
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := encodeHCLRowPolicy(object)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		result.Blocks = append(result.Blocks, block)
	}
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

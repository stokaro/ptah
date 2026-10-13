package pgroutinesource

import (
	"context"
	"fmt"
	"strconv"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgroutine"
)

// HCL registers PostgreSQL evaluation properties on function blocks. Attribute
// expressions are evaluated by the host; the owner receives only typed data.
func HCL() hclext.Extension {
	return hclext.Extension{Owner: pgroutine.PropertiesOwner, Kinds: []schemaext.Kind{pgroutine.PropertiesKind},
		Blocks: []hclext.Block{{Type: "function", Site: hclext.RoutineAttributes, Attributes: []hclext.Attribute{
			{Name: "leakproof", Kind: hclext.Bool}, {Name: "parallel", Kind: hclext.Text}, {Name: "strict", Kind: hclext.Bool},
		}}}, Decoder: hclCodec{}, Encoder: hclCodec{}, Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return Coverage() },
	}
}

type hclCodec struct{}

func (hclCodec) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		attributes := make(map[string]string, len(claim.Attributes))
		for name, value := range claim.Attributes {
			attributes[name] = value.Text
			if value.Kind == hclext.Bool {
				attributes[name] = strconv.FormatBool(value.Bool)
			}
		}
		facets, err := properties(attributes)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations[i].Facets = facets
	}
	return result, ctx.Err()
}
func (hclCodec) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if err := exportCoverage(request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	result := hclext.EncodeResult{}
	for _, slot := range request.Routines {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		if slot.Slot.Kind != hclext.SlotFunction {
			return hclext.EncodeResult{}, fmt.Errorf("%w: function properties on %s", schemaext.ErrInvalidValue, slot.Slot)
		}
		value, _, err := pgroutine.PropertiesOf(slot.Facets)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		var attributes []hclext.EncodedAttribute
		if value.Leakproof {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "leakproof", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
		}
		if value.Parallel != "" {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "parallel", Value: hclext.Value{Kind: hclext.String, Text: value.Parallel}})
		}
		if value.Strict {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "strict", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
		}
		result.Attributes = append(result.Attributes, hclext.SlotAttributes{Slot: slot.Slot, Attributes: attributes})
	}
	return result, ctx.Err()
}

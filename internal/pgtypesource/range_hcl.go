package pgtypesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

func rangeBlock() hclext.Block {
	attributes := []hclext.Attribute{
		{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true},
		{Name: "subtype", Kind: hclext.TypeName},
	}
	for _, name := range pgtype.RangeAttributes() {
		attributes = append(attributes, hclext.Attribute{Name: name, Kind: hclext.Text})
	}
	attributes = append(attributes, hclext.Attribute{Name: "comment", Kind: hclext.String})
	return hclext.Block{Site: hclext.TopLevel, Type: "range", Naming: hclext.SchemaAndName, DeclaresType: true, Attributes: attributes}
}

func (hclEncoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	if ctx == nil {
		return hclext.Result{}, fmt.Errorf("%w: range decoding requires a context", schemaext.ErrInvalidValue)
	}
	result := hclext.Result{Declarations: make([]hclext.Declaration, 0, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := rangeHCLObject(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations = append(result.Declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	if err := ctx.Err(); err != nil {
		return hclext.Result{}, err
	}
	return result, nil
}

func rangeHCLObject(claim hclext.Claim) (schemaext.Object, error) {
	if claim.Block != "range" {
		return schemaext.Object{}, fmt.Errorf("%w: unexpected range block %q", schemaext.ErrInvalidValue, claim.Block)
	}
	attrs := claim.Attributes
	value := &pgtype.DesiredRange{Subtype: strings.TrimSpace(attrs["subtype"].Text), SubtypeOpClass: optionalRangeValue(attrs["subtype_opclass"].Text),
		Collation: optionalRangeValue(attrs["collation"].Text), Canonical: optionalRangeValue(attrs["canonical"].Text), SubtypeDiff: optionalRangeValue(attrs["subtype_diff"].Text), Comment: strings.TrimSpace(attrs["comment"].Text)}
	if value.Subtype == "" {
		return schemaext.Object{}, fmt.Errorf("range %q requires subtype", claim.Name)
	}
	for _, name := range pgtype.RangeAttributes() {
		if text, present := attrs[name]; present && strings.TrimSpace(text.Text) == "" {
			value.Cleared = append(value.Cleared, name)
		}
	}
	return pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, claim.Schema, claim.Name), value)
}

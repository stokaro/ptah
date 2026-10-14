package pgdefaultprivilegesource

import (
	"context"
	"fmt"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

func (hclSource) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if ctx == nil {
		return hclext.EncodeResult{}, fmt.Errorf("%w: default privilege export requires a context", schemaext.ErrInvalidValue)
	}
	limits, err := encodedLimits(request.Coverage)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	var result hclext.EncodeResult
	for _, limit := range limits {
		result.Header = append(result.Header, (coverage.Object{Kind: LimitKind, Name: limit.Name}).Directive())
	}
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := encodedHCL(object)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		result.Blocks = append(result.Blocks, block)
	}
	if err := ctx.Err(); err != nil {
		return hclext.EncodeResult{}, err
	}
	return result, nil
}

func encodedHCL(object schemaext.Object) (hclext.EncodedBlock, error) {
	identity, value, err := desiredObject(object)
	if err != nil {
		return hclext.EncodedBlock{}, err
	}
	privileges, grantable := grantLists(value)
	attributes := []hclext.EncodedAttribute{
		{Name: "for_role", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "role", Name: identity.Grantor}}},
		{Name: "object_type", Value: hclext.Value{Kind: hclext.String, Text: identity.ObjectType}},
		{Name: "to", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "role", Name: identity.Grantee}}},
	}
	for _, attribute := range []hclext.EncodedAttribute{
		{Name: "privileges", Value: hclext.Value{Kind: hclext.TextList, Texts: privileges}},
		{Name: "grantable", Value: hclext.Value{Kind: hclext.TextList, Texts: grantable}},
		{Name: "revoked", Value: hclext.Value{Kind: hclext.TextList, Texts: value.Revoked}},
	} {
		if len(attribute.Value.Texts) > 0 {
			attributes = append(attributes, attribute)
		}
	}
	if value.Comment != "" {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "comment", Value: hclext.Value{Kind: hclext.String, Text: value.Comment}})
	}
	return hclext.EncodedBlock{Type: "default_privilege", Unlabeled: true, Placement: hclext.AfterTriggers,
		Schema: identity.Schema, SchemaRole: hclext.SchemaAddress, Scope: object.Ref.Name.Source,
		Object: object.Ref, Path: "default_privileges." + object.Ref.Name.Source, Attributes: attributes}, nil
}

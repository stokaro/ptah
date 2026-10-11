package ydbsource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// hclEncoder writes the YDB owner's blocks and header lines of one document.
type hclEncoder struct{}

// EncodeHCL writes the document's account of its coordination nodes in the
// header, the captured claim [HCLCoordinationDirectives] returns, and a
// coordination_node block for each node, after the tables.
//
// The block is a Ptah extension of the format, because a document that
// cannot name a node cannot ask for it to be kept. A setting the declaration
// leaves unset is left out, and the reader takes it as YDB's default. A node
// that is not a valid declaration is refused rather than written.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	header, err := HCLCoordinationDirectives(request.Coverage)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		if schemaext.Kind(object.Ref.Kind) != ydbcoordination.Kind {
			continue
		}
		node, ok := object.Value.(*ydbcoordination.Desired)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired coordination values", schemaext.ErrInvalidValue)
		}
		if err := ydbcoordination.ValidateRef(object.Ref); err != nil {
			return hclext.EncodeResult{}, err
		}
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return hclext.EncodeResult{}, err
		}
		settings := ydbcoordination.Attributes(node.Spec)
		attributes := make([]hclext.EncodedAttribute, 0, len(settings))
		for _, setting := range settings {
			if setting[1] != "" {
				attributes = append(attributes, hclext.EncodedAttribute{Name: setting[0], Value: hclext.Value{Kind: hclext.String, Text: setting[1]}})
			}
		}
		blocks = append(blocks, hclext.EncodedBlock{Type: coordinationBlock, Placement: hclext.AfterTables,
			Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source, Object: object.Ref, Attributes: attributes})
	}
	return hclext.EncodeResult{Blocks: blocks, Header: header, Attributes: settingAttributes(request)}, nil
}

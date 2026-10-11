package pgextensionsource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// HCL is the extension owner's contribution to the HCL frontend: the
// `extension` block. One label is the extension's name, and two are the
// schema it is installed into and the name. `schema` names that schema as a
// string, a string template or a schema reference, and agrees with a schema
// label. `if_not_exists`, `version` and `comment` are the declaration's.
//
// Its claim is the one every source makes: a document describes every
// extension, and one it leaves out is absent. The writer writes each
// extension back as the same block.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: pgextension.Owner,
		Kinds: []schemaext.Kind{pgextension.ExtensionKind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "extension", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{
				{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true},
				{Name: "if_not_exists", Kind: hclext.Bool},
				{Name: "version", Kind: hclext.Text},
				{Name: "comment", Kind: hclext.Text},
			}}},
		Decoder: hclDecoder{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
			return pgextension.CompleteCoverage(schemaext.Desired)
		},
		Encoder: hclEncoder{},
	}
}

// hclDecoder reads the extension blocks of one document.
type hclDecoder struct{}

// DecodeHCL declares one extension for each block, with no target scope, as
// an HCL document names no target.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := Declare(claim.Name, pgextension.DesiredExtension{Schema: claim.Schema,
			Version: claim.Attributes["version"].Text, Comment: claim.Attributes["comment"].Text,
			IfNotExists: claim.Attributes["if_not_exists"].Bool})
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// hclEncoder writes the extensions of one document.
type hclEncoder struct{}

// EncodeHCL writes one extension block for each extension, among the blocks
// the rest of the document may depend on, as the reader reads it back. The
// schema is where the extension is installed; the names it supplies let the
// Atlas-compatible surface keep a block the document needs. A value that is
// not a valid declaration is refused rather than written.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		extension, ok := object.Value.(*pgextension.DesiredExtension)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired extension values", schemaext.ErrInvalidValue)
		}
		if err := pgextension.ValidateRef(object.Ref); err != nil {
			return hclext.EncodeResult{}, err
		}
		if err := pgextension.ValidateDesired(extension); err != nil {
			return hclext.EncodeResult{}, err
		}
		name := pgextension.Name(object.Ref)
		var attributes []hclext.EncodedAttribute
		if extension.IfNotExists {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "if_not_exists", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
		}
		for _, setting := range [][2]string{{"version", extension.Version}, {"comment", extension.Comment}} {
			if setting[1] != "" {
				attributes = append(attributes, hclext.EncodedAttribute{Name: setting[0], Value: hclext.Value{Kind: hclext.String, Text: setting[1]}})
			}
		}
		blocks = append(blocks, hclext.EncodedBlock{Type: "extension", Placement: hclext.BeforeTypes, Name: name,
			Schema: extension.Schema, SchemaRole: hclext.SchemaPlacement, Provides: slices.Clone(extension.Provides),
			Path: "extensions." + name, Object: object.Ref, Attributes: attributes})
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

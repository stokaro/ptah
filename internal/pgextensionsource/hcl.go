package pgextensionsource

import (
	"context"

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
// extension, and one it leaves out is absent.
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

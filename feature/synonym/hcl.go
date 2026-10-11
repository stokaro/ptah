package synonym

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

// HCL is the synonym owner's extension of the HCL frontend: a top-level
// `synonym` block, named by its labels like every other object, with a
// required `target` string and an optional comment.
//
// It is a Ptah extension of the format: the pinned Atlas community binary
// has no synonym block. Its claim is that every HCL document describes the
// synonyms of the database, so one without a synonym block describes a
// database without one.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: Owner,
		Kinds: []schemaext.Kind{Kind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "synonym", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "target", Kind: hclext.String}, {Name: "comment", Kind: hclext.Text}}}},
		Decoder:  hclDecoder{},
		Coverage: hclCoverage,
	}
}

// hclDecoder reads the synonym blocks of one document.
type hclDecoder struct{}

// DecodeHCL declares one synonym for each block. `target` is required: a
// synonym that stands for nothing is not one, and the server has no default.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		target := claim.Attributes["target"].Text
		if strings.TrimSpace(target) == "" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("synonym %q requires a target", claim.Name)}
		}
		declared := DesiredSynonym{Comment: claim.Attributes["comment"].Text,
			Synonym: Synonym{Schema: claim.Schema, Name: claim.Name, Target: target}}
		if err := ValidateDesired(&declared); err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{DeclaredObject(declared)}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// hclCoverage claims every synonym an HCL document can declare.
func hclCoverage() (schemaext.Coverage, error) {
	return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

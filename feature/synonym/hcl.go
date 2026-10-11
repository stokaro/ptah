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
// database without one. The writer writes each synonym back as the same
// block.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: Owner,
		Kinds: []schemaext.Kind{Kind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "synonym", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "target", Kind: hclext.String}, {Name: "comment", Kind: hclext.Text}}}},
		Decoder:  hclDecoder{},
		Coverage: hclCoverage,
		Encoder:  hclEncoder{},
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
func hclCoverage(hclext.CoverageRequest) (schemaext.Coverage, error) {
	return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// hclEncoder writes the synonyms of one document.
type hclEncoder struct{}

// EncodeHCL writes one synonym block for each synonym, after the tables a
// synonym stands for. A synonym the document holds as something other than a
// declaration, or one that is not valid, is refused rather than written: a
// block the reader would refuse is not a description of the database.
// Reading the block back binds the synonym to every target with synonyms,
// so a synonym scoped to exactly those keeps its scope.
//
// The block exists because HCL had no way to say one, and a document that
// cannot name an object cannot ask for it to be kept: `schema inspect` into a
// file and `schema apply --to` that file planned DROP SYNONYM for every
// synonym on the server, through Ptah's own output (stokaro/ptah#1031). It is
// written on the compatibility surface too: the pinned Atlas community binary
// v1.3.0 answers `unknown driver "sqlserver"`, so there is no document of its
// to mirror, and on a dialect it does speak it drops a top-level block it
// does not model at exit 0.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		declared, ok := object.Value.(*DesiredSynonym)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired synonym values", schemaext.ErrInvalidValue)
		}
		if err := ValidateDesired(declared); err != nil {
			return hclext.EncodeResult{}, err
		}
		blocks = append(blocks, hclext.EncodedBlock{Type: "synonym", Placement: hclext.AfterTables,
			Schema: declared.Schema, Name: declared.Name, Object: object.Ref, Targets: Targets(),
			Attributes: hclStrings("target", declared.Target, "comment", declared.Comment)})
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

// hclStrings writes each named string a block sets, in order, and leaves out
// one it leaves empty.
func hclStrings(pairs ...string) []hclext.EncodedAttribute {
	attributes := make([]hclext.EncodedAttribute, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			attributes = append(attributes, hclext.EncodedAttribute{Name: pairs[i], Value: hclext.Value{Kind: hclext.String, Text: pairs[i+1]}})
		}
	}
	return attributes
}

package pgtablesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// ExclusionHCL reads and writes each table's exclude blocks. The owner receives
// evaluated values and produces objects, without a second HCL parser.
func ExclusionHCL() hclext.Extension {
	return hclext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		Blocks: []hclext.Block{{Site: hclext.InTable, Type: "exclude", Attributes: []hclext.Attribute{
			{Name: "name", Kind: hclext.String}, {Name: "using", Kind: hclext.Text}, {Name: "elements", Kind: hclext.Text},
			{Name: "condition", Kind: hclext.Text}, {Name: "deferrable", Kind: hclext.Bool}, {Name: "initially", Kind: hclext.String},
			{Name: "comment", Kind: hclext.String}, {Name: "requires", Kind: hclext.String},
		}}}, Decoder: exclusionHCL{}, Encoder: exclusionHCL{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return exclusionCoverage() },
	}
}

type exclusionHCL struct{}

func (exclusionHCL) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	if ctx == nil {
		return hclext.Result{}, fmt.Errorf("%w: EXCLUDE HCL requires a context", schemaext.ErrInvalidValue)
	}
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := decodeExclusionHCL(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations[i].Objects = []schemaext.Object{object}
	}
	if err := ctx.Err(); err != nil {
		return hclext.Result{}, err
	}
	return result, nil
}

func decodeExclusionHCL(claim hclext.Claim) (schemaext.Object, error) {
	if claim.Block != "exclude" || claim.Table == nil {
		return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE requires a table claim", schemaext.ErrInvalidValue)
	}
	attributes := make(map[string]string, len(claim.Attributes))
	for name, value := range claim.Attributes {
		if name == "deferrable" {
			if value.Kind != hclext.Bool {
				return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE deferrable must be a boolean", schemaext.ErrInvalidValue)
			}
			attributes[name] = "false"
			if value.Bool {
				attributes[name] = "true"
			}
		} else {
			attributes[name] = value.Text
		}
	}
	table := annotation.Table{Schema: claim.Table.Schema, Name: claim.Table.Name, Struct: claim.Table.Struct}
	attributes["table"] = table.QualifiedName()
	return exclusionAnnotation(annotation.Declaration{Directive: ExclusionDirective, Attributes: attributes, Struct: claim.Table.Struct}, annotation.Tables{table})
}

func (exclusionHCL) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if ctx == nil {
		return hclext.EncodeResult{}, fmt.Errorf("%w: EXCLUDE HCL requires a context", schemaext.ErrInvalidValue)
	}
	if err := exclusionExportCoverage(request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	var result hclext.EncodeResult
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := encodeExclusionHCL(object)
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

func encodeExclusionHCL(object schemaext.Object) (hclext.EncodedBlock, error) {
	value, err := exclusionValue(object)
	if err != nil {
		return hclext.EncodedBlock{}, err
	}
	if len(object.Targets) != 0 {
		return hclext.EncodedBlock{}, fmt.Errorf("%w: EXCLUDE HCL cannot preserve target scope %q", ptaherr.ErrUnsupportedFeature, object.Targets)
	}
	table := annotation.Table{Schema: sqlident.UnquoteDoubleQuoted(object.Ref.Schema.Authored()), Name: sqlident.UnquoteDoubleQuoted(object.Ref.Parent.Source)}
	// The annotation codec supplies the same field names and dependency spelling.
	// HCL adds only typed values and placement within the owning table.
	directive, err := exclusionEncodedAnnotation(object, map[objectidentity.Key]annotation.Table{pgtable.ExclusionTable(object.Ref).Key(): table})
	if err != nil {
		return hclext.EncodedBlock{}, err
	}
	block := hclext.EncodedBlock{Type: "exclude", Placement: hclext.WithinTable, Table: &hclext.Table{Schema: table.Schema, Name: table.Name},
		Object: object.Ref, Path: fmt.Sprintf("tables.%s.exclusions.%s", table.QualifiedName(), object.Ref.Name.Source)}
	for _, expression := range []string{value.Elements, value.WhereCondition} {
		tokens, tokenErr := partitionTokens(expression)
		if tokenErr != nil {
			return hclext.EncodedBlock{}, tokenErr
		}
		for _, token := range tokens {
			if token.Type == lexer.TokenIdentifier || strings.HasPrefix(token.Value, `"`) {
				block.References = append(block.References, partitionName(token.Value))
			}
		}
	}
	for _, attribute := range directive.Attributes {
		if attribute.Name == "table" {
			continue
		}
		hclValue := hclext.Value{Kind: hclext.String, Text: attribute.Value}
		if attribute.Name == "elements" || attribute.Name == "condition" {
			hclValue.Kind = hclext.SQL
		}
		if attribute.Name == "deferrable" {
			hclValue = hclext.Value{Kind: hclext.Bool, Bool: value.Deferrable}
		}
		block.Attributes = append(block.Attributes, hclext.EncodedAttribute{Name: attribute.Name, Value: hclValue})
	}
	return block, nil
}

// Package pgtypesource connects PostgreSQL user type models to schema sources.
// It reads and writes domain, composite and range declarations in Go
// annotations, HCL and SQL through source contracts, without owning a source
// frontend. Domains and composite types share their syntax with Oracle's,
// which stay on the common model, so a declaration reaches this owner only
// where its document or scope names a PostgreSQL-family target or no target.
package pgtypesource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// HCL reads and writes domain, composite and range blocks. It reads a domain
// or composite block of a document of targets or of no target; one of another
// target, such as Oracle, stays with the frontend's own grammar. Type
// references resolve through the frontend's declared type registry.
func HCL(targets []string) hclext.Extension {
	return hclext.Extension{Owner: pgtype.Owner, Kinds: pgtype.Kinds(), Encoder: hclEncoder{}, Decoder: hclEncoder{},
		Blocks: []hclext.Block{domainBlock(targets), compositeBlockShape(targets), rangeBlock()}, Header: []coverage.Kind{coverage.Range},
		Coverage: func(request hclext.CoverageRequest) (schemaext.Coverage, error) {
			return sourceCoverage(request.Limits)
		},
	}
}

// hclEncoder writes the types of one document.
type hclEncoder struct{}

// EncodeHCL writes one block for each type. A value that is not a valid
// declaration is refused rather than written.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if ctx == nil {
		return hclext.EncodeResult{}, fmt.Errorf("%w: type export requires a context", schemaext.ErrInvalidValue)
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := typeBlock(object)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		blocks = append(blocks, block)
	}
	limits, err := rangeLimits(request.Coverage.SelectKinds([]schemaext.Kind{pgtype.RangeKind}))
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	result := hclext.EncodeResult{Blocks: blocks}
	for _, limit := range limits {
		result.Header = append(result.Header, (coverage.Object{Kind: coverage.Range, Name: limit.Name}).Directive())
	}
	return result, nil
}

// typeBlock is the block of one type: its type and attributes by its model,
// its schema as the source wrote it, and its name.
func typeBlock(object schemaext.Object) (hclext.EncodedBlock, error) {
	if err := pgtype.ValidateRef(object.Ref); err != nil {
		return hclext.EncodedBlock{}, err
	}
	block := hclext.EncodedBlock{Placement: hclext.UserTypes, Schema: pgtype.AuthoredSchema(object.Ref), Name: object.Ref.Name.Source,
		Object: object.Ref}
	var err error
	switch value := object.Value.(type) {
	case *pgtype.DesiredDomain:
		block.Type = "domain"
		block.Attributes, err = domainAttributes(value)
	case *pgtype.DesiredComposite:
		block.Type = "composite"
		block.Attributes, block.Children, err = compositeBlock(value)
	case *pgtype.DesiredRange:
		block.Type = "range"
		block.Attributes, err = rangeAttributes(value)
	default:
		err = fmt.Errorf("%w: HCL writes declared PostgreSQL types, not %T", schemaext.ErrInvalidValue, object.Value)
	}
	if err != nil {
		return hclext.EncodedBlock{}, err
	}
	return block, nil
}

// domainAttributes are a domain's attributes in the order the writer writes
// the common model's: its type, `null = false` for NOT NULL, a default
// expression as SQL or else a literal default as a string, its CHECK, and
// its comment. The type, the CHECK and the comment are written without the
// space around them, as the common model's are.
func domainAttributes(domain *pgtype.DesiredDomain) ([]hclext.EncodedAttribute, error) {
	if err := pgtype.ValidateDesiredDomain(domain); err != nil {
		return nil, err
	}
	attributes := []hclext.EncodedAttribute{sqlAttribute("type", strings.TrimSpace(domain.BaseType))}
	if domain.NotNull {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "null", Value: hclext.Value{Kind: hclext.Bool}})
	}
	switch {
	case domain.DefaultExpr != "":
		attributes = append(attributes, sqlAttribute("default", domain.DefaultExpr))
	case domain.Default != "":
		attributes = append(attributes, stringAttribute("default", domain.Default))
	}
	if check := strings.TrimSpace(domain.Check); check != "" {
		attributes = append(attributes, sqlAttribute("check", check))
	}
	return appendComment(attributes, domain.Comment), nil
}

// compositeBlock is a composite type's comment and a `field` child for each
// of its attributes, in order.
func compositeBlock(composite *pgtype.DesiredComposite) ([]hclext.EncodedAttribute, []hclext.EncodedChild, error) {
	if err := pgtype.ValidateDesiredComposite(composite); err != nil {
		return nil, nil, err
	}
	fields := make([]hclext.EncodedChild, 0, len(composite.Attributes))
	for _, attribute := range composite.Attributes {
		fields = append(fields, hclext.EncodedChild{Type: "field", Name: attribute.Name,
			Attributes: []hclext.EncodedAttribute{sqlAttribute("type", attribute.Type)}})
	}
	return appendComment(nil, composite.Comment), fields, nil
}

// rangeAttributes are a range type's subtype, each optional attribute it
// states, or states as cleared with an empty string, and its comment.
func rangeAttributes(rangeType *pgtype.DesiredRange) ([]hclext.EncodedAttribute, error) {
	if err := pgtype.ValidateDesiredRange(rangeType); err != nil {
		return nil, err
	}
	attributes := []hclext.EncodedAttribute{sqlAttribute("subtype", strings.TrimSpace(rangeType.Subtype))}
	for _, name := range pgtype.RangeAttributes() {
		if value := rangeType.Attribute(name); value != "" || slices.Contains(rangeType.Cleared, name) {
			attributes = append(attributes, stringAttribute(name, value))
		}
	}
	return appendComment(attributes, rangeType.Comment), nil
}

// appendComment appends a comment that is not blank, without the space
// around it.
func appendComment(attributes []hclext.EncodedAttribute, comment string) []hclext.EncodedAttribute {
	if comment = strings.TrimSpace(comment); comment == "" {
		return attributes
	}
	return append(attributes, stringAttribute("comment", comment))
}

func sqlAttribute(name, text string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.SQL, Text: text}}
}

func stringAttribute(name, text string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: text}}
}

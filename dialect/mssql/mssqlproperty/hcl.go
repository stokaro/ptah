package mssqlproperty

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// propertyBlock is the HCL block that declares an extended property.
const propertyBlock = "extended_property"

// HCL is the owner's extension of the HCL frontend: a top-level
// `extended_property` block, named by its labels, that addresses the database,
// a schema, a table or a column with `schema`, `table` and `column`, and
// carries a `value` and a comment.
//
// It is a Ptah extension of the format. Its claim is that every HCL document
// describes the extended properties of the database, so one without such a
// block describes a database without one. The writer writes each property
// back as the same block.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: Owner,
		Kinds: []schemaext.Kind{Kind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: propertyBlock, Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{
				{Name: "table", Kind: hclext.String}, {Name: "column", Kind: hclext.String},
				{Name: "value", Kind: hclext.String}, {Name: "comment", Kind: hclext.Text},
			}}},
		Decoder: hclDecoder{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
			return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		},
		Encoder: hclEncoder{},
	}
}

// hclDecoder reads the extended_property blocks of one document.
type hclDecoder struct{}

// DecodeHCL declares one property for each block.
//
// The address is refused exactly where the SQL Server renderer refuses it: a
// `table` needs the `schema` that holds it and a `column` needs its `table`,
// because each level is passed by name (`@level1name = N'users'`) and a level
// whose parent is missing addresses nothing. Refusing here rather than at
// render time means the document says where the mistake is. An absent
// `schema` with no `table` is not a mistake: that is the DATABASE-scoped
// property, the one address that passes no level at all.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	var declared schemaext.Objects
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		property, err := hclProperty(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		object := DeclaredObject(property)
		if declared, err = declared.With(object); err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i,
				Err: fmt.Errorf("%s %s is declared twice: %v", propertyBlock, property.Label(), err)}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// hclProperty reads one block into the property it declares.
func hclProperty(claim hclext.Claim) (DesiredProperty, error) {
	table, column := claim.Attributes["table"].Text, claim.Attributes["column"].Text
	if strings.TrimSpace(table) != "" && strings.TrimSpace(claim.Schema) == "" {
		return DesiredProperty{}, fmt.Errorf("%s %q names table %q and no schema; SQL Server addresses a table "+
			"through the schema that holds it", propertyBlock, claim.Name, table)
	}
	if strings.TrimSpace(column) != "" && strings.TrimSpace(table) == "" {
		return DesiredProperty{}, fmt.Errorf("%s %q names column %q and no table; SQL Server addresses a column "+
			"through the table that holds it", propertyBlock, claim.Name, column)
	}
	property := DesiredProperty{Comment: claim.Attributes["comment"].Text, Property: Property{Name: claim.Name, Schema: claim.Schema,
		Table: table, Column: column, Value: claim.Attributes["value"].Text}}
	if err := ValidateDesired(&property); err != nil {
		return DesiredProperty{}, err
	}
	return property, nil
}

// hclEncoder writes the extended properties of one document.
type hclEncoder struct{}

// EncodeHCL writes one extended_property block for each property, after the
// tables a property is attached to, in the order of their labels.
//
// The address is written as plain strings rather than as `table.users` and
// `column.title` references, and that is a decision rather than an omission.
// SQL Server addresses a property by name -- `@level1name = N'users'` -- so
// the string is what the statement carries; and a reference would resolve to
// nothing the moment selection removed the table, leaving a document that
// names a block it does not contain.
//
// The schema is written as the property holds it: an empty schema is the
// DATABASE scope, a fourth address beside schema, table and column, and
// writing the document's default schema for it would move a database
// property onto a schema. [Property.Label] spells that address `(database)`.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	type labeled struct {
		label string
		block hclext.EncodedBlock
	}
	properties := make([]labeled, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		property, ok := object.Value.(*DesiredProperty)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired extended property values", schemaext.ErrInvalidValue)
		}
		if err := ValidateDesired(property); err != nil {
			return hclext.EncodeResult{}, err
		}
		properties = append(properties, labeled{label: property.Label(), block: hclext.EncodedBlock{
			Type: propertyBlock, Placement: hclext.AfterTables,
			Schema: property.Schema, SchemaIsAddress: true, Name: property.Name,
			Object: object.Ref, Targets: []string{platform.SQLServer},
			Attributes: hclStrings("table", property.Table, "column", property.Column, "value", property.Value, "comment", property.Comment),
		}})
	}
	slices.SortStableFunc(properties, func(a, b labeled) int { return cmp.Compare(a.label, b.label) })
	blocks := make([]hclext.EncodedBlock, 0, len(properties))
	for _, property := range properties {
		blocks = append(blocks, property.block)
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

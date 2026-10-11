package mssqlproperty

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
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
// block describes a database without one.
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
		Coverage: func() (schemaext.Coverage, error) {
			return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		},
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

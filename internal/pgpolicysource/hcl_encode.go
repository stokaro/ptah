package pgpolicysource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
)

// hclEncoder writes the row-level security of one document.
type hclEncoder struct{}

// EncodeHCL writes a row_security block in each table whose switches the
// document holds, and a policy block for each policy, after the triggers.
//
// A document's claim is that it describes every policy and every table's
// switches, so coverage a source could not describe is refused rather than
// written as absent, and so is a value that is not a valid declaration.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	objects, err := schemaext.NewObjects(request.Objects...)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	if err := RequireRepresentable(request.Coverage, objects); err != nil {
		return hclext.EncodeResult{}, err
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Tables)+len(request.Objects))
	for _, table := range request.Tables {
		block, written, err := encodeSwitches(table)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		if written {
			blocks = append(blocks, block)
		}
	}
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := encodePolicy(object)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		blocks = append(blocks, block)
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

// encodeSwitches writes a table's row-level security switches as its
// row_security block, and false for a table that holds none. A block can only
// enable, so a table forced without being enabled is a loss, and so is a
// binding to targets.
func encodeSwitches(table hclext.TableFacets) (hclext.EncodedBlock, bool, error) {
	state, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](table.Facets, pgpolicy.TableStateKind)
	if err != nil || !found {
		return hclext.EncodedBlock{}, false, err
	}
	if err := pgpolicy.ValidateDesiredTableState(state); err != nil {
		return hclext.EncodedBlock{}, false, err
	}
	block := hclext.EncodedBlock{Type: hclRowSecurityBlock, Placement: hclext.WithinTable, Table: &table.Table,
		Path: "table." + schemamodel.QualifyTableName(table.Table.Schema, table.Table.Name) + ".row_security"}
	if scope := table.Facets.TargetScope(pgpolicy.TableStateKind); len(scope) > 0 {
		block.Losses = append(block.Losses, fmt.Sprintf("dialect scope %q is not represented in HCL", strings.Join(scope, ",")))
	}
	if !state.Enabled {
		if state.Forced {
			block.Losses = append(block.Losses, "FORCE ROW LEVEL SECURITY without ENABLE cannot be represented in HCL")
		}
		block.Unwritten = true
		return block, len(block.Losses) > 0, nil
	}
	block.Attributes = []hclext.EncodedAttribute{{Name: "enabled", Value: hclext.Value{Kind: hclext.Bool, Bool: true}}}
	if state.Forced {
		block.Attributes = append(block.Attributes, hclext.EncodedAttribute{Name: "enforced", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
	}
	block.Attributes = append(block.Attributes, hclStringAttributes("comment", state.Comment)...)
	return block, true, nil
}

// encodePolicy writes a policy, writing each value its declaration states
// and nothing it leaves out, so the document reads back as the same
// declaration. A role named like a role keyword is a loss, and the block is
// left out: a reader takes the name for the keyword.
func encodePolicy(object schemaext.Object) (hclext.EncodedBlock, error) {
	policy, ok := object.Value.(*pgpolicy.DesiredPolicy)
	if !ok {
		return hclext.EncodedBlock{}, fmt.Errorf("%w: HCL requires desired policy values", schemaext.ErrInvalidValue)
	}
	if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
		return hclext.EncodedBlock{}, err
	}
	if err := pgpolicy.ValidateDesiredPolicy(policy); err != nil {
		return hclext.EncodedBlock{}, err
	}
	schema, table := object.Ref.Schema.Authored(), object.Ref.Parent.Source
	qualified := schemamodel.QualifyTableName(schema, table)
	block := hclext.EncodedBlock{Type: hclPolicyBlock, Placement: hclext.AfterTriggers, Name: object.Ref.Name.Source,
		Scope: qualified, SchemaIsAddress: true, Object: object.Ref, Path: "rls_policies." + qualified + "." + object.Ref.Name.Source}
	roles, ok := hclRoles(policy.Roles)
	if !ok {
		block.Losses, block.Unwritten = []string{"a role named like a role keyword cannot be represented in HCL"}, true
		return block, nil
	}
	block.Attributes = []hclext.EncodedAttribute{{Name: "on", Value: hclext.Value{Kind: hclext.Ref,
		Reference: &hclext.Reference{Kind: "table", Schema: schema, Name: table}}}}
	block.Attributes = append(block.Attributes, hclStringAttributes("for", string(policy.Command))...)
	if roles != nil {
		block.Attributes = append(block.Attributes, hclext.EncodedAttribute{Name: "to", Value: hclext.Value{Kind: hclext.Refs, References: roles}})
	}
	block.Attributes = append(block.Attributes, hclStringAttributes("as", strings.ToUpper(string(policy.Composition)),
		"using", optionalText(policy.Using), "check", optionalText(policy.WithCheck), "comment", policy.Comment)...)
	return block, nil
}

// hclRoles writes a policy's TO list: a keyword as a name of no kind, which
// the writer writes as its quoted spelling, and a role as a role. It reports
// false for a role named like a keyword. A policy that names no roles has no
// list.
func hclRoles(roles []pgpolicy.RoleSelector) ([]hclext.Reference, bool) {
	if roles == nil {
		return nil, true
	}
	references := make([]hclext.Reference, 0, len(roles))
	for _, role := range pgpolicy.CanonicalRoles(roles) {
		if role.Keyword != "" {
			references = append(references, hclext.Reference{Name: string(role.Keyword)})
			continue
		}
		if SpellsKeyword(role.Name) {
			return nil, false
		}
		references = append(references, hclext.Reference{Kind: "role", Name: role.Name})
	}
	return references, true
}

// optionalText is a clause a declaration may leave out, empty when it does.
func optionalText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// hclStringAttributes writes each named string a block sets, in order, and
// leaves out one it leaves empty.
func hclStringAttributes(pairs ...string) []hclext.EncodedAttribute {
	attributes := make([]hclext.EncodedAttribute, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			attributes = append(attributes, hclext.EncodedAttribute{Name: pairs[i], Value: hclext.Value{Kind: hclext.String, Text: pairs[i+1]}})
		}
	}
	return attributes
}

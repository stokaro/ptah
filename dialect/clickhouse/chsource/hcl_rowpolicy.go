package chsource

import (
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
)

func rowPolicyHCLBlock() hclext.Block {
	return hclext.Block{Site: hclext.TopLevel, Type: "row_policy", Naming: hclext.JoinedName,
		Attributes: []hclext.Attribute{
			{Name: "on", Kind: hclext.Ref, Refers: "table"},
			{Name: "using", Kind: hclext.Text},
			{Name: "to", Kind: hclext.String},
			{Name: "as", Kind: hclext.String},
		}}
}

func decodeHCLRowPolicy(claim hclext.Claim, collector *chpolicysource.Collector) (hclext.Declaration, error) {
	written, err := composition(claim.Attributes["as"].Text)
	if err != nil {
		return hclext.Declaration{}, err
	}
	policy, err := (chpolicysource.Attributes{To: claim.Attributes["to"].Text, Using: claim.Attributes["using"].Text}).Policy()
	if err != nil {
		return hclext.Declaration{}, err
	}
	policy.Composition = written
	on := claim.Attributes["on"].Reference
	ref := chschema.RowPolicyRef(on.Schema, on.Name, claim.Name)
	if err := collector.AddPolicy(fmt.Sprintf("row_policy %q at %s", claim.Name, claim.At), ref, policy, []string{platform.ClickHouse}); err != nil {
		return hclext.Declaration{}, err
	}
	object, _, err := collector.Objects().Get(ref)
	if err != nil {
		return hclext.Declaration{}, err
	}
	return hclext.Declaration{Objects: []schemaext.Object{object}}, nil
}

func encodeHCLRowPolicy(object schemaext.Object) (hclext.EncodedBlock, error) {
	policy, ok := object.Value.(*chschema.DesiredRowPolicy)
	if !ok {
		return hclext.EncodedBlock{}, fmt.Errorf("%w: HCL requires desired ClickHouse row policy values", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateRowPolicyRef(object.Ref); err != nil {
		return hclext.EncodedBlock{}, err
	}
	if err := chschema.ValidateDesiredRowPolicy(policy); err != nil {
		return hclext.EncodedBlock{}, err
	}
	database, table := chschema.RowPolicyDatabase(object.Ref), object.Ref.Parent.Source
	qualified := schemamodel.QualifyTableName(database, table)
	block := hclext.EncodedBlock{Type: "row_policy", Placement: hclext.AfterTriggers, Name: object.Ref.Name.Source,
		Scope: qualified, SchemaRole: hclext.SchemaAddress, Object: object.Ref, Targets: []string{platform.ClickHouse}, Path: "row_policies." + qualified + "." + object.Ref.Name.Source,
		Attributes: []hclext.EncodedAttribute{{Name: "on", Value: hclext.Value{Kind: hclext.Ref,
			Reference: &hclext.Reference{Kind: "table", Schema: database, Name: table}}}},
	}
	if policy.Filter != nil {
		block.Attributes = append(block.Attributes, hclext.EncodedAttribute{Name: "using", Value: hclext.Value{Kind: hclext.String, Text: *policy.Filter}})
	}
	for _, attribute := range []struct{ name, value string }{
		{"to", chpolicysource.FormatRoles(policy.Roles)}, {"as", strings.ToUpper(string(policy.Composition))},
	} {
		if attribute.value != "" {
			block.Attributes = append(block.Attributes, hclext.EncodedAttribute{Name: attribute.name, Value: hclext.Value{Kind: hclext.String, Text: attribute.value}})
		}
	}
	return block, nil
}

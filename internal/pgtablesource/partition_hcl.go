package pgtablesource

import (
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/lexer"
)

func partitionBlock() hclext.Block {
	return hclext.Block{Site: hclext.InTable, Type: "partition", Attributes: []hclext.Attribute{
		{Name: "type", Kind: hclext.Text}, {Name: "columns", Kind: hclext.Refs, Refers: "column"},
	}, Children: []hclext.Child{{Type: "by", Unlabeled: true, Attributes: []hclext.Attribute{
		{Name: "column", Kind: hclext.Ref, Refers: "column", Optional: true}, {Name: "expr", Kind: hclext.Text},
		{Name: "collate", Kind: hclext.String}, {Name: "ops", Kind: hclext.String},
	}}}}
}

func hclPartition(claim hclext.Claim) (schemaext.Facets, error) {
	if claim.Table == nil {
		return schemaext.Facets{}, partitionSyntax("partition requires a table claim")
	}
	value := &pgtable.DesiredPartition{Type: strings.ToUpper(claim.Attributes["type"].Text)}
	columns, written := claim.Attributes["columns"]
	if written && len(claim.Children) > 0 {
		return schemaext.Facets{}, partitionSyntax("partition cannot mix columns attribute with by blocks")
	}
	if written {
		for _, column := range columns.References {
			value.Parts = append(value.Parts, pgtable.PartitionPart{Name: column.Name})
		}
	}
	for _, child := range claim.Children {
		part := pgtable.PartitionPart{Expr: child.Attributes["expr"].Text}
		if column, found := child.Attributes["column"]; found {
			if column.Reference == nil {
				return schemaext.Facets{}, partitionSyntax("partition by requires a column reference")
			}
			part.Name = column.Reference.Name
		}
		var err error
		part.Collation, err = partitionModifier(child.Attributes["collate"].Text)
		if err != nil {
			return schemaext.Facets{}, err
		}
		part.OperatorClass, err = partitionModifier(child.Attributes["ops"].Text)
		if err != nil {
			return schemaext.Facets{}, err
		}
		value.Parts = append(value.Parts, part)
	}
	if err := pgtable.ValidateDesiredPartition(value); err != nil {
		return schemaext.Facets{}, err
	}
	return schemaext.NewFacets(value)
}

func partitionModifier(text string) (string, error) {
	if text == "" {
		return "", nil
	}
	tokens, err := partitionTokens(text)
	if err != nil {
		return "", err
	}
	name, end, err := partitionIdentifier(tokens, 0)
	if err != nil {
		return "", err
	}
	if end != len(tokens) {
		return "", partitionSyntax("invalid partition key modifier " + text)
	}
	return name, nil
}

func encodePartition(table hclext.TableFacets) (*hclext.EncodedBlock, error) {
	value, found, err := pgtable.PartitionOf(table.Facets)
	if err != nil || !found {
		return nil, err
	}
	block := &hclext.EncodedBlock{Type: "partition", Placement: hclext.WithinTable, Table: &table.Table,
		Path:       fmt.Sprintf("tables.%s.partition", table.Table.Name),
		Attributes: []hclext.EncodedAttribute{{Name: "type", Value: hclext.Value{Kind: hclext.String, Text: value.Type}}}}
	for _, part := range value.Parts {
		for _, sql := range []string{part.Expr, part.Collation, part.OperatorClass} {
			tokens, err := partitionTokens(sql)
			if err != nil {
				return nil, err
			}
			for _, token := range tokens {
				if token.Type == lexer.TokenIdentifier || strings.HasPrefix(token.Value, `"`) {
					block.References = append(block.References, partitionName(token.Value))
				}
			}
		}
		child := hclext.EncodedChild{Type: "by", Unlabeled: true}
		if part.Name != "" {
			child.Attributes = append(child.Attributes, hclext.EncodedAttribute{Name: "column", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "column", Name: part.Name}}})
		}
		if part.Expr != "" {
			child.Attributes = append(child.Attributes, hclext.EncodedAttribute{Name: "expr", Value: hclext.Value{Kind: hclext.SQL, Text: part.Expr}})
		}
		for _, modifier := range []struct{ name, value string }{{"collate", part.Collation}, {"ops", part.OperatorClass}} {
			if modifier.value != "" {
				child.Attributes = append(child.Attributes, hclext.EncodedAttribute{Name: modifier.name, Value: hclext.Value{Kind: hclext.String, Text: modifier.value}})
			}
		}
		block.Children = append(block.Children, child)
	}
	return block, nil
}

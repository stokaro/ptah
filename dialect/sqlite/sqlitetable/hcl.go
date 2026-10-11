package sqlitetable

import (
	"context"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

// HCL returns the owner's extension of the HCL frontend. It reads no block
// and makes no claim. Its encoder writes a table's options into the writer's
// table block as `strict` and `without_rowid`, the spelling the community
// binary reads, each only when set. It writes nothing for a virtual table's
// module declaration, which no HCL block carries; the inspect command reports
// that loss with the module.
func HCL() hclext.Extension {
	return hclext.Extension{Owner: Owner, Kinds: []schemaext.Kind{TableKind, VirtualKind}, Encoder: hclEncoder{}}
}

// hclEncoder writes the owner's table options into the HCL writer's table
// blocks.
type hclEncoder struct{}

// EncodeHCL writes the options of each table, declared or read. A value the
// owner's models cannot read writes nothing, and is refused where it is used.
func (hclEncoder) EncodeHCL(_ context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	var result hclext.EncodeResult
	for _, table := range request.Tables {
		options := tableOptions(table.Facets)
		var attributes []hclext.EncodedAttribute
		if options.Strict {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "strict", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
		}
		if options.WithoutRowID {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "without_rowid", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
		}
		if len(attributes) > 0 {
			result.Attributes = append(result.Attributes, hclext.SlotAttributes{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table.Table},
				Attributes: attributes})
		}
	}
	return result, nil
}

// tableOptions are the options a table's facets hold, declared or read.
func tableOptions(facets schemaext.Facets) Options {
	if declared, found, err := schemaext.FacetAs[*DesiredTable](facets, TableKind); err == nil && found && declared != nil {
		return declared.Options
	}
	if observed, found, err := schemaext.FacetAs[*ObservedTable](facets, TableKind); err == nil && found && observed != nil {
		return observed.Options
	}
	return Options{}
}

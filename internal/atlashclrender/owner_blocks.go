package atlashclrender

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemamodel"
)

// encodeOwnerBlocks hands the owners the document: its objects, its tables'
// facets and its coverage. The writers take no context yet; the owners
// encode in process.
func (r *renderer) encodeOwnerBlocks(owners hclext.Set) (hclext.EncodeResult, error) {
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	tables := make([]hclext.TableFacets, 0, len(r.db.Tables))
	for _, table := range r.db.Tables {
		tables = append(tables, hclext.TableFacets{Table: hclext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName},
			Facets: table.Facets})
	}
	return owners.Encode(context.Background(), hclext.EncodeRequest{Objects: objects, Tables: tables, Coverage: r.db.FeatureCoverage})
}

// renderAfterTables writes the blocks that stand for a table or are attached
// to one -- a hypertable, a continuous aggregate, a synonym, a YDB
// coordination node, a SQL Server extended property -- after the tables, so a
// reader meets the structure before the things that point at it. None of
// them is an Atlas block, so no Atlas output fixes their order, and the order
// by type, schema and name is the stable one.
func (r *renderer) renderAfterTables() {
	r.renderOwnerBlocks(hclext.AfterTables, nil)
}

// renderOwnerBlocks writes the blocks the owners placed at placement, in a
// table when table is not nil, ordered by type, schema, scope and name so a
// document's order does not depend on which owner wrote a block or when its
// owner was registered.
func (r *renderer) renderOwnerBlocks(placement hclext.Placement, table *hclext.Table) {
	type placed struct {
		schema string
		block  hclext.EncodedBlock
	}
	var blocks []placed
	for _, block := range r.ownerBlocks {
		if block.Placement != placement || (table != nil && *block.Table != *table) {
			continue
		}
		blocks = append(blocks, placed{schema: r.ownerSchema(block), block: block})
	}
	slices.SortStableFunc(blocks, func(a, b placed) int {
		return cmp.Or(cmp.Compare(a.block.Type, b.block.Type), cmp.Compare(a.schema, b.schema),
			cmp.Compare(a.block.Scope, b.block.Scope), cmp.Compare(a.block.Name, b.block.Name))
	})
	for _, block := range blocks {
		r.writeOwnerBlock(block.block, block.schema)
	}
}

// ownerSchema is the schema an owner's block writes: the one the block names
// or, unless the block's empty schema is an address of its own, the
// document's default schema.
func (r *renderer) ownerSchema(block hclext.EncodedBlock) string {
	if !block.SchemaIsAddress {
		return r.schemaFor(block.Schema)
	}
	if strings.TrimSpace(block.Schema) == "" {
		return ""
	}
	return block.Schema
}

// writeOwnerBlock writes a block an owner encoded where it stands: first the
// losses the owner reports about it, then, unless the block is unwritten or
// the Atlas-compatible surface leaves its type out, its label, its `schema`
// reference and its attributes in the owner's order. A block in a table is
// indented one level and takes no label.
func (r *renderer) writeOwnerBlock(block hclext.EncodedBlock, schema string) {
	for _, loss := range block.Losses {
		r.warn(block.Path, loss)
	}
	if block.Unwritten {
		return
	}
	if block.Table != nil {
		r.linef("  %s {", block.Type)
		for _, attribute := range block.Attributes {
			r.rawAttr(2, attribute.Name, r.ownerValue(attribute.Value))
		}
		r.line("  }")
		return
	}
	if r.omitRefusedBlock(block.Path, block.Type, block.Name) {
		return
	}
	r.linef(`%s %s {`, block.Type, quote(block.Name))
	if schema != "" {
		r.rawAttr(1, "schema", r.schemaRef(schema))
	}
	for _, attribute := range block.Attributes {
		r.rawAttr(1, attribute.Name, r.ownerValue(attribute.Value))
	}
	r.line("}")
	r.line("")
}

// ownerValue writes an encoded value: a string quoted, a bool bare, a
// reference to a table as the reference this document resolves, and a list
// of roles element by element, a role the document declares as a reference
// and any other element as a string. Set.Encode admits no other value.
func (r *renderer) ownerValue(value hclext.Value) string {
	switch value.Kind {
	case hclext.Bool:
		return strconv.FormatBool(value.Bool)
	case hclext.Ref:
		return r.tableRef(schemamodel.QualifyTableName(value.Reference.Schema, value.Reference.Name))
	case hclext.Refs:
		elements := make([]string, 0, len(value.References))
		for _, element := range value.References {
			if element.Kind == "" {
				elements = append(elements, quote(element.Name))
				continue
			}
			elements = append(elements, r.roleTarget(element.Name))
		}
		return "[" + strings.Join(elements, ", ") + "]"
	default:
		return quote(value.Text)
	}
}

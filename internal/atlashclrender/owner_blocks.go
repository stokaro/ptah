package atlashclrender

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/hclext"
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

// slotBlock is one top-level block of a placement, written and waiting for its
// place: blocks of one placement are ordered by type, schema and name, so a
// document's order does not depend on which owner wrote a block or when its
// owner was registered.
type slotBlock struct {
	blockType string
	schema    string
	name      string
	text      string
}

// renderAfterTables writes the blocks that stand for a table or are attached
// to one -- a hypertable, a continuous aggregate, a synonym, a YDB
// coordination node, a SQL Server extended property -- after the tables, so a
// reader meets the structure before the things that point at it. None of
// them is an Atlas block, so no Atlas output fixes their order, and the order
// by type, schema and name is the stable one.
func (r *renderer) renderAfterTables() {
	var blocks []slotBlock
	for _, block := range r.ownerBlocks {
		if block.Placement == hclext.AfterTables {
			blocks = append(blocks, r.writeOwnerBlock(block))
		}
	}
	slices.SortStableFunc(blocks, func(a, b slotBlock) int {
		return cmp.Or(cmp.Compare(a.blockType, b.blockType), cmp.Compare(a.schema, b.schema), cmp.Compare(a.name, b.name))
	})
	for _, block := range blocks {
		r.builder.WriteString(block.text)
	}
}

// writeOwnerBlock writes a block an owner encoded: its label, its `schema`
// reference, then its attributes in the owner's order. The schema is the
// one the block names or, unless the block's empty schema is an address of
// its own, the document's default schema.
func (r *renderer) writeOwnerBlock(block hclext.EncodedBlock) slotBlock {
	schema := block.Schema
	if !block.SchemaIsAddress {
		schema = r.schemaFor(schema)
	} else if strings.TrimSpace(schema) == "" {
		schema = ""
	}
	text := r.capture(func() {
		r.linef(`%s %s {`, block.Type, quote(block.Name))
		if schema != "" {
			r.rawAttr(1, "schema", r.schemaRef(schema))
		}
		for _, attribute := range block.Attributes {
			r.rawAttr(1, attribute.Name, ownerValue(attribute.Value))
		}
		r.line("}")
		r.line("")
	})
	return slotBlock{blockType: block.Type, schema: schema, name: block.Name, text: text}
}

// ownerValue writes an encoded value: a string quoted, a bool bare. Set.Encode
// admits no other kind.
func ownerValue(value hclext.Value) string {
	if value.Kind == hclext.Bool {
		return strconv.FormatBool(value.Bool)
	}
	return quote(value.Text)
}

// capture returns what write writes, leaving the document as it was.
func (r *renderer) capture(write func()) string {
	written := r.builder
	r.builder = strings.Builder{}
	write()
	text := r.builder.String()
	r.builder = written
	return text
}

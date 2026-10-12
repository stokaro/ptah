package atlashclrender

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/systemschema"
)

// encodeOwnerBlocks hands the owners the document: its objects, the facets
// of its tables and of the blocks it writes in them, and its coverage. The
// writers take no context yet; the owners encode in process.
func (r *renderer) encodeOwnerBlocks(owners hclext.Set) (hclext.EncodeResult, error) {
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	request, err := r.slotRequest()
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	request.Objects, request.Coverage = objects, r.db.FeatureCoverage
	return owners.Encode(context.Background(), request)
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
// table when table is not nil; see [renderer.renderSlot].
func (r *renderer) renderOwnerBlocks(placement hclext.Placement, table *hclext.Table) {
	r.renderSlot(placement, table)
}

// slotEntry is one block of a placement waiting for its place, with what
// orders it and what writes it.
type slotEntry struct {
	blockType string
	schema    string
	scope     string
	name      string
	write     func()
}

// renderSlot writes the blocks of a placement, this package's own entries
// and the blocks the owners placed there, in a table when table is not nil,
// ordered by type, schema, scope and name so a document's order does not
// depend on which owner wrote a block or when its owner was registered. A
// schema that is only where an object is installed does not order it.
func (r *renderer) renderSlot(placement hclext.Placement, table *hclext.Table, entries ...slotEntry) {
	for _, block := range r.ownerBlocks {
		if block.Placement != placement || (table != nil && *block.Table != *table) {
			continue
		}
		schema := r.ownerSchema(block)
		order := schema
		if block.SchemaRole == hclext.SchemaPlacement {
			order = ""
		}
		entries = append(entries, slotEntry{blockType: block.Type, schema: order, scope: block.Scope, name: block.Name,
			write: func() { r.writeOwnerBlock(block, schema) }})
	}
	slices.SortStableFunc(entries, func(a, b slotEntry) int {
		return cmp.Or(cmp.Compare(a.blockType, b.blockType), cmp.Compare(a.schema, b.schema),
			cmp.Compare(a.scope, b.scope), cmp.Compare(a.name, b.name))
	})
	for _, entry := range entries {
		entry.write()
	}
}

// ownerSchema is the schema an owner's block writes, by what the schema is: a
// namespace takes an inspected document's default schema when it is blank,
// an address is left out when it is blank, and a placement takes the default
// only when it is empty, since a blank name is still a schema.
func (r *renderer) ownerSchema(block hclext.EncodedBlock) string {
	switch block.SchemaRole {
	case hclext.SchemaAddress:
		if strings.TrimSpace(block.Schema) == "" {
			return ""
		}
		return block.Schema
	case hclext.SchemaPlacement:
		return cmp.Or(block.Schema, r.defaultSchema)
	default:
		return r.schemaFor(block.Schema)
	}
}

// writeOwnerBlock writes a block an owner encoded where it stands: first the
// losses the owner reports about it, then, unless the block is unwritten or
// the Atlas-compatible surface leaves it out, its label, its `schema`, its
// attributes and then its children, each in the owner's order. A block in a
// table is indented one level and takes no label.
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
	if r.omitOwnerBlock(block) {
		return
	}
	r.linef(`%s %s {`, block.Type, quote(block.Name))
	r.writeOwnerSchema(block, schema)
	for _, attribute := range block.Attributes {
		r.rawAttr(1, attribute.Name, r.ownerValue(attribute.Value))
	}
	for _, child := range block.Children {
		r.linef("  %s %s {", child.Type, quote(child.Name))
		for _, attribute := range child.Attributes {
			r.rawAttr(2, attribute.Name, r.ownerValue(attribute.Value))
		}
		r.line("  }")
	}
	r.line("}")
	r.line("")
}

// omitOwnerBlock decides an owner's block's fate on the Atlas-compatible
// surface, by its type, as this package decides its own blocks': an
// extension by what the document needs of it, any other type by whether the
// document names the block or something it supplies.
func (r *renderer) omitOwnerBlock(block hclext.EncodedBlock) bool {
	if block.Type == blockExtension {
		return r.omitRefusedExtension(block.Path, block.Name, block.Provides)
	}
	return r.omitRefusedBlock(block.Path, block.Type, append([]string{block.Name}, block.Provides...)...)
}

// writeOwnerSchema writes a block's schema as a reference to the schema
// block the document then declares, except a placement in a schema the
// target owns, which is written as a string so the document declares no
// schema block the server would refuse to create. The common PostgreSQL
// namespaces are recognized even when the dialect is empty, which keeps
// dialect-neutral documents from turning placement into a declaration.
func (r *renderer) writeOwnerSchema(block hclext.EncodedBlock, schema string) {
	switch {
	case schema == "":
	case block.SchemaRole == hclext.SchemaPlacement && systemschema.IsPostgresFamilySystemSchema(r.dialect, schema):
		r.stringAttr(1, "schema", schema)
	default:
		r.rawAttr(1, "schema", r.schemaRef(schema))
	}
}

// ownerValue writes an encoded value: a string quoted, a bool and digits
// bare, SQL text as an sql() call it never evaluates, a reference to a table
// as the reference this document resolves, and a list of roles element by
// element, a role the document declares as a reference and any other element
// as a string. Set.Encode admits no other value.
func (r *renderer) ownerValue(value hclext.Value) string {
	switch value.Kind {
	case hclext.Bool:
		return strconv.FormatBool(value.Bool)
	case hclext.Number:
		return value.Text
	case hclext.SQL:
		return sqlCall(value.Text)
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

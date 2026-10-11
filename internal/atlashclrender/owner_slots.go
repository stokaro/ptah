package atlashclrender

import (
	"fmt"
	"slices"
	"sort"
	"strconv"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// tableLayout is the tables a render writes, in the order it writes them,
// with the columns, indexes and constraints each holds. It is computed once,
// so the blocks the owners are handed are the blocks the render writes.
type tableLayout struct {
	tables             []schemamodel.Table
	fieldsByStruct     map[string][]schemamodel.Field
	indexesByTable     map[string][]schemamodel.Index
	orphanIndexes      []schemamodel.Index
	constraintsByTable map[string][]schemamodel.Constraint
	orphanConstraints  []schemamodel.Constraint
}

func layTables(db *schemamodel.Database) tableLayout {
	tables := append([]schemamodel.Table(nil), db.Tables...)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].QualifiedName() < tables[j].QualifiedName()
	})
	layout := tableLayout{tables: tables, fieldsByStruct: groupFieldsByStruct(db.Fields)}
	layout.indexesByTable, layout.orphanIndexes = groupIndexesByTable(db.Indexes, tables)
	layout.constraintsByTable, layout.orphanConstraints = groupConstraintsByTable(db.Constraints, tables)
	return layout
}

// slotPositions are the attributes of an owner the writer's blocks have a
// place for, by kind of block: the places the Atlas layout fixes for them. A
// table's `engine` stands first and takes the place of the common engine, its
// `charset` after its API names, and `auto_increment`, `strict` and
// `without_rowid` after its comment; a column's `on_update` after its default
// and its `charset` after its check name; an index's `parser` after its type.
var slotPositions = map[hclext.SlotKind][]string{
	hclext.SlotTable:  {"engine", "charset", "auto_increment", "strict", "without_rowid"},
	hclext.SlotColumn: {"on_update", "charset"},
	hclext.SlotIndex:  {"parser"},
}

// ownerSlot is what the owners add to one of the writer's blocks: the
// attributes by name, and the losses in the owners' order.
type ownerSlot struct {
	attributes map[string]hclext.Value
	losses     []string
}

// slotTable names a table as the owners are handed it.
func slotTable(table schemamodel.Table) hclext.Table {
	return hclext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}
}

// slotRequest is the writer's blocks the render writes that hold facets, as
// the owners are handed them: each table's columns, primary key, constraints
// and indexes, in the order they are written. A document that writes one
// block twice with facets an owner writes is refused, since HCL cannot tell
// the two apart.
func (r *renderer) slotRequest() (hclext.EncodeRequest, error) {
	var request hclext.EncodeRequest
	seen := make(map[hclext.Slot]bool)
	add := func(slots *[]hclext.SlotFacets, slot hclext.Slot, facets schemaext.Facets) error {
		if facets.Len() == 0 {
			return nil
		}
		if slices.ContainsFunc(facets.Kinds(), r.owners.Encodes) {
			if seen[slot] {
				return fmt.Errorf("the document writes %s twice with owner settings, which HCL cannot tell apart", slot)
			}
			seen[slot] = true
		}
		*slots = append(*slots, hclext.SlotFacets{Slot: slot, Facets: facets})
		return nil
	}
	for _, table := range r.layout.tables {
		owner := slotTable(table)
		fields := r.layout.fieldsByStruct[table.StructName]
		for _, field := range fields {
			if err := add(&request.Columns, hclext.Slot{Kind: hclext.SlotColumn, Table: owner, Name: field.Name}, field.Facets); err != nil {
				return hclext.EncodeRequest{}, err
			}
		}
		if writesPrimaryKey(table, fields) {
			if err := add(&request.Keys, hclext.Slot{Kind: hclext.SlotKey, Table: owner, Primary: true}, table.PrimaryKeyFacets); err != nil {
				return hclext.EncodeRequest{}, err
			}
		}
		for _, constraint := range r.layout.constraintsByTable[table.QualifiedName()] {
			if err := add(&request.Keys, hclext.Slot{Kind: hclext.SlotKey, Table: owner, Name: constraint.Name}, constraint.Facets); err != nil {
				return hclext.EncodeRequest{}, err
			}
		}
		for _, index := range r.layout.indexesByTable[table.QualifiedName()] {
			if err := add(&request.Indexes, hclext.Slot{Kind: hclext.SlotIndex, Table: owner, Name: index.Name}, index.Facets); err != nil {
				return hclext.EncodeRequest{}, err
			}
		}
	}
	return request, nil
}

// collectSlots merges what the owners add to the writer's blocks, by block,
// and refuses an attribute the block has no place for.
func collectSlots(written []hclext.SlotAttributes) (map[hclext.Slot]ownerSlot, error) {
	slots := make(map[hclext.Slot]ownerSlot, len(written))
	for _, entry := range written {
		slot := slots[entry.Slot]
		for _, attribute := range entry.Attributes {
			if !slices.Contains(slotPositions[entry.Slot.Kind], attribute.Name) {
				return nil, fmt.Errorf("an HCL encoder wrote attribute %q of %s, which the writer has no place for", attribute.Name, entry.Slot)
			}
			if slot.attributes == nil {
				slot.attributes = make(map[string]hclext.Value)
			}
			slot.attributes[attribute.Name] = attribute.Value
		}
		slot.losses = append(slot.losses, entry.Losses...)
		slots[entry.Slot] = slot
	}
	return slots, nil
}

// ownerSlot is what the owners add to the block slot names, which holds
// facets. A block whose facets hold no kind an owner writes takes nothing,
// even where another block of the same name does: only blocks with such
// facets are refused when they repeat a name (see [renderer.slotRequest]),
// so only their name is theirs alone.
func (r *renderer) ownerSlot(slot hclext.Slot, facets schemaext.Facets) ownerSlot {
	if !slices.ContainsFunc(facets.Kinds(), r.owners.Encodes) {
		return ownerSlot{}
	}
	return r.slots[slot]
}

// slotAttr writes the attribute name an owner gave the block, if one did.
func (r *renderer) slotAttr(indent int, slot ownerSlot, name string) {
	if value, found := slot.attributes[name]; found {
		r.rawAttr(indent, name, r.ownerValue(value))
	}
}

// reportSlotLosses reports the losses the owners name for a block at the
// path this writer names the block with.
func (r *renderer) reportSlotLosses(path string, slot ownerSlot) {
	for _, loss := range slot.losses {
		r.warn(path, loss)
	}
}

// primaryKeyPath names a table's primary key in a diagnostic.
func primaryKeyPath(table schemamodel.Table) string {
	return "tables[" + strconv.Quote(table.Name) + "].primary_key"
}

// constraintPath names a constraint in a diagnostic.
func constraintPath(constraint schemamodel.Constraint) string {
	return "constraints[" + strconv.Quote(constraint.Name) + "]"
}

package schemadiff

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/tableidentity"
)

type facetOwnerSlot struct {
	subject objectidentity.ID
	values  *schemaext.Facets
}

// Inventory, capture, and restoration use the same identity for every slot.
// Index owners are resolved through the common table/materialized-view rules;
// the identifier snapshot decides whether their namespace includes the table.
// A materialized view is named the way the view comparison pairs it: a
// schema-scoped identity, read from the declaration's possibly qualified name.
func declaredFacetSlots(db *schemamodel.Database, target string, semantics identifier.Semantics) ([]facetOwnerSlot, error) {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Fields)+len(db.Indexes)+len(db.MaterializedViews))
	tables := make(map[string]objectidentity.ID, len(db.Tables))
	for i := range db.Tables {
		table := &db.Tables[i]
		subject := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		slots = append(slots, facetOwnerSlot{subject, &table.Facets})
		tables[table.StructName] = subject
	}
	// A field is a column of the table its struct declares. A field of a
	// struct that declares no table has no column identity, and a facet on it
	// is refused by the caller's capture check.
	for i := range db.Fields {
		field := &db.Fields[i]
		if table, found := tables[field.StructName]; found {
			slots = append(slots, facetOwnerSlot{columnSubject(table, field.Name, semantics), &field.Facets})
		}
	}
	owners := schemamodel.ResolveIndexOwners(db.Indexes, db.Tables, db.MaterializedViews)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		owner, valid := tableref.Parse(owners[i])
		if !valid {
			return nil, fmt.Errorf("%w: index %q has no resolved owner", ptaherr.ErrInvalidSchemaDiff, index.Name)
		}
		subject := objectidentity.NewBuilder(semantics).IndexParts(owner.Schema, owner.Name, index.Name)
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	for _, i := range standingMaterializedViews(db.MaterializedViews, semantics) {
		view := &db.MaterializedViews[i]
		slots = append(slots, facetOwnerSlot{declaredMaterializedViewSubject(view.Name, semantics), &view.Facets})
	}
	return slots, nil
}

// tablelessFieldFacets addresses the facets of every field whose struct
// declares no table.
func tablelessFieldFacets(db *schemamodel.Database) []*schemaext.Facets {
	tables := make(map[string]bool, len(db.Tables))
	for _, table := range db.Tables {
		tables[table.StructName] = true
	}
	var result []*schemaext.Facets
	for i := range db.Fields {
		if !tables[db.Fields[i].StructName] {
			result = append(result, &db.Fields[i].Facets)
		}
	}
	return result
}

// columnSubject is the identity of a column of table, under the schema and
// name the table's own identity holds, so a column pairs with its table on
// every target, SQLite's verbatim schema included.
func columnSubject(table objectidentity.ID, column string, semantics identifier.Semantics) objectidentity.ID {
	subject := objectidentity.NewBuilder(semantics).ColumnParts(table.Schema.Source, table.Name.Source, column)
	subject.Schema, subject.Parent = table.Schema, table.Name
	return subject
}

// standingMaterializedViews returns the positions of the declarations that
// stand, in declaration order: of two declarations of one view, such as
// `daily` and `analytics.daily` where analytics is the default schema, the
// last one stands, as it does for the view's body in the common comparison.
func standingMaterializedViews(views []schemamodel.MaterializedView, semantics identifier.Semantics) []int {
	last := make(map[objectidentity.Key]int, len(views))
	for i, view := range views {
		last[declaredMaterializedViewSubject(view.Name, semantics).Key()] = i
	}
	standing := make([]int, 0, len(last))
	for i, view := range views {
		if last[declaredMaterializedViewSubject(view.Name, semantics).Key()] == i {
			standing = append(standing, i)
		}
	}
	return standing
}

// declaredMaterializedViewSubject is the identity of a declared materialized
// view, whose name may carry its schema. A name whose own text contains a dot
// is not mistaken for a qualified one.
func declaredMaterializedViewSubject(name string, semantics identifier.Semantics) objectidentity.ID {
	return objectidentity.NewBuilder(semantics).SchemaScoped(objectidentity.KindMatView, name)
}

func observedFacetSlots(db *catalog.Database, target string, semantics identifier.Semantics) []facetOwnerSlot {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Indexes)+len(db.MatViews))
	for i := range db.Tables {
		table := &db.Tables[i]
		subject := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		slots = append(slots, facetOwnerSlot{subject, &table.Facets})
		for j := range table.Columns {
			column := &table.Columns[j]
			slots = append(slots, facetOwnerSlot{columnSubject(subject, column.Name, semantics), &column.Facets})
		}
	}
	builder := objectidentity.NewBuilder(semantics)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		subject := builder.IndexParts(index.Schema, index.TableName, index.Name)
		if index.IsPrimary {
			// A primary key's index is its table's key, so it is named within
			// its table even where index names are the schema's: Spanner names
			// every one PRIMARY_KEY (stokaro/ptah#4287). A declaration names no
			// primary key index, so no declared slot is keyed this way.
			subject.Parent = builder.TableParts(index.Schema, index.TableName).Name
		}
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	for i := range db.MatViews {
		view := &db.MatViews[i]
		subject := objectidentity.NewBuilder(semantics).SchemaScopedParts(objectidentity.KindMatView, view.Schema, view.Name)
		slots = append(slots, facetOwnerSlot{subject, &view.Facets})
	}
	return slots
}
